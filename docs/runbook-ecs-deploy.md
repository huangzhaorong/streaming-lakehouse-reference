# Runbook — 阿里云 ECS 原生部署（从零到端到端验证）

前置：本机已装 terraform ≥1.9、ansible-core ≥2.16、aliyun CLI（或 `ALICLOUD_ACCESS_KEY/SECRET` 环境变量）、ossutil、docker（仅提取 Iggy 二进制用）。
设计依据：`docs/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md`；任务级计划：`docs/plans/2026-10-04-ecs-native-oss-hdfs-n8n.md`。

---

## Phase 0 — 本地准备（一次性）

```bash
# 1. 提取 Iggy 二进制（官方无发行资产；需本机 docker）
./scripts/extract-iggy-binary.sh dist/
# 记录 dist/iggy-0.7.0-linux-x86_64.tar.gz.sha256 的值
#    → 回填 iac/ansible/inventories/prod/group_vars/all.yml 的 iggy_artifact_sha256

# 2. 准备 Terraform 变量与 Ansible 秘密
cd iac/terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars   # 填 vpc_name/vswitch_cidr/admin_cidrs/私网 IP/公钥
cd ../../..
cd ansible
cp vault-example.yml inventories/prod/vault.yml
ansible-vault encrypt inventories/prod/vault.yml   # 编辑填入真值：ansible-vault edit ...
echo "<vault 密码>" > .vault_pass && chmod 600 .vault_pass
```

## Phase 1 — 基础设施（Terraform）

```bash
cd iac/terraform/environments/prod
terraform init -backend-config=backend.conf
# ⚠️ 本机 init 需临时把 backend.conf endpoint 改为公网 oss-cn-beijing.aliyuncs.com（勿提交）
terraform plan -out tfplan && terraform apply tfplan
terraform output   # 记录 data_ip / app_ip / bucket_lakehouse / bucket_artifacts
```

**带外步骤①：开通 OSS-HDFS**（Terraform 无此能力）
控制台 → OSS → `slr-lakehouse-prod` → 数据湖加速（OSS-HDFS/JindoFS 服务）→ 开通（cn-beijing）。未开通则 Phase 2 G1 金丝雀必失败（天然拦截）。

**上传部署工件**：
```bash
ossutil cp dist/iggy-0.7.0-linux-x86_64.tar.gz oss://slr-artifacts-prod/
ossutil cp dist/iggy-0.7.0-linux-x86_64.tar.gz.sha256 oss://slr-artifacts-prod/
# Grafana infinity 插件离线包（可选，装机期目标机可出公网则不需要）
```

**生成 inventory**：
```bash
./iac/scripts/generate-inventory.sh prod    # terraform output → iac/ansible/inventories/prod/hosts
ansible -i iac/ansible/inventories/prod all -m ping   # 双机 pong
```

host_vars 实测回填（禁 auto-detect）：SSH 上两台 `lsblk` 确认数据盘设备名 →
`iac/ansible/inventories/prod/host_vars/ecs-data-01.yml`（与 app）写 `slr_data_disk_device: /dev/vdb`。

## Phase 2 — 数据面

```bash
cd iac/ansible
ansible-playbook playbook_dataplane.yaml
```

**G1 金丝雀（OSS-HDFS 读写，硬门槛）** — SSH ecs-data-01：
```bash
sudo -u flink env JAVA_HOME=/usr/lib/jvm/java-17-openjdk HADOOP_CONF_DIR=/opt/flink/conf/hadoop \
  /opt/flink/bin/sql-client.sh embedded <<'EOSQL'
SET 'execution.runtime-mode' = 'batch';
CREATE TABLE canary (id INT) WITH ('connector'='filesystem','path'='oss://slr-lakehouse-prod/canary/','format'='parquet');
INSERT INTO canary VALUES (1);
SELECT * FROM canary;
EOSQL
# 判定：SELECT 返回 1；控制台 slr-lakehouse-prod → canary/ 出现在 HDFS 命名空间（目录树视图）
```
失败回退（R1/R2）：`fs.oss.endpoint` 改 `oss-cn-beijing-internal.aliyuncs.com`（普通 OSS）、
flink-conf/core-site 删 Jindo provider 三键、恢复 `flink-oss-fs-hadoop-1.20.3.jar` 到 lib、
state 桶改 `slr-lakehouse-prod-state`；JindoFuse URI 同步改普通 endpoint。重跑 playbook_dataplane。

**G2 金丝雀（Iggy HTTP）**：
```bash
TOKEN=$(curl -s -X POST http://localhost:3000/users/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"<vault_iggy_password>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
curl -s -X POST http://localhost:3000/streams/crypto/topics/prices/messages -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"partitioning":{"kind":"messages_key","value":"'$(printf BTC-USD | base64)'"},"messages":[{"id":0,"payload":"'$(printf '{"canary":1}' | base64)'"}]}'
# 判定：HTTP 2xx；iggy CLI poll 回读可见
```
前置核对：`/opt/iggy/configs/server.toml` 的 `data_dir` 已指向 /data/iggy（提取件布局差异时手工修正后 `systemctl restart iggy`）。

**G3 金丝雀（fuse 直读，两台都跑）**：
```bash
ls /mnt/warehouse/canary/
python3 -c "import duckdb; print(duckdb.sql(\"SELECT * FROM read_parquet('/mnt/warehouse/canary/**/*.parquet')\").fetchall())"
# 判定：[(1,)]。失败兜底（R3）：ossutil 定时同步 canary/warehouse 到本地盘再读（新鲜度延迟一个周期）
```

**提交 Flink 作业**（G1-G3 全过后）：
```bash
ansible-playbook playbook_jobs.yaml
# 顺序内置：lakehouse-tier → fluss-hot-tier → seed-balance(守卫) → clearing-house → RUNNING≥3 校验
# replay-pipeline 按需：sudo -u flink /opt/flink/bin/sql-client.sh embedded -f /opt/flink/sql/replay-pipeline.sql
```

验证：`systemctl list-timers reconcile-snapshot`；5 分钟后 `ls -l /mnt/warehouse/paimon/crypto.db/reconcile_balance.json`（mtime 滚动）。

**彻底重置 Flink 状态**（重放 seed 等）：清 ZK `zkCli.sh deleteall /streaming-lakehouse` + 清 `oss://slr-lakehouse-prod/flink/ha/` → 重启 JM/TM。

## Phase 3 — 应用面

```bash
ansible-playbook playbook_appplane.yaml
```

**带外步骤②：n8n owner 账号**（首启一次性）：
```bash
ssh -L 5678:localhost:5678 root@<app_ip>
# 浏览器 http://localhost:5678 → 创建 owner 账号
# CLI 导入（role 已跑）与 owner 无关；owner 仅用于 UI 查看执行历史/手动 Execute
```

**backfill 历史数据**（lancer/replay 需要历史深度；不迁移旧 compose 数据）：
```bash
# SSH ecs-data-01（fuse rw）：
cd /opt/slr/scripts && python3 backfill-candles.py --days 60   # 产出 → /mnt/warehouse/backfill/
sudo -u flink env JAVA_HOME=/usr/lib/jvm/java-17-openjdk HADOOP_CONF_DIR=/opt/flink/conf/hadoop \
  /opt/flink/bin/sql-client.sh embedded -f /opt/flink/sql/import-backfill.sql
systemctl start lancer-indexer    # 首轮建 LanceDB（此后每小时 timer 增量）
```

## Phase 4 — 端到端验证与切换

```bash
# 1. 价格流入
curl -s http://<app_ip 或隧道>:9090/api/v1/query?query=crypto_price | head   # 有样本
# Grafana live-ticks 出实时线；bridge_consumed_total 递增

# 2. n8n 决策周期
# UI（隧道 :5678）→ Executions：60s 周期 success；抽查一条执行详情节点链完整
curl -s http://localhost:8009/api/state | python3 -m json.tool   # updated_at 滚动

# 3. 手动触发一笔测试单（buy 0.001 BTC）
TOKEN=$(...login...)
curl -s -X POST http://<data_ip>:3000/streams/crypto/topics/orders/messages -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"partitioning":{"kind":"messages_key","value":"'$(printf BTC-USD|base64)'"},"messages":[{"id":0,"payload":"'$(printf '{"order_id":"test-1","pair":"BTC-USD","side":"BUY","price":50000,"quantity":0.001,"signal_similarity":0.9,"signal_sentiment":"Bullish","time":"2026-10-04T00:00:00.000000Z"}' | base64)'"}]}'
# Flink UI clearing-house numRecordsIn +1 → 下一轮 reconcile（≤5min）后：
curl -s http://localhost:8009/api/balance    # USD 减 50.05（含费），BTC 增
curl -s http://localhost:8009/api/trades     # 出现该笔
# Grafana trading dashboard 出数 + trade annotation

# 4. killswitch
curl -X POST http://localhost:5678/webhook/killswitch -H 'Content-Type: application/json' -d '{"killswitch":true}'
# 下一轮 execution：开仓全 BLOCKED(kill_switch)，已有持仓的平仓仍放行
curl http://localhost:5678/webhook/killswitch          # 确认
curl -X POST http://localhost:5678/webhook/killswitch -d '{"killswitch":false}' -H 'Content-Type: application/json'

# 5.（可选）熔断演练：n8n UI 临时把 DEFAULTS.MAX_DAILY_DRAWDOWN 改 0 → 下轮 circuit_broken=true → 改回
```

全部通过 → 本地 `docker compose down`（如仍在跑；卷留 30 天冷静期后清理）。

---

## 日常运维

| 操作 | 命令 |
|---|---|
| 服务状态 | `systemctl status iggy zookeeper fluss-coordinator fluss-tablet flink-jobmanager flink-taskmanager jindofuse-warehouse`（数据面）；`systemctl status poller bridge lancer query-api prometheus grafana n8n`（应用面） |
| 重部署代码 | 本机 `ansible-playbook playbook_appplane.yaml`（rsync + restart；workflow JSON 变更自动 re-import） |
| Flink 作业重提交 | `ansible-playbook playbook_jobs.yaml`（seed 有守卫） |
| UI 访问 | SSH 隧道：`ssh -L 3001:localhost:3001 -L 5678:localhost:5678 -L 8081:localhost:8081 root@<app_ip>`（8081 需再跳数据面或用 ui_cidrs 白名单） |
| 日志 | `journalctl -u <service> -f` |
| n8n 兜底停机 | `sudo -u n8n N8N_USER_FOLDER=/var/lib/n8n /opt/node/bin/n8n unpublish:workflow --id=slr-trading-decision` |

## 回退与故障对照（风险表 R1-R10 操作化）

| 症状 | 处置 |
|---|---|
| G1 失败（oss:// 不可写/类加载错） | 上文"失败回退"段：普通 OSS endpoint + flink-oss-fs-hadoop 插件 + state 分桶 |
| ECS_ROLE 免 AK 失败（403） | vault 填 `vault_oss_access_key_id/secret` → flink-conf 模板已内置 R2 回退块，重跑 playbook |
| fuse 上 DuckDB 读超时/报错 | ossutil 定时同步到 /data/warehouse-cache，query-api env 指 `QUERY_API_WAREHOUSE_PATH` 改本地缓存路径 |
| jindofuse 反复重启 | `journalctl -u jindofuse-warehouse`；常见为 RAM role 未绑定（terraform output instance_registry 核对）或 endpoint 写错 |
| n8n execution 连续失败 | UI 看节点错误；staticData 未 commit（失败轮不落账，偏保守）；持续失败先 killswitch 再排查 |
| Iggy 无法启动 | 核对 server.toml data_dir 与 /data/iggy 权限；`ldd /opt/iggy/iggy-server` 验证 glibc 兼容 |
| Flink TM 注册不上 | JM/TM 同机 rpc=127.0.0.1；查 flink-conf 渲染与 /var/log/flink |
