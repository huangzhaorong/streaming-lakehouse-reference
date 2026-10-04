# Streaming Lakehouse → 阿里云 ECS 原生部署 + OSS-HDFS + n8n 决策链 + Terraform/Ansible IaC

## Context

streaming-lakehouse-reference 目前以 Docker Compose 运行（14 服务），LLM 情绪分析依赖宿主 Ollama（Llama3 8B），warehouse 数据存本地 named volume。目标：

1. 迁移到阿里云 ECS **原生部署**（systemd，无 Docker），彻底删除 Compose 路径
2. warehouse（Paimon/Iceberg/checkpoints/HA）**迁 OSS-HDFS**（cn-beijing 新建专用 bucket）
3. **n8n 彻底替代 analyst + consensus**：决策链（信号拉取→LLM 情绪→风控→发 OrderRequest 到 Iggy）全部在 n8n workflow；LLM 用百炼 DashScope（OpenAI-compatible，模型可切换）
4. **Terraform + Ansible 全量 IaC**，自包含于项目内 `iac/`（拷贝 adtrader-iac-infra 成熟模式：OSS backend state、RAM role 免 AK、SG 组内互通、role 骨架 download→/opt→j2→systemd、inventory 生成管道）

用户已确认：两台 ECS 分层（数据面 8C32G + 应用面 4C16G）；方案 1 = JindoFuse 挂载 + n8n 全决策 + 瘦 query-api sidecar；旧数据不迁移（backfill 重拉）。

## 已实证的调研结论

| 项 | 结论 |
|---|---|
| Flink 1.20.3 × OSS-HDFS | JindoSDK **6.10.8**（`jindosdk-6.10.8-linux.tar.gz`）：`jindo-flink-6.10.8-full.jar` + `jindo-sdk/core-nextarch.jar` 进 `$FLINK_HOME/lib/`，native `.so` 进 `/usr/lib64/`；**必须不存在 flink-oss-fs-hadoop jar**。URI 用纯桶名 `oss://<bucket>/warehouse/...` + `fs.oss.endpoint: cn-beijing.oss-dls.aliyuncs.com`（⚠️ Paimon 拒绝 bucket 内嵌 authority 的 URI——adtrader 金丝雀实证）；RAM 免 AK：`fs.oss.provider.endpoint: ECS_ROLE` + `fs.oss.provider.format: JSON`；bucket 内嵌 dls URI 仅用于 JindoFuse 挂载 |
| Iggy 0.7.0 HTTP 发单 | ✅ `POST /streams/{s}/topics/{t}/messages`，`POST /users/login` 取 Bearer token；partitioning `messages_key`=base64(pair)；payload=base64(OrderRequest JSON) → n8n HTTP 节点直接发单，无需转发 shim |
| Fluss 0.9.0 binary | ✅ `archive.apache.org/dist/incubator/fluss/fluss-0.9.0-incubating/fluss-0.9.0-incubating-bin.tgz` |
| Iggy 0.7.0 binary | ❌ 官方无发行资产 → `scripts/extract-iggy-binary.sh` 从 docker 镜像一次性提取（iggy-server/iggy CLI/configs）+ sha256 → 上传 artifacts OSS 桶（glibc bookworm→ALinux3 兼容已核） |
| n8n 原生 | Node **24 LTS** + `npm i -g n8n@2.41.6`（锁死；npm 安装 3.0 起废弃）；`N8N_USER_FOLDER=/var/lib/n8n` SQLite；workflow JSON 入库 + `n8n import:workflow --separate` / `publish:workflow` 幂等导入；凭据 `import:credentials`（与运行实例共用 vault 下发的 `N8N_ENCRYPTION_KEY`）；owner 账号首启 Web 建立（带外，SSH 隧道） |
| JindoFuse | 同一 tarball；`jindo-fuse /mnt/warehouse -ouri=oss://<bucket>.cn-beijing.oss-dls.aliyuncs.com/`；jindosdk.cfg 配 ECS_ROLE；systemd Type=forking；数据面 rw、应用面 ro；Python pyarrow/DuckDB 直读零改动 |

## 目标架构

```
cn-beijing，复用现有 VPC/vSwitch（data source 只读引用），单 SG inner_access_policy=Accept（两台全端口互通）
┌─ ecs-app-01 应用面 g8i.xlarge 4C16G（40G+100G ESSD）─┐   ┌─ ecs-data-01 数据面 g8i.2xlarge 8C32G（40G+200G PL1）─┐
│ poller:8000 bridge:8001 replay:8002(oneshot)        │   │ Iggy 0.7.0 TCP:8090 HTTP:3000（/data/iggy）            │
│ lancer:8003/8004 + indexer(systemd timer 每小时)     │──▶│ ZooKeeper 3.9.3 :2181（/data/zookeeper）               │
│ query-api:8009(新) Prometheus:9090 Grafana:3001     │TCP│ Fluss coord:9123 tablet:9124（/data/fluss 持久化）      │
│ n8n:5678  /mnt/warehouse(fuse 只读) /data/lancer    │   │ Flink 1.20.3 JM:8081 TM:9249×8slot（HA=ZK）            │
│ 公网出向：Coinbase WSS、DashScope                    │   │ /mnt/warehouse(fuse 读写) reconcile timer(5min)        │
└──────────────────────────────────────────────────────┘   └────────────────────────────────────────────────────────┘
        双方 → OSS-HDFS 桶 slr-lakehouse-prod：warehouse/{paimon,iceberg,backfill} + flink/{checkpoints,savepoints,ha}
        artifacts 桶 slr-artifacts-prod(public-read)：iggy 提取件等
        公网入向仅 SSH 白名单；Grafana/n8n/Flink UI 走 SSH 隧道（可选 var.ui_cidrs 开直连）
删除：analyst(:8005/6)、consensus(:8007/8)、iggy-web-ui(:8888，无原生发行形态，iggy CLI+Grafana 覆盖其用途)
```

## 仓库最终结构

```
iac/terraform/{providers.tf,variables.tf,modules/{ecs-instance,oss-bucket,ram-role,security-group},
               environments/prod/{backend.conf,main.tf,locals.tf,terraform.tfvars.example,outputs.tf,inventory.tmpl}}
iac/ansible/{ansible.cfg,requirements.yml,inventories/prod/{hosts(生成),group_vars/{all,dataplane,appplane}.yml},
             playbook_{dataplane,appplane,jobs}.yaml,
             roles/{common,zookeeper,iggy,fluss,flink,jindofuse,python-service,prometheus,grafana,n8n}}
iac/scripts/{generate-inventory.sh,render-inventory.py,render_inventory_lib.py}   # 拷 adtrader，仅留 prod
services/query-api/{main.py,requirements.txt,test_query_api.py}                   # 唯一新服务 ≤300 行
n8n/workflows/trading-decision.json + credentials/*.example
scripts/{download-jars.sh(+jindosdk),render-sql.sh(新),reconcile-snapshot.sh(裸机化),
         backfill-candles.py,extract-iggy-binary.sh(新)}
docs/{runbook-ecs-deploy.md(新),architecture.md(更新),env-reference.md(新，替代 .env.example)}
删除：docker-compose.yml、flink/Dockerfile、analyst/、consensus/、iggy-web-ui、.env.example、
      prometheus/prometheus.yml 与 grafana/{dashboards,datasources}（移入对应 role 模板）
poller/bridge/replay/lancer 零代码改动（纯环境变量）。flink/sql/ 本体零改动（部署期渲染）。
```

## 实施步骤

### Phase 0 仓库准备（本地）
1. `extract-iggy-binary.sh` 提取 Iggy 二进制 + sha256（开发机最后一次用 docker；上传 artifacts 桶在 Phase 1 后）
2. **query-api 抽取**：从 consensus/main.py 搬移 `/api/audit /api/trades /api/candles/{pair} /api/positions /api/health`（DuckDB 读 reconcile_*.json + parquet，仅 append-only 表）；新增 `/api/balance`、`/api/state`（POST 原子落 `/var/lib/query-api/state.json`，GET 返回——n8n 写、Grafana 读展示副本）。不含任何决策/Iggy 写。附 pytest
3. `render-sql.sh`：sed 渲染 `SLR_WAREHOUSE_BASE=oss://<bucket>/warehouse`、`SLR_IGGY_HOST=127.0.0.1`（替换 `/opt/flink/warehouse`、`file:///…/backfill/`、`'iggy'`；不用 sql-client -i，因需替换 catalog WITH 选项）
4. `reconcile-snapshot.sh` 裸机化：去 docker exec，直接 `/opt/flink/bin/sql-client.sh embedded`；JSON 写 `/mnt/warehouse/paimon/crypto.db/`
5. n8n workflow JSON 初版（本地 `npx n8n` 干跑）；执行删除清单；iac/ 骨架拷贝裁剪
6. grep `8007|8008|consensus|analyst|docker` 全仓出残留清单（含 Grafana dashboards：trading.json 的 consensus URL → `http://localhost:8009` 路径不变；analyst narrative 面板删除）

### Phase 1 Terraform 基础设施
7. `environments/prod`：data source（vpcs/vswitches/images ALinux3）+ 模块：SG（Accept 组内互通，**勿写自引用规则**）、oss-bucket×2（lakehouse private / artifacts public-read，无生命周期规则）、ram-role（trust=ecs，policy=oss:* on lakehouse 桶 + artifacts 读，两台绑定）、key_pair、ecs-instance×2（固定私网 IP、bandwidth_out=100 PayByTraffic）。SG 规则：入向仅 22(var.admin_cidrs) + 可选 3001/5678/8081/9090(var.ui_cidrs 默认[])。State=OSS backend
8. `terraform apply` → **带外**：控制台为 slr-lakehouse-prod 开通 OSS-HDFS（runbook 固化；TF 无此能力）→ 上传 iggy 提取件到 artifacts 桶
9. `generate-inventory.sh prod` → `ansible -m ping` 双机通

### Phase 2 数据面（含金丝雀硬门槛）
10. `playbook_dataplane.yaml`：common(java-17/fuse3/chronyd/python3.12，dnf 无则 uv standalone) → jindofuse(rw) → zookeeper → iggy（EnvironmentFile + `LimitMEMLOCK=infinity` + `AmbientCapabilities=CAP_SYS_NICE`）→ fluss（**data.dir=/data/fluss/data、remote.data.dir=/data/fluss/remote** 修掉 /tmp 易失）→ flink（8 既有 jar + jindosdk 三件套 + `find lib -name 'flink-oss-fs-hadoop*' -delete` 防御；flink-conf.yaml.j2 + core-site.xml.j2；ZK HA cluster-id=/streaming-lakehouse、hashmap backend、8 slot、prom :9249）
11. **G1 金丝雀**：sql-client 建 filesystem 表 INSERT→SELECT `oss://…/canary/`，控制台确认落 HDFS 命名空间；dummy 流作业验证 checkpoints 落 oss://。失败回退：endpoint 改普通内网 OSS + flink-oss-fs-hadoop 插件（架构零变化，fuse URI 同步改）
12. **G2**：curl Iggy login→发消息→poll 回读。**G3**：两台 fuse ls/cat + duckdb/pyarrow 直读 canary parquet（失败兜底：ossutil 定时同步本地盘再读）
13. `playbook_jobs.yaml` 按序提交：lakehouse-tier → fluss-hot-tier → ohlcv-candles → **seed-balance（batch 一次性，守卫：balance 非空跳过）** → clearing-house →（replay-pipeline 按需）。全新 ZK ⇒ 无 HA seed 重放陷阱；彻底重置 = 清 ZK `/streaming-lakehouse` + 清 `oss://bucket/flink/ha`（runbook）
14. reconcile systemd timer（5min）启用，验证 JSON mtime 滚动

### Phase 3 应用面
15. `playbook_appplane.yaml`：common → jindofuse(ro) → python-service role×N（通用骨架：rsync 源码 + venv/pip 镜像 + EnvironmentFile j2 + `python -u main.py`；poller/bridge `IGGY_HOST=<data_ip>`，replay/lancer 加 `*_WAREHOUSE_PATH=/mnt/warehouse`；replay=oneshot disabled、indexer=oneshot+timer）→ query-api → prometheus（scrape：bridge 1s/poller/lancer/query-api/n8n localhost:5678/flink `<data_ip>:9249`；删 analyst/consensus/replay job）→ grafana（infinity 插件离线预下载进 artifacts；datasource localhost:9090；5 dashboards provisioning）→ n8n
16. **带外**：SSH 隧道建 n8n owner；role 自动 import workflow/credentials + publish
17. backfill：data-01 跑 `backfill-candles.py`（输出 `/mnt/warehouse/backfill/`）拉 30–90 天 → import-backfill.sql → indexer 首轮建 LanceDB

### Phase 4 切换
18. 本地 `docker compose down`（卷留 30 天冷静期）；W1 publish 观察完整决策周期；更新 README/CLAUDE.md/architecture.md，env-reference.md 落盘

## n8n workflow 设计（W1 trading-decision，唯一 workflow）

双触发器共享 static data：**A** Schedule 60s；**B** Webhook `/webhook/killswitch`（POST 写/GET 读）。
主链：Code(init：读 staticData positions/cash/day_baseline/killswitch + 常量区阈值 ENTER=0.20/EXIT=0.10/SIZE=$50/DRAWDOWN=2%/SIGNAL_AGE=120/PRICE_AGE=30/HEARTBEAT=180/VIABILITY=1.5/FEE=0.001/SLIPPAGE=0.0005/6 pairs，UTC 日切) → SplitInBatches → HTTP lancer `:8003/api/signals/{pair}` → HTTP Prometheus `crypto_price` → HTTP query-api `/api/balance` → **HTTP DashScope** `compatible-mode/v1/chat/completions`（model 可配 qwen-turbo/plus/max，temperature 0.3，max_tokens 200，json 输出 `{sentiment,narrative}`，解析失败回退正则）→ Code(风控裁决：consensus 全部规则单节点实现，输出 BUY/SELL/HOLD/BLOCKED+reason) → Switch → HTTP Iggy login(token 缓存 staticData，401 重登) → Code 组包 + HTTP POST messages（messages_key=pair，OrderRequest schema 与 consensus 完全一致）→ HTTP Grafana annotation（trade 事件 + narrative 每 5 轮）→ Code(commit staticData，trade_log 截断) → HTTP POST query-api `/api/state`（fire-and-forget）。

风控映射：similarity/sentiment 进出场、日回撤熔断（只挡开仓）、价格延迟>30s、信号心跳>180s、新鲜度 120s、viability≥1.5、EMERGENCY_EXIT=hold、killswitch——全部 1:1 对应 consensus 原逻辑（实施时逐条对照 main.py 复核，含 killswitch 下平仓是否豁免）。审计持久化 = Iggy orders→Flink clearing→Paimon trades（湖内不可变，强于原内存日志）。

凭据（vault → n8n credential）：DashScope key、Iggy Basic Auth、Grafana Basic Auth；`N8N_ENCRYPTION_KEY` vault 渲染 EnvironmentFile。

## 关键约束（迁移必须保留）

- DuckDB 只读 append-only 表（ohlcv_1m）；balance/trades PK 表走 Flink SQL（reconcile 脚本承担）
- Paimon balance 为 aggregation delta 语义，seed 只跑一次（job 守卫 + runbook 双保险）
- SQL 提交顺序、`pipeline.jars`、`classloader.parent-first-patterns.additional`（org.apache.paimon./org.apache.iceberg./org.apache.hadoop./com.codahale.）保持
- 所有配置走 group_vars/vault，源码零硬编码；提交信息 Conventional Commits（中文）

## 验证（端到端）

1. **Phase 2**：7 个 systemd 服务 active；`echo srvr | nc 127.0.0.1 2181`；`curl :8081/taskmanagers`=8 slots；`iggy ping`；G1/G2/G3 过；`curl :8081/jobs/overview` ≥3 流作业 RUNNING 且 checkpoint 在 oss://；seed 后 balance=1000 且重跑守卫跳过；reconcile JSON mtime 5min 滚动
2. **Phase 3**：python 服务/query-api/prometheus/grafana/n8n active；`:8009/api/health`、`/api/candles/BTC-USD` 非空；Prometheus targets 全 UP（含跨机 9249、n8n 5678）；Grafana 5 dashboards 出数；n8n execution 60s 周期 success
3. **Phase 4 全链路**：prices 流入 → 手动 Execute W1（或 curl Iggy 发测试单 buy 0.001 BTC）→ Flink clearing numRecordsIn+1 → 下轮 reconcile 后 `/api/balance` USDT 减/BTC 增、`/api/trades` 出现该笔 → Grafana trade annotation → killswitch POST→全 BLOCKED→GET→恢复 → 可选熔断演练

## 风险

R1 jindo-flink×Flink1.20.3 官方未验证（按 1.x API 编译，预期可用）→ G1 硬门槛+普通 OSS 回退；R2 ECS_ROLE JVM 层文档未逐字覆盖 → 回退 AK+vault；R3 fuse 上 DuckDB 随机读性能 → G3 实测+ossutil 同步兜底；R4 Iggy 提取件 → sha256 钉死可复现；R5 n8n 2.41.6 锁死（3.0 废弃 npm 装法）→ workflow JSON 可导出无锁定；R6 staticData 仅成功后保存 → killswitch webhook 失败告警 + `unpublish` 兜底；R7 Fluss 本地盘（0.9 无 OSS-HDFS remote 支持）→ 200G PL1 + 可重放；R10 DashScope key 落 n8n SQLite → ENCRYPTION_KEY vault + 目录 700 + RAM 最小权限；未决：ZK/Node patch 版本、ALinux3 python3.12 可得性（实施首日机上定版回填 all.yml）。

## 备注

- 本文件即设计 spec；批准后第一步可将其归档至 `docs/superpowers/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md` 并提交
- adtrader-iac-infra 仅作参考（只读拷贝模块/脚本模式），不修改该仓库
