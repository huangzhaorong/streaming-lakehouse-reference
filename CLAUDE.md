# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Streaming Lakehouse Reference：加密货币实时行情 → 分层流式存储（Fluss/Paimon/Iceberg on **OSS-HDFS**）→ 向量模式匹配 → **n8n + DashScope Qwen** 决策 → 模拟盘交易的参考架构（无传统数据库，无真实资金）。

部署形态：**阿里云 ECS 原生（systemd，无 Docker）**，两台分层（数据面 8C32G / 应用面 4C16G，cn-beijing），全量 IaC（`iac/terraform` + `iac/ansible`）。原 Docker Compose / Ollama / analyst / consensus 路径已于 2026-10 迁移中**彻底删除**。

详细设计见 `architecture.md`；部署手册 `docs/runbook-ecs-deploy.md`；配置归属 `docs/env-reference.md`；重置与回填 `docs/clean-slate-guide.md`；迁移设计记录 `docs/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md`。

## 常用命令

```bash
# 本地门禁（提交前全跑）
bash scripts/tests/test_render_sql.sh
PYTHONPATH=services/query-api/_deps python3 -m pytest services/query-api n8n/tests/test_workflow_json.py iac/scripts/tests -q
node --test n8n/tests/*.mjs
terraform -chdir=iac/terraform/environments/prod validate   # 需 TF_VAR_vpc_name/vswitch_cidr/slr_instances，见计划 Task 6
for pb in dataplane appplane jobs; do ansible-playbook -i iac/ansible/inventories/prod/hosts.dummy iac/ansible/playbook_$pb.yaml --syntax-check; done

# n8n workflow：改 workflows/decide.js 或 build_workflow.py 后必须重建 + 测试
python3 n8n/build_workflow.py && node --test n8n/tests/*.mjs

# 依赖（query-api 本地测试）：uv pip install --target services/query-api/_deps -r services/query-api/requirements.txt pytest httpx

# 部署（需云凭证，操作者执行）：见 docs/runbook-ecs-deploy.md
# terraform apply → generate-inventory.sh prod → playbook_dataplane → 金丝雀 G1/G2/G3 → playbook_jobs → playbook_appplane
```

无传统测试框架的项目级约定：query-api=pytest、n8n 风控=node --test、inventory=pytest、SQL 渲染=bash 门禁、Ansible=syntax-check、Terraform=validate。

## 架构要点

数据流：Coinbase WebSocket → **poller**(app) → **Iggy**(data，`crypto/prices|orders|replay`) → 四条下游：
1. **bridge**：Iggy → Prometheus（sequence 去重）→ Grafana
2. **Flink**(data，3 流作业)：热层 Iggy→Fluss；湖仓 Iggy→Paimon `ohlcv_1m`+Iceberg ticks（warehouse=OSS-HDFS）；清算 Iggy orders→Paimon `balance`/`trades`+Fluss `executed_trades`
3. **lancer**：indexer(timer，Paimon→LanceDB) / signal(实时匹配，FastAPI :8003)
4. **n8n** `slr-trading-decision`：每 60s 拉信号+价格+余额 → Qwen 情绪 → decide() 风控 → Iggy HTTP 发单 → Grafana 注解 → 状态镜像 query-api；killswitch 走 Webhook

**query-api**(:8009)：DuckDB 只读查询（audit/candles）+ reconcile JSON（balance）+ n8n 状态镜像（state/positions/trades）+ `/metrics`（state→`slr_*` 指标）。不含决策。

Python 服务经 **JindoFuse** 挂载 `/mnt/warehouse` 直读湖内 parquet（零代码改动）；Flink 经 JindoSDK 直写 OSS-HDFS。

### 关键约束（改代码前必读）

- **DuckDB 读 Paimon 只对 append-only 表安全**（`ohlcv_1m`）；PK+merge-engine 表（`balance`/`trades`）必须走 Flink SQL（reconcile timer 是唯一权威读方）
- **Paimon `balance` 是 aggregation merge engine**：INSERT 是 delta；seed 只跑一次（playbook_jobs 守卫 + `/data/.slr-seed-done` 标记）
- **OSS URI 双规范**：Flink/SQL 侧纯桶名 `oss://<bucket>/path` + `fs.oss.endpoint` 区域 dls（Paimon 拒绝 bucket 内嵌 authority）；bucket 内嵌 dls URI 仅 JindoFuse 挂载用
- **jindo-flink 与 flink-oss-fs-hadoop 互斥**：Flink lib 只能存在其一（flink role 有防御删除）
- **flink/sql/ 本体不含环境路径**：部署期 `scripts/render-sql.sh` 渲染（SLR_WAREHOUSE_BASE/SLR_IGGY_HOST/SLR_FLUSS_BOOTSTRAP）；改 SQL 后跑 `bash scripts/tests/test_render_sql.sh`
- **n8n 风控核心 = `n8n/workflows/decide.js` 纯函数**（14 项单测钉住，与已删除的 consensus 规则 1:1）；改完必须 `python3 n8n/build_workflow.py` 重建 JSON（同步门禁会拦截漂移）
- **n8n staticData 仅成功轮持久化**；决策状态事实源在 n8n SQLite，query-api state.json 只是展示镜像
- **配置零硬编码**：版本锁/端口/路径在 `iac/ansible/inventories/prod/group_vars/`；秘密在 vault.yml（键名清单 `iac/ansible/vault-example.yml`）；源码只读环境变量
- **顺序即语义**：inventory/tfvars 的 IP/主机名列表永不重排（ZK myid、Fluss tablet id 吃顺序）
- 提交信息遵循 Conventional Commits（中文描述）

## 目录速查

| 目录 | 内容 |
|---|---|
| `iac/terraform/` | modules(4，拷自 adtrader 微调) + environments/prod（OSS backend state） |
| `iac/ansible/` | 10 roles + 3 playbooks + group_vars（版本锁唯一事实源） |
| `iac/scripts/` | terraform output → inventory 生成（含 .bak 防护与占位符门禁） |
| `services/query-api/` | 瘦查询服务（pytest 门禁） |
| `n8n/` | decide.js（事实源）+ build_workflow.py + workflows/trading-decision.json（生成物）+ tests |
| `flink/sql/` | 7 个 SQL 作业（本体零环境耦合） |
| `scripts/` | download-jars（含 JindoSDK）/ render-sql / reconcile-snapshot / backfill-candles / backtest / extract-iggy-binary |
