# Streaming Lakehouse Reference

A streaming data platform processing live crypto ticks through six tiers
with no traditional database — deployed **natively on Alibaba Cloud ECS**
(systemd, no Docker), with the lakehouse on **OSS-HDFS** and the trading
decision chain fully owned by **n8n + DashScope Qwen**.

**No real money. Real data. Real architecture.**

---

## What This Is

A platform for watching cryptocurrency markets, simulating trading decisions
(paper trading), and learning modern data engineering through hands-on use.
Everything — cloud resources, software installation, configuration — is
Infrastructure-as-Code (Terraform + Ansible, self-contained in `iac/`).

## What This Is Not

- A financial trading platform
- Investment advice
- Production software

---

## The Stack

| Layer | Technology |
|---|---|
| Data source | Coinbase Exchange WebSocket（6 pairs，sub-second ticks） |
| Event bus | Apache Iggy 0.7.0（`crypto/prices` `crypto/orders` `crypto/replay`） |
| Stream processing | Apache Flink 1.20.3（SQL，8 slots，ZooKeeper HA，state 全落 OSS-HDFS） |
| Hot tier | Apache Fluss 0.9.0-incubating |
| Lakehouse | Apache Paimon（`ohlcv_1m`/`balance`/`trades`）+ Apache Iceberg（tick 归档），warehouse = **OSS-HDFS**（JindoSDK） |
| Vector matching | LanceDB（lancer indexer/signal，pyarrow/DuckDB 经 JindoFuse 直读湖内 parquet） |
| Decision chain | **n8n 2.41.6**（信号→DashScope Qwen 情绪→风控→Iggy HTTP 发单；替代原 analyst+consensus） |
| Query surface | **query-api**（FastAPI :8009，DuckDB 只读查询 + n8n 状态镜像 + `slr_*` 指标） |
| Observability | Prometheus v2.53.0 + Grafana 11.1.0（5 dashboards） |
| IaC | Terraform（alicloud，OSS backend state）+ Ansible（systemd 原生部署） |

## Deployment Topology（cn-beijing）

```
ecs-app-01 应用面 4C16G              ecs-data-01 数据面 8C32G
  poller/bridge/lancer(+indexer timer)   Iggy :8090/:3000 · ZooKeeper :2181
  replay(oneshot) · query-api :8009      Fluss :9123/:9124 · Flink JM :8081/TM
  Prometheus :9090 · Grafana :3001       reconcile timer(5min)
  n8n :5678 · /mnt/warehouse(fuse ro)    /mnt/warehouse(fuse rw)
        └────────────┬────────────────────────┘
          OSS-HDFS 桶 slr-lakehouse-prod
          warehouse/{paimon,iceberg,backfill} + flink/{checkpoints,savepoints,ha}
```

公网入向仅 SSH 白名单；UI 一律 SSH 隧道（详见 runbook）。

---

## Repository Layout

```
iac/terraform/     资源供给（ECS×2 / OSS×3 / RAM role / SG，environments/prod）
iac/ansible/       软件安装与配置（10 roles / 3 playbooks / group_vars 版本锁）
iac/scripts/       terraform output → ansible inventory 生成管道
flink/sql/         Flink SQL 作业（部署期经 scripts/render-sql.sh 渲染路径）
poller/ bridge/ replay/ lancer/   Python 服务（systemd 部署，零 compose 依赖）
services/query-api/               瘦只读查询服务（替代 consensus 查询面）
n8n/               trading-decision workflow（decide.js 事实源 + 构建脚本 + 测试）
scripts/           download-jars / render-sql / reconcile-snapshot / backfill / extract-iggy-binary
docs/              runbook-ecs-deploy（部署手册）· env-reference · clean-slate-guide · specs/ · plans/
```

## Deploy

完整步骤（含 OSS-HDFS 控制台开通、金丝雀门槛 G1-G3、n8n owner 带外步骤、端到端验证）：

**→ [`docs/runbook-ecs-deploy.md`](docs/runbook-ecs-deploy.md)**

摘要：

```bash
./scripts/extract-iggy-binary.sh dist/          # 一次性：Iggy 二进制提取（需本机 docker）
cd iac/terraform/environments/prod && terraform init -backend-config=backend.conf && terraform apply
./iac/scripts/generate-inventory.sh prod
cd iac/ansible
ansible-playbook playbook_dataplane.yaml        # 金丝雀 G1/G2/G3（runbook）
ansible-playbook playbook_jobs.yaml             # Flink 作业（seed 守卫内置）
ansible-playbook playbook_appplane.yaml         # Python/n8n/监控
```

## Verify

```bash
curl -s http://localhost:8081/jobs/overview    # ≥3 流作业 RUNNING（数据面）
curl -s http://localhost:8009/api/health       # query-api
curl -s http://localhost:8009/api/state        # n8n 决策状态镜像（updated_at 滚动）
# Grafana :3001（SSH 隧道）5 dashboards；n8n :5678 Executions 60s 周期 success
```

## Tests（本地门禁）

```bash
bash scripts/tests/test_render_sql.sh
PYTHONPATH=services/query-api/_deps python3 -m pytest services/query-api n8n/tests/test_workflow_json.py iac/scripts/tests -q
node --test n8n/tests/*.mjs                    # decide() 风控 14 例
terraform -chdir=iac/terraform/environments/prod validate
ansible-playbook -i iac/ansible/tests/hosts.dummy iac/ansible/playbook_dataplane.yaml --syntax-check
```

---

## Documentation

- [`architecture.md`](architecture.md) — 数据流、组件职责、端口表、存储布局
- [`docs/runbook-ecs-deploy.md`](docs/runbook-ecs-deploy.md) — 部署/验证/回退手册
- [`docs/env-reference.md`](docs/env-reference.md) — 全部配置项归属（替代 .env.example）
- [`docs/clean-slate-guide.md`](docs/clean-slate-guide.md) — 彻底重置与历史回填
- [`docs/specs/`](docs/specs/) · [`docs/plans/`](docs/plans/) — 迁移设计与实施计划

## History

Phases 1–15 built this system on Docker Compose with a local Ollama analyst
and a Python consensus engine. The 2026-10 ECS migration replaced the
runtime (systemd), the lake storage (OSS-HDFS), and the decision chain
(n8n + Qwen); the compose path was removed entirely. Design record:
`docs/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md`.
