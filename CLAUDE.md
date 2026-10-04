# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Streaming Lakehouse Reference：加密货币实时行情 → 分层流式存储 → 向量模式匹配 → LLM 分析 → 模拟盘交易的参考架构（无传统数据库，无真实资金）。所有服务通过 Docker Compose 运行；除 Docker 与宿主机 Ollama 外无外部依赖。

详细设计见 `architecture.md`（数据流图、组件职责、端口表、存储布局）；完整重置+历史回填见 `docs/clean-slate-guide.md`。

## 常用命令

```bash
# 首次启动
cp .env.example .env
./scripts/download-jars.sh        # 下载 Flink connector JARs（~250MB，SHA-256 校验）到 jars/
docker compose up -d

# 提交 Flink SQL 作业（容器重启后作业丢失，必须重新提交；顺序重要）
# 等 ~30s 让 Iggy/Fluss 就绪；clearing-house 需 consensus 先启动创建 crypto/orders topic
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/fluss-hot-tier.sql
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/lakehouse-tier.sql
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/clearing-house.sql

# 首次种子余额（仅执行一次！Paimon aggregation 表，重复 INSERT 会重复累加）
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/seed-balance.sql

# 验证
curl -s http://localhost:8081/jobs   # 3 个 Flink 作业应为 RUNNING
curl -s http://localhost:8007/api/health  # consensus
curl -s http://localhost:8005/api/health  # analyst
curl -s http://localhost:8003/api/health  # lancer

# 可选操作（compose profiles）
docker compose --profile index run --rm lancer-indexer   # 重建 LanceDB 索引
docker compose --profile replay up replay -d             # 历史回放（.env 配 REPLAY_SPEED/DATE/PAIRS）
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/replay-pipeline.sql

# 宿主机脚本（需 pip 依赖，见各脚本 docstring）
python3 scripts/backtest.py --sweep    # 离线回测，直接读 parquet，无需服务运行
./scripts/reconcile-snapshot.sh        # Paimon → JSON 对账快照（生产由 cron 每 5 分钟执行）
```

无测试框架。验证方式 = `docker compose logs <service>` + 上述 health/jobs 接口 + Grafana（http://localhost:3001, admin/admin）。

## 架构要点

数据流：Coinbase WebSocket → **poller** → **Iggy**（`crypto/prices`、`crypto/orders`、`crypto/replay` 三个 topic）→ 四条下游路径：

1. **bridge**：Iggy → Prometheus 指标（按 sequence 去重）→ Grafana
2. **Flink**（3 个流作业）：热层 Iggy→**Fluss**；湖仓 Iggy→**Paimon** `ohlcv_1m` + **Iceberg** tick 归档；清算 Iggy orders→Paimon `balance`/`trades` + Fluss `executed_trades`
3. **lancer**：双模式——indexer（Paimon→LanceDB 批量建索引）/ signal（实时 Iggy→LanceDB 相似度匹配，FastAPI :8003）
4. **analyst**（Ollama LLM 情绪）+ lancer 信号 → **consensus**（模拟盘引擎，发 OrderRequest 回 Iggy :8007）

每个 Python 服务 = 一个目录（`poller/` `bridge/` `replay/` `lancer/` `analyst/` `consensus/`），单 `main.py` + Dockerfile（`python:3.12-slim`）。Flink SQL 全部在 `flink/sql/`，自包含、经 sql-client 提交。Flink 镜像 `flink/Dockerfile` 打包 `jars/` 下的 connector。

### 关键约束（改代码前必读）

- **DuckDB 读 Paimon 只对 append-only 表安全**（`ohlcv_1m`）。PK + merge-engine 表（`balance`、`trades`）会读到 compaction 前的过期快照，必须走 Flink SQL。
- **Paimon `balance` 是 aggregation merge engine**：每条 INSERT 是增量（delta）而非事实，seed 操作只能执行一次。
- **Flink HA（ZooKeeper）会恢复所有作业**，包括已完成的一次性 seed 作业。流程：seed → 验证 → 重启集群 → 再提交生产作业。
- **analyst 和 consensus 用 `network_mode: host`**（访问宿主机 Ollama/Iggy）；Prometheus 抓取它们时用 `172.17.0.1`（Docker 网桥网关）。
- **重启后状态**：Iggy topic/消息、Flink 作业、consensus 内存仓位会丢（自动重建/需重提交）；Paimon/Iceberg（`flink-warehouse` volume）、LanceDB/Prometheus/Grafana（`./data/` bind mount）持久。
- 所有配置走 `.env`（模板 `.env.example` 已入库），源码禁止硬编码。
- 提交信息遵循 Conventional Commits（`feat:` `fix:` `docs:` 等）。
