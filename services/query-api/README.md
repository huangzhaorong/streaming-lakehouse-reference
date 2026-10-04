# query-api

瘦只读查询服务（替代原 consensus 的查询面，决策链已迁 n8n）。

- **查询**：`/api/audit`、`/api/candles/{pair}`（DuckDB 读 Paimon parquet，经 JindoFuse 挂载 `/mnt/warehouse`）；`/api/balance`（读 reconcile timer 产出的 JSON——balance 为 PK 表禁止直读 parquet）
- **状态镜像**：n8n workflow 每轮 `POST /api/state`，Grafana/运维 `GET /api/state`、`/api/positions`、`/api/trades`。原子写（tmp+rename）
- **不做**：决策、风控、连 Iggy、下单——那些在 n8n `trading-decision` workflow

- **指标**：`GET /metrics`（同端口 8009）——把 n8n 镜像的 state.json 转为 `slr_*` Prometheus 指标（equity/balance/circuit_breaker/position_value/pnl/trades_total/signals_checked），替代原 consensus_* 指标面，trading dashboard 已 sed 平移改名

环境变量：`QUERY_API_PORT`(8009)、`QUERY_API_WAREHOUSE_PATH`(/mnt/warehouse/paimon/crypto.db)、`QUERY_API_STATE_DIR`(/var/lib/query-api)、`TRADING_PAIRS`

本地测试（无 venv 也可）：`uv pip install --target _deps -r requirements.txt pytest httpx && PYTHONPATH=_deps python3 -m pytest test_query_api.py -q`

ponytail: state.json 为单文件全局状态，写方仅 n8n 单 workflow（60s 串行）；若未来多写方需加锁/换存储。
