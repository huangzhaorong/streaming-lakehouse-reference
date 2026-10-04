# 环境变量参考（替代原 .env.example）

配置唯一事实源：`iac/ansible/inventories/prod/group_vars/*.yml`（明文）+ `inventories/prod/vault.yml`（秘密，ansible-vault）。
运行时下发：各服务 systemd EnvironmentFile（`/etc/slr/<name>.env`，0600）。**源码零硬编码，禁止在目标机手改 env 文件**（重跑 playbook 覆盖）。

## 原 .env.example → 新归属对照

| 原变量 | 新归属 | 说明 |
|---|---|---|
| `COINBASE_WS_URL` | appplane.yml → poller env | 不变 |
| `TRADING_PAIRS` | all.yml `trading_pairs` | poller/lancer/query-api/n8n(SLR_TRADING_PAIRS) 共用 |
| `IGGY_HOST` | appplane.yml `data_ip`（跨机）/ flink role 渲染 127.0.0.1（SQL） | compose 服务名 → IP |
| `IGGY_TCP_PORT`/`IGGY_HTTP_PORT` | all.yml `iggy_tcp_port: 8090` / `iggy_http_port: 3000` | |
| `IGGY_USERNAME`/`IGGY_PASSWORD` | all.yml `iggy_root_username: admin` + **vault** `vault_iggy_password` | 原默认 iggy/iggy 废除 |
| `IGGY_STREAM`/`IGGY_TOPIC`/`IGGY_TOPIC_PARTITIONS`/`IGGY_REPLAY_TOPIC`/`IGGY_ORDERS_TOPIC` | all.yml `iggy_stream` + 各服务 env | |
| `IGGY_UI_PORT` | 已删除（iggy-web-ui 不部署） | |
| `POLLER_METRICS_PORT`/`BRIDGE_METRICS_PORT`/`BRIDGE_POLL_INTERVAL`/`REPLAY_*` | appplane.yml `slr_python_services[].env` | |
| `LANCER_MODE`/`LANCER_WINDOW_SIZE`/`LANCER_TOP_K`/`LANCER_SIMILARITY_THRESHOLD`/`LANCER_MIN_OUTCOME_PCT`/`LANCER_API_PORT`/`LANCER_METRICS_PORT`/`LANCER_DB_PATH` | appplane.yml lancer/lancer-indexer env | `LANCER_DB_PATH=/data/lancer` |
| `LANCER_WAREHOUSE_PATH`（隐含） | appplane.yml：`{{ jindofuse_mount }}/paimon/crypto.db/ohlcv_1m` | fuse 挂载路径 |
| `REPLAY_WAREHOUSE_PATH`（隐含） | appplane.yml：`{{ jindofuse_mount }}/iceberg/crypto/ticks/data` | |
| `OLLAMA_URL`/`OLLAMA_MODEL` | **已删除** → n8n env `SLR_LLM_MODEL`（DashScope Qwen，credential 存 API key） | |
| `ANALYST_*` | **已删除**（analyst 由 n8n LLM 节点取代） | 节奏=workflow Schedule 60s |
| `CONSENSUS_*`（阈值/风控全套） | **n8n/workflows/decide.js `DEFAULTS`**（单文件可 grep，14 项单测钉住） | ENTER=0.20 EXIT=0.10 SIZE=50 DRAWDOWN=2% 等逐字保留 |
| `CONSENSUS_API_PORT`/`METRICS_PORT` | **已删除** → query-api `QUERY_API_PORT: 8009`（/metrics 同端口） | |
| `CONSENSUS_WAREHOUSE_PATH`/`STATE_DIR` | query-api env `QUERY_API_WAREHOUSE_PATH`/`QUERY_API_STATE_DIR` | |
| `PROMETHEUS_URL`/`GRAFANA_URL`/`GRAFANA_USER`/`GRAFANA_PASSWORD` | appplane.yml（localhost URL）+ **vault** `vault_grafana_admin_password` | n8n 用 `SLR_PROM_URL`/`SLR_GRAFANA_URL` |
| `GF_SECURITY_ADMIN_PASSWORD` | **vault** `vault_grafana_admin_password` | |

## n8n 运行时变量（/etc/slr/n8n.env，n8n role 渲染）

`N8N_USER_FOLDER` `N8N_ENCRYPTION_KEY`(vault) `N8N_PORT=5678` `N8N_METRICS=true` `N8N_BLOCK_ENV_ACCESS_IN_NODE=false`；
`SLR_TRADING_PAIRS` `SLR_PROM_URL` `SLR_LANCER_URL` `SLR_QUERY_API_URL` `SLR_IGGY_HTTP_URL`(http://data_ip:3000) `SLR_IGGY_USER` `SLR_IGGY_PASSWORD`(vault) `SLR_GRAFANA_URL` `SLR_LLM_MODEL`(默认 qwen-plus，可切 qwen-turbo/qwen-max)。

## Flink 侧（flink-conf.yaml.j2 渲染，非 env）

warehouse/checkpoint/savepoint/HA 路径 = `oss://{{ slr_bucket }}/...`；`fs.oss.endpoint={{ oss_dls_endpoint }}`；RAM 免 AK 三键（ECS_ROLE/JSON）；R2 回退 AK 块由 vault 变量触发。
SQL 渲染变量（render-sql.sh）：`SLR_WAREHOUSE_BASE` `SLR_IGGY_HOST=127.0.0.1` `SLR_FLUSS_BOOTSTRAP=127.0.0.1:9123`（flink role 自动注入）。

## vault 键名清单（inventories/prod/vault.yml）

`vault_iggy_password` `vault_grafana_admin_password` `vault_n8n_encryption_key` `vault_dashscope_api_key`；回退：`vault_oss_access_key_id` `vault_oss_access_key_secret`（默认空=用 ECS RAM role 免 AK）。
