# architecture.md — System Design

## Project: Streaming Lakehouse Reference

部署形态：阿里云 ECS 原生（systemd，无 Docker）· 湖仓 OSS-HDFS · 决策链 n8n · 全量 IaC。
迁移设计记录：`docs/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md`。

---

## Purpose

A personal crypto price monitoring, paper trading simulation, and data
engineering learning platform. Real market data. No real money.
Using high-frequency WebSockets, tiered streaming storage, vector similarity
search, and LLM-driven sentiment to build a "Market Time Machine."

---

## Deployment Topology

```
              cn-beijing VPC（复用现有 VPC/vSwitch，data source 只读引用）
              单安全组 sg-slr-prod（inner_access_policy=Accept ⇒ 三台全端口互通）
┌─────────────────────────────┐  ┌────────────────────────────┐  ┌─────────────────────────────┐
│ ecs-app-01 应用面 4C16G      │  │ ecs-data-01 数据面 8C32G    │  │ ecs-compute-01 计算面 8C32G  │
│ poller    :8000 ─TCP 8090───┼─▶│ Iggy 0.7.0 TCP:8090        │◀─┼─ Flink 1.20.3 JM:8081       │
│ bridge    :8001 ─TCP 8090───┼─▶│   HTTP:3000 /data/iggy     │  │   TM 8 slots metrics :9249  │
│ replay    :8002 (oneshot)   │  │ ZooKeeper 3.9.3 :2181 ◀────┼──┼─ (Flink HA quorum)          │
│ lancer    :8003/:8004       │  │ Fluss coord:9123 ◀─────────┼──┼─ (SQL connector)            │
│ lancer-indexer (timer 每小时)│  │   tablet:9124              │  │ reconcile timer (5min)      │
│ query-api :8009 (api+metrics)│ │   /data/fluss/{data,remote}│  │ /mnt/warehouse (fuse 读写)   │
│ Prometheus :9090 ─scrape────┼──┼────────────────────────────┼─▶│   (:9249)                   │
│ Grafana   :3001             │  │                            │  │                             │
│ n8n       :5678 (/metrics) ─┼──┼─HTTP 3000（Iggy 发单）──────▶│  │                             │
│ /mnt/warehouse (fuse 只读)   │  │                            │  │                             │
│ /data/lancer (LanceDB)      │  │                            │  │                             │
└──────┬──────────────────────┘  └────────────────────────────┘  └──────┬──────────────────────┘
       │ JindoFuse (bucket 内嵌 dls URI)                                  │ jindo-flink/JindoSDK (纯桶名 URI + dls endpoint)
       ▼                                                                 ▼
  OSS-HDFS 桶 slr-lakehouse-prod（HDFS 语义控制台带外开通）
  ├── warehouse/paimon/crypto.db/{ohlcv_1m,balance,trades,reconcile_balance.json}
  ├── warehouse/iceberg/crypto/ticks/…    ├── warehouse/backfill/
  └── flink/{checkpoints,savepoints,ha}
  备用桶 slr-lakehouse-prod-state（R1 回退：flink-oss-fs-hadoop 插件方案时启用）
  工件桶 slr-artifacts-prod（public-read：iggy 提取件等）
  公网出向：app-01→Coinbase WSS、DashScope；三台装机期下载
  公网入向：仅 SSH 白名单；Grafana/n8n/Flink UI 走 SSH 隧道
```

---

## Data Flow

```
Coinbase WebSocket
      │
      ▼
   poller ──▶ Iggy crypto/prices (3 partitions)
                 │
    ┌────────────┼───────────────┬──────────────────────┐
    ▼            ▼               ▼                      ▼
  bridge      Flink(3 jobs)   lancer(signal)          replay(按需)
    │            │               │                      │
    ▼            │               ▼                      ▼
 Prometheus      │        LanceDB 相似度匹配       Iggy crypto/replay
    │            ├─▶ Fluss crypto.ticks (hot)           │
    ▼            ├─▶ Paimon ohlcv_1m (warm)             ▼
  Grafana        └─▶ Iceberg ticks (cold)         Flink replay-pipeline
                                                        │
                                                        ▼
                                              Paimon replay_ohlcv_1m

决策环（每 60s，n8n trading-decision workflow）：
  lancer /api/signals/{pair} ─┐
  Prometheus crypto_price ────┼─▶ n8n: BuildPrompt → DashScope Qwen 情绪
  query-api /api/balance ─────┘        │
                                       ▼
                          RiskDecide（decide() 纯函数：入场/出场/风控熔断）
                                       │ BUY/SELL
                                       ▼
                     Iggy HTTP :3000 crypto/orders（messages_key=pair）
                                       │
                                       ▼
                     Flink clearing-house → Paimon trades/balance + Fluss executed_trades
                                       │
              reconcile timer(5min) ───┘ Flink SQL 查 balance → reconcile_balance.json（fuse）
                                       │
                                       ▼
                     query-api /api/balance /api/audit /api/candles → Grafana / n8n
```

### Key Data Paths

1. **Live ticks**: Coinbase → poller → Iggy → bridge → Prometheus → Grafana (1s scrape)
2. **Candles**: Iggy → Flink lakehouse-tier → Paimon `ohlcv_1m` → query-api(DuckDB via fuse) → Grafana 蜡烛图
3. **Archive**: Iggy → Flink → Iceberg `crypto.ticks`（日分区，parquet+zstd）
4. **Hot SQL**: Iggy → Flink fluss-hot-tier → Fluss `crypto.ticks`（bucket=4）
5. **Pattern**: Paimon → lancer-indexer(timer) → LanceDB → lancer(signal) `/api/signals`
6. **Trading loop**: n8n（信号+LLM+风控）→ Iggy orders → Flink clearing → Paimon trades/balance
7. **Reconciliation**: timer → Flink SQL(batch) → reconcile_balance.json → query-api → n8n/Grafana
8. **Replay**: Iceberg parquet → replay(oneshot) → Iggy replay → Flink → Paimon replay 表

---

## Data Integrity & Deduplication

- bridge 按 Iggy sequence 去重（`bridge_deduplicated_total`）
- Paimon `ohlcv_1m` 窗口聚合以 window_start+pair 为键幂等
- reconcile 校准：n8n Init 每轮读 query-api `/api/balance`，在 reconcile_balance.json 每次更新后采纳湖内 USD cash（consensus `_reconcile_from_ledger` 的核心语义）；positions 级 ghost/orphan 校准为延后项（偏差经 state 镜像 lake_cash/drift 可观测）
- 信号缺失语义：lancer 不可达/无匹配 → HOLD（不强平；与 consensus 的 similarity=0→SELL 有意不同，见 n8n/README「已知语义差异」）
- n8n staticData 仅整轮执行成功后持久化（失败轮不落账——保守语义，等价原 consensus 原子写）
- 订单幂等：order_id=UUID，湖内 trades PK 去重

---

## Component Responsibilities

### poller / bridge / replay / lancer（Python 3.12，systemd，零改动迁移）
环境变量配置（`docs/env-reference.md`）；warehouse 路径统一 `/mnt/warehouse`（JindoFuse）。
replay=oneshot 手动；lancer-indexer=oneshot+每小时 timer。

### Iggy 0.7.0（数据面）
二进制为 docker 镜像提取件（官方无发行资产，`scripts/extract-iggy-binary.sh` 可复现）。
TCP 8090（Python 客户端）+ HTTP 3000（n8n 发单：`POST /users/login` → Bearer →
`POST /streams/crypto/topics/orders/messages`，partitioning=messages_key(pair)）。
unit 带 `LimitMEMLOCK=infinity` + `AmbientCapabilities=CAP_SYS_NICE`（io_uring）。

### Apache Flink 1.20.3（计算面，JM+TM 同机 standalone；Iggy/Fluss/ZK 跨机连数据面）
3 个流作业（lakehouse-tier / fluss-hot-tier / clearing-house）+ 按需（ohlcv-candles /
replay-pipeline / import-backfill）。ZooKeeper HA（cluster-id `/streaming-lakehouse`）。
lib = 8 个 connector jar（`scripts/download-jars.sh` SHA-256 校验）+ JindoSDK 三件套
（jindo-flink-nextarch-full / jindo-sdk / jindo-core）；**lib 内禁止 flink-oss-fs-hadoop**（互斥）。
SQL 本体不含环境路径——部署期 `render-sql.sh` 渲染（warehouse→oss://，iggy/fluss host→数据面 IP）。

### Fluss / ZooKeeper / Paimon / Iceberg / LanceDB
职责同前（热层 SQL / 协调 / 温层聚合表 / 冷层 tick 归档 / 向量库）。
Fluss data+remote 均在 /data/fluss（本地盘，热层可重建）。
Paimon `balance` = aggregation merge engine（delta 语义，seed 只跑一次——
playbook_jobs 守卫 + `/data/.slr-seed-done` 标记双保险）。

### n8n（应用面 :5678）— 决策链唯一入口
workflow `slr-trading-decision`：Schedule 60s + killswitch Webhook 双触发器共享 staticData。
链路：Init → PromPrices/PromPriceLag/PromSignalLag/FetchBalance → SplitInBatches(每 pair)
→ FetchSignal → BuildPrompt → DashScope Qwen（json 输出，回退正则）→ RiskDecide
（decide()/updateEquity() 纯函数，`n8n/workflows/decide.js` 事实源，14 项单测）
→ Switch → IggyLogin → BuildOrder → PublishOrder → Annotate → CommitState
→ MirrorState(query-api) → IfNarrative(每 5 轮注解)。
风控规则与原 consensus 1:1（阈值见 decide.js DEFAULTS；只挡开仓、平仓永远放行）。

### query-api（应用面 :8009）— 只读查询 + 状态镜像
DuckDB 读 Paimon parquet（仅 append-only 表）：`/api/audit` `/api/candles/{pair}`；
reconcile JSON：`/api/balance`；n8n 镜像：`/api/state`(GET/POST 原子写) `/api/positions`
`/api/trades`；`/metrics` 把 state 转 `slr_*` 指标（equity/balance/circuit_breaker/
position_value/pnl/trades_total/signals_checked）供 Grafana trading dashboard。
不含决策逻辑，不连 Iggy。

### Reconciliation（计算面 systemd timer，5min）
`scripts/reconcile-snapshot.sh`：Flink SQL(batch) 查 Paimon balance（PK 表唯一权威读法）
→ tableau 解析 → 原子写 `/mnt/warehouse/paimon/crypto.db/reconcile_balance.json` →
取消遗留 SELECT 作业。

### Prometheus / Grafana（应用面）
Prometheus 抓取：bridge(1s)/poller/lancer/query-api/n8n(本机) + Flink TM :9249(跨机)。
Grafana 5 dashboards（live-ticks[home]/flink-pipeline/replay/lancer/trading），
infinity 数据源指向 query-api；trading 指标已从 consensus_* 平移为 slr_*。

---

## Port Map

| Port | Service | Host | 说明 |
|---|---|---|---|
| 8090/3000 | Iggy TCP/HTTP | data | 客户端 / n8n 发单 |
| 2181 | ZooKeeper | data | Flink HA + Fluss |
| 9123/9124 | Fluss coord/tablet | data | |
| 8081 | Flink JM REST/UI | compute | SSH 隧道（经 app-01 再跳 compute） |
| 9249 | Flink TM metrics | compute | Prometheus 跨机抓取 |
| 8000/8001/8002 | poller/bridge/replay metrics | app | |
| 8003/8004 | lancer API/metrics | app | |
| 8009 | query-api（api+metrics） | app | Grafana infinity 数据源 |
| 9090 | Prometheus | app | SSH 隧道 |
| 3001 | Grafana | app | SSH 隧道 |
| 5678 | n8n（UI+/metrics+webhook） | app | killswitch webhook |

已删除端口：8005/8006(analyst)、8007/8008(consensus)、8888(iggy-web-ui)。

## Storage Layout

| 位置 | 内容 | 持久性 |
|---|---|---|
| OSS-HDFS `slr-lakehouse-prod` | warehouse/{paimon,iceberg,backfill} + flink/{checkpoints,savepoints,ha} + canary/ | 持久（湖权威存储） |
| data:/data/iggy | Iggy 消息 | 重启可丢（topic 自动重建） |
| data:/data/fluss | Fluss data/remote | 热层可重建 |
| data:/data/zookeeper | ZK（Flink HA 元数据） | 持久（清掉=作业状态全丢） |
| app:/data/lancer | LanceDB 索引 | indexer timer 可重建 |
| app:/var/lib/n8n | n8n SQLite（workflow/凭据/staticData） | 持久（备份=拷文件）；staticData=决策事实源 |
| app:/var/lib/query-api/state.json | n8n 状态镜像 | 展示副本，可丢 |
| app:/data/prometheus | TSDB 7d | 滚动 |
| app:/data/grafana | dashboards/用户 | 持久 |

## Critical Constraints

1. **DuckDB 读 Paimon 只对 append-only 表安全**（ohlcv_1m）；PK+merge-engine 表（balance/trades 聚合值）必须走 Flink SQL（reconcile 脚本承担）
2. **Paimon balance 是 aggregation delta 语义**：seed 只能执行一次（playbook 守卫）
3. **Flink HA 会恢复所有作业**；彻底重置 = 清 ZK `/streaming-lakehouse` + 清 `oss://…/flink/ha`（见 runbook）
4. **Paimon 拒绝 bucket 内嵌 authority 的 oss URI**：SQL/Flink 侧一律 `oss://<bucket>/path` + `fs.oss.endpoint` 配区域 dls；bucket 内嵌 dls URI 仅 JindoFuse 挂载用
5. **jindo-flink 与 flink-oss-fs-hadoop 互斥**：lib 内只能存在其一（flink role 有防御性 delete）
6. **n8n staticData 仅成功轮持久化**：失败轮决策不落账（保守）；killswitch 兜底 = unpublish workflow
7. **顺序即语义**：inventory/私网 IP/主机名列表永不重排（generate-inventory 有 .bak 防护）

## Technology Decision Rationale

- **OSS-HDFS 而非普通 OSS**：湖格式（Paimon/Iceberg）依赖 HDFS 语义（原子 rename、目录原子性）；JindoFuse 同时让 Python 直读栈零改动
- **n8n 而非自研 Python**：决策链可视化、可编辑、执行历史内建；风控核心以纯函数（decide.js）保存可测试性
- **DashScope Qwen 而非本地 Ollama**：无 GPU 依赖、按 token 计费、OpenAI-compatible 可切换模型
- **systemd 而非容器**：Iggy io_uring 需求（memlock/SYS_NICE）裸机天然满足；单机单进程无编排需求
- **三台分层**：数据（Iggy/ZK/Fluss）、计算（Flink）、应用（Python/观测/n8n）三面资源与故障域分离；Flink 内存吃紧时可单独升配计算面
