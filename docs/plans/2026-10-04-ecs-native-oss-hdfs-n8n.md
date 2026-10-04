# ECS 原生部署 + OSS-HDFS + n8n 决策链 + IaC 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 streaming-lakehouse-reference 从 Docker Compose 迁移为阿里云 ECS 原生部署（systemd），warehouse 迁 OSS-HDFS，n8n 彻底替代 analyst+consensus 决策链，全部资源与部署 Terraform+Ansible IaC 化（自包含 `iac/`）。

**Architecture:** 两台 ECS（数据面 8C32G：Iggy/ZK/Fluss/Flink；应用面 4C16G：Python 服务/n8n/query-api/Prometheus/Grafana）。Flink 经 JindoSDK 6.10.8 直写 OSS-HDFS（纯桶名 URI + 区域 dls endpoint）；应用面 JindoFuse 只读挂载 `/mnt/warehouse` 使 Python pyarrow/DuckDB 直读零改动。n8n 单 workflow（Schedule 60s + killswitch Webhook）承载全部风控与下单（Iggy HTTP API）。瘦 query-api（FastAPI :8009）承接 DuckDB 只读查询 + n8n 状态镜像。

**Tech Stack:** Terraform(alicloud provider, OSS backend)、Ansible、Flink 1.20.3、JindoSDK 6.10.8、Iggy 0.7.0、Fluss 0.9.0-incubating、ZooKeeper 3.9.3、Prometheus v2.53.0、Grafana 11.1.0、n8n 2.41.6(Node 24)、Python 3.12、FastAPI/DuckDB、DashScope Qwen。

**Spec:** `docs/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md`（本仓库内，已提交）

## Global Constraints

- 版本锁定（group_vars/all.yml 为唯一事实源）：Flink 1.20.3 / Iggy 0.7.0 / Fluss 0.9.0-incubating / ZooKeeper 3.9.3 / Prometheus v2.53.0 / Grafana 11.1.0 / n8n 2.41.6 / Node 24 LTS / JindoSDK 6.10.8 / Python 3.12
- region=cn-beijing；bucket：`slr-lakehouse-prod`(private, OSS-HDFS 控制台带外开通)、`slr-artifacts-prod`(public-read)
- Flink/SQL 侧 OSS URI 一律纯桶名：`oss://slr-lakehouse-prod/warehouse/...`；`fs.oss.endpoint: cn-beijing.oss-dls.aliyuncs.com`；**lib 中禁止存在 flink-oss-fs-hadoop jar**；JindoFuse 挂载 URI 才用 bucket 内嵌 dls 形式
- RAM 免 AK：`fs.oss.provider.endpoint: ECS_ROLE` + `fs.oss.provider.format: JSON`（回退 AK+vault）
- 源码/配置零硬编码秘密：一律 ansible-vault（iggy_password、grafana_admin_password、n8n_encryption_key、dashscope_api_key）
- poller/bridge/replay/lancer 及 flink/sql/ 本体零代码改动；提交信息 Conventional Commits 中文
- DuckDB 只读 append-only 表（ohlcv_1m）；balance/trades PK 表只能走 Flink SQL（reconcile 脚本）
- 风控阈值（与 consensus/main.py 逐字一致）：ENTER_SIM=0.20、EXIT_SIM=0.10、SIZE=$50、MAX_SIGNAL_AGE=120s、MAX_PRICE_AGE=30s、心跳 price_lag>60s / signal_lag>180s、MAX_DAILY_DRAWDOWN=2%、TAKER_FEE=0.001、SLIPPAGE=0.0005、MIN_VIABILITY_RATIO=1.5（uneconomic 阈值=0.45%×avg_max_outcome）、熔断/killswitch **只挡开仓，平仓永远放行**；BUY 另需 avg_outcome>0 且 cash≥50

## Review Focus

1. **jindo-flink 6.10.8 × Flink 1.20.3 兼容性**（官方仅承诺 <1.16）——本地不可测；由部署期 G1 金丝雀把守（runbook Task 12），失败回退普通 OSS endpoint + flink-oss-fs-hadoop 插件
2. **n8n decide() 与 consensus 风控等价性**——Task 5 用 node 对 workflow JSON 内嵌 jsCode 做单测钉住全部 BUY/SELL/BLOCKED/熔断分支
3. **render-sql.sh 替换完备性**——Task 2 测试断言渲染产物中 `grep -c "opt/flink/warehouse\|'iggy'"` 为 0
4. **fuse 上 DuckDB/pyarrow 随机读**——部署期 G3 金丝雀把守（runbook Task 12）；query-api 代码路径与普通 FS 相同
5. **n8n staticData 仅成功执行后持久化**（killswitch webhook 失败即丢）——Task 5 workflow 中 killswitch 分支独立提交 + Task 12 runbook 提供 `n8n unpublish:workflow` 兜底

---

### Task 1: extract-iggy-binary.sh（Iggy 二进制提取脚本）

**Files:**
- Create: `scripts/extract-iggy-binary.sh`
- Test: bash -n + dry-run 模式

**Interfaces:**
- Produces: `iggy-0.7.0-linux-x86_64.tar.gz`（含 `iggy-server`、`iggy` CLI、`configs/server.toml`）+ `.sha256`；tarball 命名被 Task 8 iggy role 的 `iggy_artifact_url`/`iggy_artifact_sha256` 变量消费

- [ ] **Step 1: 写脚本**（核心逻辑）：

```bash
#!/bin/bash
# 从 apache/iggy:0.7.0 镜像一次性提取原生二进制（官方无发行资产）。
# 用法: ./scripts/extract-iggy-binary.sh [输出目录，默认 dist/]
set -euo pipefail
IMAGE="apache/iggy:0.7.0"; OUT="${1:-dist}"; NAME="iggy-0.7.0-linux-x86_64"
mkdir -p "$OUT"; TMP=$(mktemp -d); trap 'rm -rf "$TMP" ; docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT
CID=$(docker create "$IMAGE")
docker cp "$CID":/iggy-server "$TMP/iggy-server"
docker cp "$CID":/iggy "$TMP/iggy"            # CLI
docker cp "$CID":/configs "$TMP/configs"
tar -C "$TMP" -czf "$OUT/$NAME.tar.gz" iggy-server iggy configs
(cd "$OUT" && shasum -a 256 "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
echo "OK: $OUT/$NAME.tar.gz"; cat "$OUT/$NAME.tar.gz.sha256"
```

注意：容器内实际路径实施时用 `docker run --rm --entrypoint sh apache/iggy:0.7.0 -c 'ls / /configs'` 核对（镜像 WORKDIR 为 /app 时路径可能是 /app/iggy-server），以实测为准修正 docker cp 源路径。

- [ ] **Step 2: 语法检查**：`bash -n scripts/extract-iggy-binary.sh` → Expected: 无输出（通过）
- [ ] **Step 3: 本机有 docker 则实跑**：`./scripts/extract-iggy-binary.sh` → Expected: 输出 OK + sha256；`tar tzf dist/iggy-*.tar.gz` 列出 3 项。无 docker 则跳过实跑（部署前由操作者执行），在脚本头注释注明
- [ ] **Step 4: Commit**：`git add scripts/extract-iggy-binary.sh && git commit -m "feat(scripts): 新增 Iggy 0.7.0 二进制提取脚本（官方无发行资产）"`

### Task 2: render-sql.sh（Flink SQL 部署期渲染）

**Files:**
- Create: `scripts/render-sql.sh`
- Create: `scripts/tests/test_render_sql.sh`

**Interfaces:**
- Produces: `SLR_WAREHOUSE_BASE`/`SLR_IGGY_HOST` 两环境变量驱动的渲染器，`render-sql.sh <src_dir> <out_dir>`；被 Task 9 flink role 在数据面调用（渲染到 /opt/flink/sql/）
- Consumes: `flink/sql/*.sql` 现状硬编码：`/opt/flink/warehouse/paimon`、`/opt/flink/warehouse/iceberg`、`file:///opt/flink/warehouse/backfill/`、`'host'      = 'iggy'`

- [ ] **Step 1: 写失败测试** `scripts/tests/test_render_sql.sh`：

```bash
#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
SLR_WAREHOUSE_BASE="oss://test-bucket/warehouse" SLR_IGGY_HOST="10.0.0.9" \
  ./scripts/render-sql.sh flink/sql "$OUT"
fail=0
grep -rl "opt/flink/warehouse" "$OUT" && { echo "FAIL: 残留 warehouse 本地路径"; fail=1; }
grep -rl "= 'iggy'" "$OUT" && { echo "FAIL: 残留 iggy 主机名"; fail=1; }
grep -q "oss://test-bucket/warehouse/paimon" "$OUT/lakehouse-tier.sql" || { echo "FAIL: paimon 未替换"; fail=1; }
grep -q "oss://test-bucket/warehouse/iceberg" "$OUT/lakehouse-tier.sql" || { echo "FAIL: iceberg 未替换"; fail=1; }
grep -q "'host'      = '10.0.0.9'" "$OUT/clearing-house.sql" || { echo "FAIL: iggy host 未替换"; fail=1; }
grep -q "oss://test-bucket/warehouse/backfill/" "$OUT/import-backfill.sql" || { echo "FAIL: backfill 未替换"; fail=1; }
ls "$OUT" | wc -l | grep -q "^7$" || { echo "FAIL: 应渲染 7 个 SQL"; fail=1; }
[ $fail -eq 0 ] && echo "ALL PASS"
```

- [ ] **Step 2: 运行确认失败**：`bash scripts/tests/test_render_sql.sh` → Expected: FAIL（render-sql.sh 不存在）
- [ ] **Step 3: 实现** `scripts/render-sql.sh`：

```bash
#!/bin/bash
# 部署期渲染 Flink SQL：warehouse 本地路径 → OSS-HDFS，'iggy' → 实际主机。
# 用法: SLR_WAREHOUSE_BASE=oss://<bucket>/warehouse SLR_IGGY_HOST=<ip> render-sql.sh <src> <out>
set -euo pipefail
SRC="${1:?用法: render-sql.sh <src_dir> <out_dir>}"; OUT="${2:?}"
: "${SLR_WAREHOUSE_BASE:?需设置 SLR_WAREHOUSE_BASE，如 oss://slr-lakehouse-prod/warehouse}"
: "${SLR_IGGY_HOST:?需设置 SLR_IGGY_HOST，如 127.0.0.1}"
mkdir -p "$OUT"
for f in "$SRC"/*.sql; do
  sed -e "s|file:///opt/flink/warehouse/|${SLR_WAREHOUSE_BASE}/|g" \
      -e "s|/opt/flink/warehouse|${SLR_WAREHOUSE_BASE}|g" \
      -e "s|'host'\( *= *\)'iggy'|'host'\1'${SLR_IGGY_HOST}'|" "$f" > "$OUT/$(basename "$f")"
done
echo "rendered $(ls "$OUT" | wc -l | tr -d ' ') sql files → $OUT"
```

注意 `file:///opt/flink/lib/*.jar` 的 pipeline.jars 行**不得**被误替换——替换模式仅匹配 `warehouse`，lib jar 路径不含该词，天然安全（测试第 2、3 行断言覆盖）。

- [ ] **Step 4: 运行确认通过**：`bash scripts/tests/test_render_sql.sh` → Expected: `ALL PASS`
- [ ] **Step 5: Commit**：`git add scripts/render-sql.sh scripts/tests && git commit -m "feat(scripts): 新增 Flink SQL 部署期渲染器（warehouse→OSS-HDFS，iggy host 参数化）"`

### Task 3: reconcile-snapshot.sh 裸机化

**Files:**
- Modify: `scripts/reconcile-snapshot.sh`

**Interfaces:**
- Produces: 数据面 systemd timer 每 5min 执行的脚本；写 `${SLR_WAREHOUSE_BASE_MOUNT:-/mnt/warehouse}/paimon/crypto.db/reconcile_{balance,trades}.json`；被 Task 9 flink role 的 timer unit 消费
- Consumes: Task 2 的路径约定（SQL 内 warehouse 用 `SLR_PAIMON_WAREHOUSE` 环境变量注入，默认 `oss://slr-lakehouse-prod/warehouse/paimon`）

- [ ] **Step 1: 修改**：三处变更——
  1. `DEST="/opt/flink/warehouse/paimon/crypto.db"` → `DEST="${RECONCILE_DEST:-/mnt/warehouse/paimon/crypto.db}"`（数据面 fuse rw 挂载点）
  2. SQL 内 `'warehouse' = '/opt/flink/warehouse/paimon'` → `'warehouse' = '${SLR_PAIMON_WAREHOUSE:-oss://slr-lakehouse-prod/warehouse/paimon}'`（heredoc 变量展开，BALANCE_SQL 与 trades SQL 两处）
  3. `docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded` → `${FLINK_HOME:-/opt/flink}/bin/sql-client.sh embedded`
  同步更新头注释（cron → systemd timer：`reconcile-snapshot.timer`）
- [ ] **Step 2: 验证**：`bash -n scripts/reconcile-snapshot.sh && ! grep -n "docker" scripts/reconcile-snapshot.sh` → Expected: 语法通过、无 docker 残留
- [ ] **Step 3: Commit**：`git commit -am "refactor(scripts): reconcile-snapshot 裸机化（去 docker exec，warehouse 路径参数化）"`

### Task 4: services/query-api（瘦只读查询服务）

**Files:**
- Create: `services/query-api/main.py`、`services/query-api/requirements.txt`、`services/query-api/test_query_api.py`、`services/query-api/README.md`
- 参考搬移源: `consensus/main.py:595-684`（audit/candles 的 DuckDB 查询逐字搬移；positions/trades/health/killswitch 语义改造见下）

**Interfaces:**
- Produces（被 Task 5 n8n、Task 9 prometheus/grafana/python-service role、Task 10 dashboard 清理消费）:
  - `GET /api/health` → `{"status":"ok","pairs":[...]}`
  - `GET /api/audit` → consensus 同名端点逐字（DuckDB 读 `{WAREHOUSE_PATH}/trades/**/data-*.parquet`）
  - `GET /api/candles/{pair}?minutes=60` → consensus 同名端点逐字（读 `{WAREHOUSE_PATH}/ohlcv_1m`）
  - `GET /api/balance` → 读 `{WAREHOUSE_PATH}/reconcile_balance.json` → `{"balances":{"USD":1000.0,...},"as_of":<mtime>}`
  - `GET /api/trades` / `GET /api/positions` → 读 state.json 的 `trade_log[-50:]` / `positions`（n8n 镜像）
  - `GET/POST /api/state` → GET 返回 state.json 全量；POST body 原子写（tmp+rename）`STATE_DIR/state.json`
  - `GET /metrics`（prometheus_client，端口 `QUERY_API_METRICS_PORT` 默认 8010）
  - 环境变量：`QUERY_API_PORT`(8009)、`QUERY_API_METRICS_PORT`(8010)、`QUERY_API_WAREHOUSE_PATH`(/mnt/warehouse/paimon/crypto.db)、`QUERY_API_STATE_DIR`(/var/lib/query-api)、`TRADING_PAIRS`
- Consumes: 无（纯读 + state 落盘；不连 Iggy，不含决策）

- [ ] **Step 1: 写失败测试** `services/query-api/test_query_api.py`：

```python
import json, os, tempfile, time
import pyarrow as pa, pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

@pytest.fixture()
def client(tmp_path, monkeypatch):
    wh = tmp_path / "wh"; wh.mkdir()
    # 假 ohlcv_1m parquet（append-only，窗口列名与 Paimon 一致）
    d = wh / "ohlcv_1m" / "bucket-0"; d.mkdir(parents=True)
    pq.write_table(pa.table({
        "pair": ["BTC-USD"], "window_start": [int(time.time() * 1000)],
        "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5],
        "tick_count": [10]}), d / "data-0.parquet")
    (wh / "reconcile_balance.json").write_text(json.dumps({"USD": 950.0, "BTC": 0.01}))
    st = tmp_path / "state"; st.mkdir()
    monkeypatch.setenv("QUERY_API_WAREHOUSE_PATH", str(wh))
    monkeypatch.setenv("QUERY_API_STATE_DIR", str(st))
    import importlib, main
    importlib.reload(main)
    return TestClient(main.app)

def test_health(client):
    r = client.get("/api/health"); assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_balance_from_reconcile_json(client):
    r = client.get("/api/balance"); assert r.status_code == 200
    assert r.json()["balances"]["USD"] == 950.0

def test_state_roundtrip_atomic(client):
    payload = {"positions": {"BTC-USD": {"quantity": 0.01, "entry_price": 100.0}},
               "cash": 900.0, "killswitch": False, "trade_log": [],
               "day_baseline": {"date": "2026-10-04", "equity": 1000.0}}
    assert client.post("/api/state", json=payload).status_code == 200
    got = client.get("/api/state").json()
    assert got["cash"] == 900.0
    assert client.get("/api/positions").json()["positions"]["BTC-USD"]["quantity"] == 0.01
    assert client.get("/api/trades").json() == []

def test_state_get_empty_defaults(client):
    got = client.get("/api/state").json()
    assert got == {} or got.get("positions", {}) == {}   # 无 state 文件时不 500

def test_audit_no_trades_returns_error_note(client):
    r = client.get("/api/audit")
    assert r.status_code == 200 and "error" in r.json()  # trades 表不存在→优雅降级
```

- [ ] **Step 2: 运行确认失败**：`cd services/query-api && python3 -m venv .venv && .venv/bin/pip install -q fastapi uvicorn httpx duckdb pyarrow prometheus-client pytest && .venv/bin/pytest test_query_api.py -x -q` → Expected: FAIL（main 不存在，collection error）
- [ ] **Step 3: 实现 main.py**（~200 行）：搬移 consensus `audit()`（595-622）与 `candles()`（642-673）逐字（`WAREHOUSE_PATH` 改读 `QUERY_API_WAREHOUSE_PATH`；candles 的 `window_start` 比较沿用原 SQL）；新写 balance/state/positions/trades/health 端点（state 原子写：`tempfile.NamedTemporaryFile(dir=STATE_DIR, delete=False)` + `os.replace`）；`prometheus_client.start_http_server(QUERY_API_METRICS_PORT)`；`uvicorn.run(app, host="0.0.0.0", port=QUERY_API_PORT)`。requirements.txt：`fastapi\nuvicorn\nduckdb\npyarrow\nprometheus-client\nhttpx`
- [ ] **Step 4: 运行确认通过**：同 Step 2 命令 → Expected: `5 passed`
- [ ] **Step 5: Commit**：`git add services/query-api && git commit -m "feat(query-api): 新增瘦只读查询服务（承接 consensus 的 DuckDB 查询 + n8n 状态镜像）"`

### Task 5: n8n workflow（trading-decision）+ decide() 单测

**Files:**
- Create: `n8n/workflows/trading-decision.json`
- Create: `n8n/credentials/dashscope.json.example`、`n8n/credentials/iggy.json.example`、`n8n/credentials/grafana.json.example`
- Create: `n8n/tests/test_decide.mjs`、`n8n/tests/test_workflow_json.py`
- Create: `n8n/README.md`（导入/导出/发布流程、staticData 语义、killswitch 用法）

**Interfaces:**
- Consumes: Task 4 `GET :8009/api/balance`、`POST :8009/api/state`；lancer `:8003/api/signals/{pair}`（现状）；Prometheus `:9090/api/v1/query`；Iggy HTTP `:3000/users/login` + `POST /streams/crypto/topics/orders/messages`；DashScope `https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions`
- Produces: workflow id `slr-trading-decision`（Task 9 n8n role import/publish 用）；风控核心 = 内嵌纯函数 `decide(cfg, cycle, st, pair, sig)` 与 `updateEquity(st, prices, cfg)`（jsCode 字符串，被测试提取执行）

- [ ] **Step 1: 写 decide() 参考实现与失败测试** `n8n/tests/test_decide.mjs`（node ≥20 自带 test runner）。测试从 workflow JSON 提取 Code(风控裁决) 节点 jsCode，eval 后断言：

```js
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test'; import assert from 'node:assert';

const wf = JSON.parse(readFileSync(new URL('../workflows/trading-decision.json', import.meta.url)));
const node = wf.nodes.find(n => n.name === 'RiskDecide');
assert.ok(node, 'RiskDecide 节点存在');
const dir = mkdtempSync(join(tmpdir(), 'decide-'));
const mod = join(dir, 'decide.cjs');
writeFileSync(mod, node.parameters.jsCode + '\nmodule.exports={decide,updateEquity,DEFAULTS};');
const { decide, updateEquity, DEFAULTS } = await import(mod);

const cycle = { now: 1_700_000_000, priceLag: 1, signalLag: 1 };
const sig = { similarity: 0.5, avg_outcome: 0.8, avg_max_outcome: 1.2,
              queried_at: new Date(1_700_000_000_000 - 5000).toISOString().replace('.000Z','Z') };
const st0 = () => ({ cash: 1000, positions: {}, killswitch: false, circuit_broken: false,
                     day_baseline: { date: '2026-10-04', equity: 1000 }, trade_log: [] });

test('BUY: 高相似度+Bullish+无障碍', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', sig, { price: 100, ts: cycle.now - 1 }, 'Bullish');
  assert.equal(r.action, 'BUY'); assert.ok(Math.abs(r.order.quantity - 0.5) < 1e-9);
});
test('SELL 永远放行: killswitch 下 Bearish 仍平仓', () => {
  const st = st0(); st.killswitch = true;
  st.positions['BTC-USD'] = { quantity: 0.5, entry_price: 100 };
  const r = decide(DEFAULTS, cycle, st, 'BTC-USD', { ...sig, similarity: 0.05 }, { price: 110, ts: cycle.now - 1 }, 'Bearish');
  assert.equal(r.action, 'SELL');
});
test('BLOCKED: killswitch 挡开仓并记账', () => {
  const st = st0(); st.killswitch = true;
  const r = decide(DEFAULTS, cycle, st, 'BTC-USD', sig, { price: 100, ts: cycle.now - 1 }, 'Bullish');
  assert.equal(r.action, 'BLOCKED'); assert.equal(r.reason, 'kill_switch');
});
test('BLOCKED: stale_signal(>120s)', () => {
  const old = { ...sig, queried_at: new Date((cycle.now - 200) * 1000).toISOString().replace('.000Z','Z') };
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', old, { price: 100, ts: cycle.now - 1 }, 'Bullish');
  assert.equal(r.reason, 'stale_signal');
});
test('BLOCKED: uneconomic(avg_max_outcome<0.45)', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', { ...sig, avg_max_outcome: 0.3 }, { price: 100, ts: cycle.now - 1 }, 'Bullish');
  assert.equal(r.reason, 'uneconomic');
});
test('HOLD: Neutral 且无仓位', () => {
  const r = decide(DEFAULTS, cycle, st0(), 'BTC-USD', sig, { price: 100, ts: cycle.now - 1 }, 'Neutral');
  assert.equal(r.action, 'HOLD');
});
test('现金不足不下单', () => {
  const st = st0(); st.cash = 10;
  assert.equal(decide(DEFAULTS, cycle, st, 'BTC-USD', sig, { price: 100, ts: cycle.now - 1 }, 'Bullish').action, 'HOLD');
});
test('updateEquity: 回撤>2% 熔断，只挡开仓；UTC 日切重置', () => {
  const st = st0();
  updateEquity(st, { 'BTC-USD': 50 }, DEFAULTS, '2026-10-04');  // equity=1000+持仓估值
  st.cash = 900; // 制造 10% 回撤
  updateEquity(st, {}, DEFAULTS, '2026-10-04');
  assert.equal(st.circuit_broken, true);
  updateEquity(st, {}, DEFAULTS, '2026-10-05');
  assert.equal(st.circuit_broken, false);
});
```

- [ ] **Step 2: 运行确认失败**：`node --test n8n/tests/` → Expected: FAIL（workflow JSON 不存在）
- [ ] **Step 3: 编写 workflow JSON**。节点清单（连接顺序即 spec §n8n）：
  `Schedule(60s)` 与 `Webhook(/webhook/killswitch, GET+POST)` → `Init(Code)`：读 `$('...').item` 无关，直接 staticData + DEFAULTS 常量区（阈值逐字=Global Constraints）+ UTC 日切 → `FetchCycle(HttpRequest×3 并行：Prometheus crypto_price 全 pair、priceLag=time()-max(timestamp(crypto_price))、signalLag=time()-max(timestamp(lancer_queries_total)))` → `SplitInBatches(每 pair)` → `FetchSignal(HTTP GET http://localhost:8003/api/signals/{{pair}}，onError=continue→signal_stale)` → `FetchPrice`（从 FetchCycle 结果取，无网络）→ `FetchBalance(HTTP GET http://localhost:8009/api/balance)` → `LLM(HTTP POST DashScope compatible-mode/v1/chat/completions，Header Auth credential；body: model={{ $('Init').item.json.cfg.model }}, temperature 0.3, max_tokens 200, response_format json_object, prompt=spec §analyst 的 RAG 拼装（Prometheus 15min 趋势+涨跌幅+波动率 & lancer 信号），解析失败回退正则 /Sentiment:\s*(Bullish|Bearish|Neutral)/)` → `RiskDecide(Code)`：内嵌 decide()/updateEquity()/DEFAULTS **纯函数**（上方测试即其契约；实现逐条对照 consensus/main.py:424-533——检查顺序 kill_switch→drawdown→stale_signal→stale_price→no_heartbeat(60/180)→uneconomic(0.45)；BUY 条件含 avg_outcome>0 与 cash≥50；BLOCKED 记 trade_log）→ `Switch(action)` → BUY/SELL 分支：`IggyLogin(HTTP POST :3000/users/login，token 缓存 staticData，401 时重登一次)` → `PublishOrder(Code 组包 base64 + HTTP POST :3000/streams/crypto/topics/orders/messages，Bearer，partitioning={kind:"messages_key",value:btoa(pair)}，messages=[{id:0,payload:btoa(JSON.stringify(order))}]，order schema 逐字=consensus:532-541)` → `Annotate(HTTP POST :3001/api/annotations，Basic Auth，trade 事件每笔 + narrative 每 5 轮，dashboardUID=crypto-live-ticks)` → `Commit(Code：更新 staticData positions/cash/trade_log(截断 200)/day_baseline)` → `MirrorState(HTTP POST :8009/api/state，onError=continue)`。killswitch 分支：Webhook → `KillSwitch(Code：POST body {killswitch:true|false} 或 GET 读，写 staticData，立即 Respond to Webhook)`（独立短链，成功执行即持久化）。workflow `id: "slr-trading-decision"`, `name: "SLR Trading Decision"`, `settings.executionOrder: "v1"`, `active: false`
- [ ] **Step 4: 运行确认通过**：`node --test n8n/tests/` → Expected: 9 pass
- [ ] **Step 5: JSON 结构校验测试** `n8n/tests/test_workflow_json.py`（python）：json.load 成功；`id=="slr-trading-decision"`；节点名集合 ⊇ {Schedule, Webhook, Init, RiskDecide, Switch, IggyLogin, PublishOrder, Annotate, Commit, MirrorState, KillSwitch}；connections 中每个引用节点存在；无 `active: true`。运行：`python3 -m pytest n8n/tests/test_workflow_json.py -q` → Expected: 先 FAIL（若 JSON 未写）后 PASS
- [ ] **Step 6: 凭据 example + README**：dashscope.json.example（headerAuth 形，`{"name":"DashScope","type":"httpHeaderAuth","data":{"name":"Authorization","value":"Bearer <DASHSCOPE_API_KEY>"}}`）、iggy.json.example（httpBasicAuth）、grafana.json.example（httpBasicAuth）；README 写明：真凭据由 Ansible vault 渲染后 `n8n import:credentials` 导入，example 仅结构参考
- [ ] **Step 7: Commit**：`git add n8n && git commit -m "feat(n8n): trading-decision workflow（替代 analyst+consensus 决策链，decide() 纯函数含 9 项单测）"`

### Task 6: iac/terraform（资源供给）

**Files:**
- Create: `iac/terraform/{providers.tf,variables.tf}`、`iac/terraform/modules/{ecs-instance,oss-bucket,ram-role,security-group}/`（拷自 `/Users/huangzhaorong/Documents/mfworkspace/adtrader-iac-infra/terraform/modules/` 同名目录，逐文件复制后按下述修改）
- Create: `iac/terraform/environments/prod/{backend.conf,main.tf,locals.tf,terraform.tfvars.example,outputs.tf,inventory.tmpl}`（骨架抄 `adtrader-iac-infra/terraform/environments/atx_v2/`）

**Interfaces:**
- Produces: terraform outputs `data_ip`、`app_ip`、`bucket_lakehouse`、`bucket_artifacts`、`oss_dls_endpoint`、`ansible_inventory`、`security_group_id`（被 Task 7 inventory 管道、Task 8 group_vars 消费）；`inventory.tmpl` 两组 `[dataplane]`/`[appplane]`
- Consumes: 现有 VPC/vSwitch（data source 按 `var.vpc_name`/`var.vswitch_cidr` 查询，不新建）

模块修改点（相对 adtrader 原版）：ecs-instance 保留 instance+RAM attachment+数据盘逻辑；oss-bucket 去掉不需要的 lifecycle 变量默认值即可；ram-role policy 改为 `oss:*` on `acs:oss:*:*:slr-lakehouse-prod` 与 `acs:oss:*:*:slr-lakehouse-prod/*` + `oss:GetObject` on artifacts；security-group 保留 `inner_access_policy="Accept"`，**删除任何 SG 自引用规则**（阿里云 API 拒绝，atx_v2 T0 实证）。

main.tf 资源：data(alicloud_vpcs/alicloud_vswitches/alicloud_images `^aliyun_3_x64_20G_alibase`)、module.security-group、module.oss-bucket×2、module.ram-role、alicloud_ecs_key_pair(var.ssh_public_key)、module.ecs-instance `dataplane`(ecs.g8i.2xlarge×1, 40G+200G cloud_essd PL1, hostname=ecs-data-01) 与 `appplane`(ecs.g8i.xlarge×1, 40G+100G PL0, hostname=ecs-app-01)，`internet_max_bandwidth_out=100`(PayByTraffic)。locals.tf SG 规则：ingress 22 from `var.admin_cidrs`；ingress [3001,5678,8081,9090] from `var.ui_cidrs`(默认 `[]`)；无其他入向。backend.conf：OSS backend（bucket=`adtrader-tfstate`，key=`slr/prod/terraform.tfstate`，内网 endpoint 注释同 atx_v2 惯例，本地 init 临时换公网）。

- [ ] **Step 1: 拷贝+修改**（上述清单）
- [ ] **Step 2: 校验**：`cd iac/terraform/environments/prod && terraform fmt -recursive ../../ && terraform init -backend=false && terraform validate` → Expected: `Success! The configuration is valid.`（本机无 terraform 二进制则 `brew install terraform` 或记录由操作者执行；validate 必须实际跑过才算完成）
- [ ] **Step 3: tfvars.example**：region/bucket 名/vpc_name/vswitch_cidr/ssh_public_key 占位/admin_cidrs/实例规格，全部含中文注释
- [ ] **Step 4: Commit**：`git add iac/terraform && git commit -m "feat(iac): Terraform 资源供给（2×ECS+2×OSS bucket+RAM role+SG，OSS backend state）"`

### Task 7: iac/scripts inventory 管道

**Files:**
- Create: `iac/scripts/{generate-inventory.sh,render-inventory.py,render_inventory_lib.py}`（拷自 `adtrader-iac-infra/scripts/` 同名文件）
- Create: `iac/scripts/tests/test_render_inventory.py`（参考 `adtrader-iac-infra/scripts/tests/test_render_inventory.py` 模式重写）

**Interfaces:**
- Consumes: Task 6 `terraform output -json`（`ansible_inventory` 结构逐字沿用 atx_v2：`[{instance_name,ansible_host,ansible_user}]`）与 `environments/prod/inventory.tmpl`（占位符 `${dataplane}`/`${appplane}`）
- Produces: `iac/ansible/inventories/prod/hosts`（组：dataplane/appplane/cluster:children；覆盖前自动 `.bak` 备份；`${...}` 残留即报错退出——原防护逻辑保留）

修改点：render_inventory_lib.py 的 GROUP_MAP 精简为 `{"dataplane":["ecs-data"],"appplane":["ecs-app"]}`（按 instance_name 前缀匹配）；删除 COLOCATE_MAP（无共置）；generate-inventory.sh 的 env case 只留 `prod`，输出路径改 `iac/ansible/inventories/prod/hosts`。

- [ ] **Step 1: 拷贝+修改**
- [ ] **Step 2: 写测试**：构造假 terraform output JSON（2 实例）+ 本项目 inventory.tmpl → 调 render 函数 → 断言 hosts 含 `[dataplane]\necs-data-01`、`[appplane]\necs-app-01`、ansible_host 正确、占位符残留时 raise
- [ ] **Step 3: 运行**：`python3 -m pytest iac/scripts/tests/ -q` → Expected: 先 FAIL 后 PASS
- [ ] **Step 4: Commit**：`git add iac/scripts && git commit -m "feat(iac): inventory 生成管道（terraform output → ansible hosts）"`

### Task 8: iac/ansible 骨架 + 数据面 roles（common/jindofuse/zookeeper/iggy/fluss）

**Files:**
- Create: `iac/ansible/{ansible.cfg,requirements.yml}`、`iac/ansible/inventories/prod/group_vars/{all.yml.example,dataplane.yml,appplane.yml}`、`iac/ansible/vault-example.yml`
- Create: `iac/ansible/roles/{common,jindofuse,zookeeper,iggy,fluss}/`（骨架模式抄 `adtrader-iac-infra/ansible/roles/fluss/` 与 `_common/`：tasks/main.yml + defaults/main.yml + templates/*.j2 + handlers）

**Interfaces:**
- Consumes: Task 1 iggy tarball 命名（`iggy_artifact_url: https://slr-artifacts-prod.oss-cn-beijing.aliyuncs.com/iggy-0.7.0-linux-x86_64.tar.gz` + sha256 变量）；Task 3 reconcile 脚本（flink role timer 用，Task 9）
- Produces: group_vars/all.yml.example 版本锁全表（Global Constraints 逐字）+ vault 键名清单（`iggy_password`、`grafana_admin_password`、`n8n_encryption_key`、`dashscope_api_key`、回退 `oss_access_key_id/secret`）；role 名清单被 Task 9/10 playbooks 引用

role 要点（每个 role 的 systemd unit 均 `Restart=on-failure` + `WantedBy=multi-user.target`）：
- **common**: dnf 安装 java-17-openjdk-headless、fuse3、chronyd(enable+start)、tar/curl；python3.12（dnf 无则 uv standalone 安装到 /opt/python3.12 并 alternatives）；创建 `/data`、`/mnt/warehouse` 目录
- **jindofuse**: 下载 jindosdk-6.10.8-linux.tar.gz（内网 endpoint 变量）→ /opt/jindosdk；`.so` → /usr/lib64（notify bus 无需）；`templates/jindosdk.cfg.j2`（`[jindosdk]` fs.oss.endpoint={{ oss_dls_endpoint }}、fs.oss.provider.endpoint=ECS_ROLE、fs.oss.provider.format=JSON）；unit `jindofuse-warehouse.service`：Type=forking、Environment=LD_LIBRARY_PATH=/opt/jindosdk/lib/native、ExecStart=/opt/jindosdk/bin/jindo-fuse /mnt/warehouse -ouri=oss://{{ slr_bucket }}.{{ oss_dls_endpoint }}/ {{ jindofuse_extra_args }}（dataplane 空/appplane `-oro`，由 group_vars 给）、ExecStop=/bin/umount -l /mnt/warehouse
- **zookeeper**: apache-zookeeper-3.9.3-bin.tar.gz(sha512 校验)→/opt/zookeeper；myid=1 写 /data/zookeeper/myid；`zoo.cfg.j2`（dataDir=/data/zookeeper、4lw.whitelist=*）；ExecStart=bin/zkServer.sh start-foreground
- **iggy**: 下载 artifacts 提取件(sha256)→/opt/iggy；`iggy.env.j2` EnvironmentFile（IGGY_TCP_ADDRESS=0.0.0.0:8090、IGGY_HTTP_ADDRESS=0.0.0.0:3000、IGGY_ROOT_USERNAME=admin、IGGY_ROOT_PASSWORD={{ iggy_password }}(vault)、数据目录 /data/iggy）；unit：ExecStart=/opt/iggy/iggy-server、`LimitMEMLOCK=infinity`、`AmbientCapabilities=CAP_SYS_NICE`、WorkingDirectory=/opt/iggy（server.toml 从提取件 configs/ 渲染 datadir 路径）
- **fluss**: fluss-0.9.0-incubating-bin.tgz(sha512)→/opt/fluss；`server.yaml.j2` 两份（coordinator: zookeeper.address={{ data_ip }}:2181、coordinator.host={{ data_ip }}、remote.data.dir=/data/fluss/remote；tablet: 同 zk + tablet-server.host={{ data_ip }}、tablet-server.port=9124、data.dir=/data/fluss/data、tablet-server.id=0）；两个 unit 分别 `bin/coordinator-server.sh start-foreground` / `bin/tablet-server.sh start-foreground`（脚本名以 tarball 内实际为准，实施时 `tar tzf` 核对）

- [ ] **Step 1: 编写全部文件**（j2 模板中所有端口/路径与 spec 架构图逐字一致）
- [ ] **Step 2: 校验**：`python3 -c "import yaml,glob; [yaml.safe_load(open(f)) for f in glob.glob('iac/ansible/roles/*/tasks/main.yml')+glob.glob('iac/ansible/roles/*/defaults/main.yml')]"`（YAML 可解析）→ Expected: 无异常；有 ansible 则加 `ansible-playbook --syntax-check`（playbook 在 Task 10 才存在，此步先 yaml 校验）
- [ ] **Step 3: Commit**：`git add iac/ansible && git commit -m "feat(iac): Ansible 骨架与数据面基础 roles（common/jindofuse/zookeeper/iggy/fluss）"`

### Task 9: 数据面 flink role + 应用面 roles（python-service/query-api 部署/prometheus/grafana/n8n）+ playbooks

**Files:**
- Create: `iac/ansible/roles/{flink,python-service,prometheus,grafana,n8n}/`
- Move: `prometheus/prometheus.yml` → `roles/prometheus/templates/prometheus.yml.j2`；`grafana/dashboards/*`+`grafana/datasources/*` → `roles/grafana/files/`（datasources 的 prometheus URL 改 `http://localhost:9090`；trading.json 中 consensus `:8007/:8008` 基址改 `http://localhost:8009`，路径不变；live-ticks.json 删除 analyst narrative 相关面板/注解引用——实施时 grep `analyst|8005|8007|8008` 逐个处理）
- Create: `iac/ansible/{playbook_dataplane.yaml,playbook_appplane.yaml,playbook_jobs.yaml}`、`iac/ansible/inventories/prod/hosts.dummy`（仅语法检查用：`[dataplane]\necs-data-01 ansible_host=127.0.0.1\n[appplane]\necs-app-01 ansible_host=127.0.0.2\n[cluster:children]\ndataplane\nappplane`）

**Interfaces:**
- Consumes: Task 2 render-sql.sh、Task 3 reconcile 脚本、Task 4 query-api 源码（`services/query-api` 经 python-service role 部署，服务名 query-api，端口 8009/8010）、Task 5 workflow JSON（n8n role 导入，id=slr-trading-decision）、Task 8 group_vars/role 骨架
- Produces: 三个 playbook（部署入口，runbook 引用）；prometheus scrape 目标表；Flink SQL 作业提交顺序（playbook_jobs）

role 要点：
- **flink**: flink-1.20.3-bin-scala_2.12.tgz→/opt/flink；调 download-jars.sh（Task 11 已含 jindosdk）装 lib；`find /opt/flink/lib -name 'flink-oss-fs-hadoop*' -delete` 防御；jindosdk `jindo-flink-6.10.8-full.jar`+`jindo-sdk/core-nextarch.jar`→lib、`.so`→/usr/lib64；`flink-conf.yaml.j2`（jobmanager.rpc.address=127.0.0.1、state.backend=hashmap、checkpoints/savepoints/ha=oss://{{ slr_bucket }}/flink/{checkpoints,savepoints,ha}、high-availability=zookeeper、ha.zookeeper.quorum=127.0.0.1:2181、ha.cluster-id=/streaming-lakehouse、taskmanager.numberOfTaskSlots=8、metrics prom reporter 9249、fs.oss.endpoint/fs.oss.impl/fs.AbstractFileSystem.oss.impl/fs.oss.provider.endpoint=ECS_ROLE/fs.oss.provider.format=JSON）；`core-site.xml.j2`（同五键，HADOOP_CONF_DIR=/opt/flink/conf/hadoop）；unit×2（jobmanager.sh/taskmanager.sh start-foreground，Environment=JAVA_HOME）；render-sql.sh 渲染 flink/sql→/opt/flink/sql（SLR_WAREHOUSE_BASE=oss://{{ slr_bucket }}/warehouse、SLR_IGGY_HOST=127.0.0.1）；`reconcile-snapshot.{service,timer}`（OnCalendar=*:0/5，EnvironmentFile 指 FLINK_HOME/SLR_PAIMON_WAREHOUSE/RECONCILE_DEST）
- **python-service**（通用 role，vars 驱动多实例）：rsync 仓库源码目录→/opt/slr/{{ svc_name }}；python3.12 -m venv + pip install -r requirements.txt（pip 镜像变量）；`{{ svc_name }}.env.j2` EnvironmentFile（每服务变量表见 spec §5.2：poller/bridge `IGGY_HOST={{ data_ip }}`；lancer 加 `LANCER_DB_PATH=/data/lancer`、`LANCER_WAREHOUSE_PATH=/mnt/warehouse/paimon/crypto.db/ohlcv_1m`；replay `REPLAY_WAREHOUSE_PATH=/mnt/warehouse/iceberg/crypto/ticks/data`）；unit `python -u main.py` Restart=always；`slr_service_mode: simple|oneshot|timer` 支持（replay=oneshot disabled；lancer-indexer=oneshot+timer 每小时，env LANCER_MODE=indexer）；应用面实例：poller、bridge、lancer(signal)、query-api（源码目录 services/query-api，env QUERY_API_*）、replay(oneshot)、lancer-indexer(timer)
- **prometheus**: v2.53.0 tarball→/opt/prometheus；prometheus.yml.j2 scrape：bridge localhost:8001(1s)、poller :8000、lancer :8004、query-api :8010、n8n localhost:5678(/metrics)、flink {{ data_ip }}:9249；--storage.tsdb.path=/data/prometheus --storage.tsdb.retention.time=7d
- **grafana**: 11.1.0 tarball→/opt/grafana；infinity 插件（离线 zip 预放 artifacts 桶，grafana-cli --pluginUrl 安装）；provisioning datasources+dashboards 从 role files/（Move 来的）；GF_SECURITY_ADMIN_PASSWORD={{ grafana_admin_password }}(vault)、http_port=3001、数据 /data/grafana
- **n8n**: Node 24 tarball→/opt/node（npmmirror）；`npm i -g n8n@2.41.6`（npmmirror registry）；user n8n；`n8n.env.j2` EnvironmentFile（N8N_USER_FOLDER=/var/lib/n8n、N8N_ENCRYPTION_KEY={{ n8n_encryption_key }}(vault)、N8N_METRICS=true、PORT=5678、N8N_DIAGNOSTICS_ENABLED=false）；部署任务：copy `n8n/workflows/`+渲染后的 credentials JSON→`sudo -u n8n n8n import:workflow --separate --input=...` + `import:credentials --input=...` + `publish:workflow --id=slr-trading-decision`（JSON 变更 handler：restart n8n + re-import；credentials 真值由 vault 渲染 j2，仓库仅 .example）
- **playbook_dataplane.yaml**: hosts=dataplane → roles: common, jindofuse, zookeeper, iggy, fluss, flink（顺序即依赖）
- **playbook_appplane.yaml**: hosts=appplane → common, jindofuse(ro), python-service×6, prometheus, grafana, n8n
- **playbook_jobs.yaml**: hosts=dataplane，tasks：渲染校验 → 依序 `sql-client.sh embedded -f /opt/flink/sql/<file>` 提交（lakehouse-tier → fluss-hot-tier → ohlcv-candles → seed-balance → clearing-house；seed 前守卫：`SELECT COUNT(*) FROM balance`>0 则 skip 并 warn）→ 启用 reconcile timer

- [ ] **Step 1: 编写全部 role/playbook/模板迁移**
- [ ] **Step 2: 校验**：`ansible-playbook -i iac/ansible/inventories/prod/hosts.dummy iac/ansible/playbook_dataplane.yaml --syntax-check`（dummy inventory：手造 2 主机条目供语法检查；appplane/jobs 同）→ Expected: 3 个 playbook 均无 syntax error；`python3 -c "import yaml,glob; ..."` 覆盖新 role YAML
- [ ] **Step 3: Commit**：`git add iac/ansible && git commit -m "feat(iac): flink/python-service/prometheus/grafana/n8n roles 与三大 playbook"`

### Task 10: 删除 Compose 路径 + 全仓残留清理

**Files:**
- Delete: `docker-compose.yml`、`flink/Dockerfile`、`analyst/`、`consensus/`、`.env.example`、`prometheus/`、`grafana/`（已迁入 roles）、compose 相关文档段落
- Modify: `scripts/download-jars.sh` 调用方注释、`docs/clean-slate-guide.md`（标注重写或删除——runbook 取代）、`scripts/backtest.py`（其读 parquet 路径若含 /opt/flink/warehouse 则加 SLR_WAREHOUSE_BASE 支持）

**Interfaces:**
- Consumes: Task 9 已完成模板迁移（先迁移后删除，同一分支内顺序执行）

- [ ] **Step 1: 删除清单执行**：`git rm -r docker-compose.yml flink/Dockerfile analyst consensus .env.example prometheus grafana`
- [ ] **Step 2: 残留 grep 门禁**：`grep -rn "docker\|consensus\|analyst\|:8005\|:8007\|8008\|iggy-web-ui" --include="*.py" --include="*.sh" --include="*.sql" --include="*.json" --include="*.yml" --include="*.yaml" . | grep -v "docs/\|n8n/tests\|.superpowers"` → Expected: 仅允许出现于注释性历史说明（逐条人工判定，代码/配置零残留；scripts/backtest.py、reconcile、download-jars、query-api、workflow JSON 重点核对）
- [ ] **Step 3: 复跑既有测试防回归**：`bash scripts/tests/test_render_sql.sh && (cd services/query-api && .venv/bin/pytest -q) && node --test n8n/tests/ && python3 -m pytest n8n/tests/test_workflow_json.py iac/scripts/tests -q` → Expected: 全 PASS
- [ ] **Step 4: Commit**：`git add -A && git commit -m "refactor!: 删除 Docker Compose 路径（analyst/consensus 由 n8n+query-api 取代）"`

### Task 11: download-jars.sh 补 JindoSDK 条目

**Files:**
- Modify: `scripts/download-jars.sh`

**Interfaces:**
- Produces: `jars/jindo-flink-6.10.8-full.jar`、`jars/jindo-sdk-6.10.8-nextarch.jar`、`jars/jindo-core-6.10.8-nextarch.jar`（flink role 消费；从 jindosdk-6.10.8-linux.tar.gz 解包提取而非独立 URL——脚本内下载 tarball→解包→取 3 jar+so→校验）
- Consumes: 现有 JARS 数组 + sha256 校验模式（沿用）

- [ ] **Step 1: 修改**：JARS 数组追加 jindosdk tarball 条目（URL=`https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/release/6.10.8/jindosdk-6.10.8-linux.tar.gz`，sha256 首次下载后 `shasum -a 256` 实测回填——本地执行一次取得真值，不得留占位）；解包逻辑：`tar xzf` 后 cp `plugins/flink/jindo-flink-6.10.8-full.jar`、`lib/jindo-sdk-*-nextarch.jar`、`lib/jindo-core-*-nextarch.jar` 至 jars/，`lib/native/*.so` 至 jars/native/
- [ ] **Step 2: 验证**：`bash -n scripts/download-jars.sh`；`curl -sI <tarball URL> | head -1` → Expected: HTTP/1.1 200 或 302（URL 有效）；如本机网络允许可实跑一次核对 sha256（463MB，可选）
- [ ] **Step 3: Commit**：`git commit -am "feat(scripts): download-jars 补 JindoSDK 6.10.8（OSS-HDFS connector）"`

### Task 12: 文档（runbook / env-reference / architecture / README / CLAUDE.md）

**Files:**
- Create: `docs/runbook-ecs-deploy.md`、`docs/env-reference.md`
- Modify: `architecture.md`、`README.md`、`CLAUDE.md`、`docs/clean-slate-guide.md`（改为 ECS 语境的彻底重置流程：清 ZK /streaming-lakehouse + 清 oss://bucket/flink/ha + 重建 Paimon 路径说明）

**Interfaces:**
- Consumes: 全部前序 Task 的产物路径与命令

- [ ] **Step 1: runbook** 按 spec Phase 1-4 逐步命令化：terraform init/apply → **带外①控制台开通 OSS-HDFS** → iggy 提取件上传 artifacts（ossutil）→ generate-inventory → ansible 三 playbook 顺序 → **带外②SSH 隧道建 n8n owner** → import/publish → G1/G2/G3 金丝雀命令与判据（G1: sql-client INSERT/SELECT oss canary + 控制台确认 HDFS 命名空间 + dummy 流作业 checkpoint 落 oss；G2: curl login→发消息→poll；G3: 两台 fuse ls/cat + duckdb 直读）→ jobs 提交（seed 守卫）→ backfill（data-01，30-90 天）→ 切换验证清单（spec §验证 逐字）→ 故障回退（R1-R10 缓解措施操作化：普通 OSS endpoint 回退、AK 回退、ossutil 同步兜底、n8n unpublish）
- [ ] **Step 2: env-reference.md**：原 .env.example 全表 → 新归属（group_vars 键名/vault 键名/systemd EnvironmentFile 路径），含 Task 4 新增 QUERY_API_* 与 n8n N8N_*
- [ ] **Step 3: architecture.md/README.md/CLAUDE.md 更新**：架构图（ECS 两台+OSS-HDFS+n8n）、端口表（删 8005-8008，增 8009/8010/5678）、常用命令（terraform/ansible/runbook 引用，删除全部 docker compose 命令）、CLAUDE.md 关键约束更新（fuse 只读、query-api 取代 DuckDB 直读入口、n8n staticData 语义、G1-G3 门槛）
- [ ] **Step 4: 校验**：`grep -rn "docker compose\|docker-compose" README.md CLAUDE.md architecture.md docs/*.md` → Expected: 仅历史说明/删除记录处出现
- [ ] **Step 5: Commit**：`git add docs architecture.md README.md CLAUDE.md && git commit -m "docs: ECS 原生部署 runbook、env 参考与架构文档更新"`

### Task 13: 终检（全部门禁一次跑齐）

**Files:** 无新增（验证性任务）

- [ ] **Step 1: 全量门禁**：

```bash
bash scripts/tests/test_render_sql.sh
bash -n scripts/extract-iggy-binary.sh scripts/reconcile-snapshot.sh scripts/download-jars.sh
(cd services/query-api && .venv/bin/pytest -q)
node --test n8n/tests/
python3 -m pytest n8n/tests/test_workflow_json.py iac/scripts/tests -q
(cd iac/terraform/environments/prod && terraform init -backend=false -reconfigure && terraform validate)
for pb in dataplane appplane jobs; do ansible-playbook -i iac/ansible/inventories/prod/hosts.dummy iac/ansible/playbook_$pb.yaml --syntax-check; done
```

→ Expected: 全部通过（输出逐一读取比对）
- [ ] **Step 2: git 状态干净**：`git status --short` → Expected: 空；`git log --oneline dev..HEAD` 列出全部任务提交
- [ ] **Step 3: 无 commit**（如有修复则 `git commit -m "fix: 终检修复"`）

---

## 部署期（本计划外，操作者按 runbook 执行，需云凭证与带外操作）

terraform apply → OSS-HDFS 开通（控制台）→ 提取件上传 → ansible 三 playbook → G1/G2/G3 金丝雀 → jobs 提交（seed 守卫）→ n8n owner（SSH 隧道）→ backfill → 端到端验证（spec §验证 3）→ 本地 compose down（卷留 30 天）。停止点：任何 apply/上传/带外操作前需操作者确认（不可逆/外部副作用）。
