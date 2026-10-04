"""query-api — 瘦只读查询服务（替代原 consensus 的查询面）。

职责边界：只做 DuckDB/pyarrow 只读查询 + n8n 决策状态镜像的读写落盘。
不含任何决策/风控逻辑，不连 Iggy，不下单。

端点（与 Grafana trading dashboard / n8n workflow 的契约）：
  GET  /api/health            服务健康 + 监控 pair 列表
  GET  /api/audit             Paimon trades 表聚合审计（DuckDB，搬自 consensus）
  GET  /api/candles/{pair}    ohlcv_1m 蜡烛（Grafana 蜡烛图数据源，搬自 consensus）
  GET  /api/balance           湖内真实余额（读 reconcile_balance.json，reconcile timer 产出）
  GET  /api/positions         n8n 镜像的当前仓位
  GET  /api/trades            n8n 镜像的最近 50 笔交易日志
  GET  /api/state             n8n 每轮 POST 的完整决策状态
  POST /api/state             原子写状态（tmp+rename）
  GET  /metrics               Prometheus 指标（独立端口）

约束：DuckDB 只读 append-only 表（ohlcv_1m/trades 数据文件）；
balance PK 表不直接读 parquet，只消费 Flink SQL 产出的 reconcile JSON。
"""
import glob as globmod
import json
import logging
import os
import tempfile
import time

import duckdb
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s query-api: %(message)s")
log = logging.getLogger("query-api")

PORT = int(os.environ.get("QUERY_API_PORT", "8009"))
METRICS_PORT = int(os.environ.get("QUERY_API_METRICS_PORT", "8010"))
WAREHOUSE_PATH = os.environ.get("QUERY_API_WAREHOUSE_PATH", "/mnt/warehouse/paimon/crypto.db")
STATE_DIR = os.environ.get("QUERY_API_STATE_DIR", "/var/lib/query-api")
PAIRS = [p.strip() for p in os.environ.get("TRADING_PAIRS", "BTC-USD,ETH-USD,SOL-USD,DOGE-USD,AVAX-USD,LINK-USD").split(",") if p.strip()]
STATE_FILE = os.path.join(STATE_DIR, "state.json")

try:  # 测试 reload 时端口可能被占用，指标服务尽力而为
    start_http_server(METRICS_PORT)
except OSError as exc:
    log.warning("metrics server not started: %s", exc)

app = FastAPI(title="SLR Query API — read-only lake queries + n8n state mirror")


def _read_state() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@app.get("/api/health")
async def health():
    return {"status": "ok", "pairs": PAIRS}


@app.get("/api/audit")
async def audit():
    """Query Paimon trades table via DuckDB for strategy audit.（搬自 consensus/main.py）"""
    try:
        con = duckdb.connect()
        trades_path = f"{WAREHOUSE_PATH}/trades"
        result = con.execute(f"""
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as buys,
                SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as sells,
                SUM(fee) as total_fees,
                SUM(CASE WHEN side = 'SELL' THEN total_cost ELSE -total_cost END) as net_pnl
            FROM read_parquet('{trades_path}/**/data-*.parquet', union_by_name=true)
        """).fetchone()
        con.close()
        return {
            "total_trades": result[0],
            "buys": result[1],
            "sells": result[2],
            "total_fees": round(result[3], 2) if result[3] else 0,
            "net_pnl": round(result[4], 2) if result[4] else 0,
        }
    except Exception as exc:
        return {"error": str(exc), "note": "No trades yet — Flink clearing house may not have processed orders"}


@app.get("/api/candles/{pair}")
async def candles(pair: str, minutes: int = 60):
    """Return OHLCV candles from Paimon for Grafana candlestick panel.（搬自 consensus/main.py）"""
    ohlcv_path = f"{WAREHOUSE_PATH}/ohlcv_1m"
    try:
        # Filter out 0-byte parquet files (Paimon partial checkpoint artefacts)
        files = [
            f for f in globmod.glob(f"{ohlcv_path}/**/data-*.parquet", recursive=True)
            if os.path.getsize(f) > 0
        ]
        if not files:
            return []
        file_list = ", ".join(f"'{f}'" for f in files)
        con = duckdb.connect()
        rows = con.execute(f"""
            SELECT
                strftime(window_start, '%Y-%m-%dT%H:%M:%SZ') as time,
                open, high, low, close, tick_count as volume
            FROM read_parquet([{file_list}], union_by_name=true)
            WHERE pair = $1
              AND window_start >= now() - INTERVAL '{int(minutes)} minutes'
            ORDER BY window_start ASC
        """, [pair]).fetchall()
        con.close()
        return [
            {"time": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows
        ]
    except Exception as exc:
        log.warning("Candle query failed: %s", exc)
        return []


@app.get("/api/balance")
async def balance():
    """湖内真实余额：reconcile-snapshot timer 经 Flink SQL 写入的 JSON（balance 为 PK 表不可直读 parquet）。"""
    path = os.path.join(WAREHOUSE_PATH, "reconcile_balance.json")
    try:
        with open(path, encoding="utf-8") as f:
            balances = json.load(f)
        return {"balances": balances, "as_of": os.path.getmtime(path)}
    except FileNotFoundError:
        return JSONResponse({"balances": {}, "as_of": 0, "note": "reconcile_balance.json 尚未生成"}, status_code=200)
    except json.JSONDecodeError as exc:
        return JSONResponse({"balances": {}, "as_of": 0, "error": str(exc)}, status_code=500)


@app.get("/api/state")
async def get_state():
    return _read_state()


@app.post("/api/state")
async def put_state(request: Request):
    """n8n 每轮决策后镜像状态；原子写（tmp+rename），读方永不见半截 JSON。"""
    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse({"error": "state must be a JSON object"}, status_code=400)
    os.makedirs(STATE_DIR, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=STATE_DIR, prefix=".state-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, STATE_FILE)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return {"ok": True, "written_at": time.time()}


@app.get("/api/positions")
async def positions():
    st = _read_state()
    return {
        "cash_balance": st.get("cash", 0),
        "positions": {
            pair: pos for pair, pos in st.get("positions", {}).items()
            if pos.get("quantity", 0) > 0
        },
    }


@app.get("/api/trades")
async def trades():
    return _read_state().get("trade_log", [])[-50:]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
