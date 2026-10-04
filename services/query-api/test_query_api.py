"""query-api 门禁测试：假 warehouse（parquet + reconcile JSON）驱动全部端点。"""
import json
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    wh = tmp_path / "wh"
    d = wh / "ohlcv_1m" / "bucket-0"
    d.mkdir(parents=True)
    # naive 本地时间：DuckDB now() 按会话时区解释 naive 时间戳；生产 ECS 为 UTC，两者一致
    now = datetime.now() - timedelta(minutes=5)
    pq.write_table(
        pa.table(
            {
                "pair": ["BTC-USD", "ETH-USD"],
                "window_start": pa.array([now, now], type=pa.timestamp("ms")),
                "open": [1.0, 2.0],
                "high": [2.0, 3.0],
                "low": [0.5, 1.5],
                "close": [1.5, 2.5],
                "tick_count": [10, 20],
            }
        ),
        d / "data-0.parquet",
    )
    (wh / "reconcile_balance.json").write_text(json.dumps({"USD": 950.0, "BTC": 0.01}))
    st = tmp_path / "state"
    st.mkdir()
    monkeypatch.setenv("QUERY_API_WAREHOUSE_PATH", str(wh))
    monkeypatch.setenv("QUERY_API_STATE_DIR", str(st))
    import importlib

    import main

    importlib.reload(main)
    return TestClient(main.app)


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_balance_from_reconcile_json(client):
    r = client.get("/api/balance")
    assert r.status_code == 200
    body = r.json()
    assert body["balances"]["USD"] == 950.0
    assert body["balances"]["BTC"] == 0.01
    assert body["as_of"] > 0


def test_state_roundtrip_atomic(client):
    payload = {
        "positions": {"BTC-USD": {"quantity": 0.01, "entry_price": 100.0}},
        "cash": 900.0,
        "killswitch": False,
        "trade_log": [],
        "day_baseline": {"date": "2026-10-04", "equity": 1000.0},
    }
    assert client.post("/api/state", json=payload).status_code == 200
    got = client.get("/api/state").json()
    assert got["cash"] == 900.0
    positions = client.get("/api/positions").json()
    assert positions["positions"]["BTC-USD"]["quantity"] == 0.01
    assert client.get("/api/trades").json() == []


def test_state_get_empty_defaults(client):
    got = client.get("/api/state").json()
    assert got.get("positions", {}) == {}  # 无 state 文件时不 500


def test_audit_no_trades_returns_error_note(client):
    r = client.get("/api/audit")
    assert r.status_code == 200
    assert "error" in r.json()  # trades 表不存在 → 优雅降级


def test_candles_filters_by_pair_and_window(client):
    rows = client.get("/api/candles/BTC-USD?minutes=60").json()
    assert len(rows) == 1
    assert rows[0]["open"] == 1.0 and rows[0]["volume"] == 10
    assert client.get("/api/candles/SOL-USD").json() == []
