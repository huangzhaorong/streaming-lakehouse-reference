#!/usr/bin/env python3
"""生成 n8n/workflows/trading-decision.json（workflow 的唯一构建入口）。

- RiskDecide 节点 jsCode = workflows/decide.js 逐字内嵌（测试断言同步）
- 其余 Code 节点 jsCode 内嵌于本文件（改完重跑本脚本 + node/pytest 测试）
- 用法: python3 n8n/build_workflow.py [-o 输出路径]（默认写 workflows/trading-decision.json）
- 部署: ansible n8n role 执行 `n8n import:workflow --separate` + `publish:workflow --id=slr-trading-decision`

运行时环境变量（Ansible 渲染进 /etc/n8n/n8n.env）:
  SLR_TRADING_PAIRS / SLR_PROM_URL / SLR_LANCER_URL / SLR_QUERY_API_URL /
  SLR_IGGY_HTTP_URL / SLR_IGGY_USER / SLR_IGGY_PASSWORD / SLR_GRAFANA_URL / SLR_LLM_MODEL
"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DECIDE_JS = (ROOT / "workflows/decide.js").read_text(encoding="utf-8")

# --- Code 节点 jsCode -------------------------------------------------------

INIT_JS = """
const st = $getWorkflowStaticData('global');
if (st.cash === undefined) {
  st.cash = 1000; st.positions = {}; st.trade_log = [];
  st.killswitch = false; st.circuit_broken = false;
}
st.cycle_count = (st.cycle_count || 0) + 1;
const pairs = ((typeof $env !== 'undefined' && $env.SLR_TRADING_PAIRS) || 'BTC-USD,ETH-USD,SOL-USD,DOGE-USD,AVAX-USD,LINK-USD')
  .split(',').map(s => s.trim()).filter(Boolean);
const cycle_ts = Date.now();
return pairs.map(pair => ({ json: { pair, cycle_ts, cycle_count: st.cycle_count } }));
""".strip()

KILLSWITCH_JS = """
// POST {"killswitch": true|false} 写；GET（无 killswitch 字段）读当前状态。
const st = $getWorkflowStaticData('global');
let out;
const body = $json.body;
if (body && typeof body === 'object' && typeof body.killswitch !== 'undefined') {
  st.killswitch = !!body.killswitch;
  out = { ok: true, killswitch: st.killswitch };
} else {
  out = {
    killswitch: !!st.killswitch, circuit_broken: !!st.circuit_broken,
    cash: st.cash, equity: st.equity, positions: st.positions || {},
  };
}
return [{ json: out }];
""".strip()

BUILD_PROMPT_JS = """
// 组装 LLM 情绪分析 prompt（RAG 上下文对齐原 analyst：15min 价格趋势 + lancer 形态信号）。
const inp = $('SplitInBatches').item.json;
const raw = $json; // FetchSignal 响应；onError=continue 时为 {error:...}
const signal = (raw && typeof raw.similarity === 'number') ? raw : null;
let trend = 'price trend: no data';
try {
  const series = ($('PromPrices').first().json?.data?.result ?? []).find(r => r.metric.pair === inp.pair);
  if (series && series.values && series.values.length > 1) {
    const closes = series.values.map(v => Number(v[1]));
    const first = closes[0], last = closes[closes.length - 1];
    const pct = ((last - first) / first * 100).toFixed(3);
    const mean = closes.reduce((a, b) => a + b, 0) / closes.length;
    const vol = Math.sqrt(closes.reduce((a, b) => a + (b - mean) ** 2, 0) / closes.length);
    trend = `15min trend: ${first.toFixed(2)} -> ${last.toFixed(2)} (${pct}%), volatility(stddev)=${vol.toFixed(2)}, samples=${closes.length}`;
  }
} catch (e) { /* 保持 no data */ }
const sigTxt = signal
  ? `Pattern match: similarity=${Number(signal.similarity).toFixed(3)}, avg_outcome=${Number(signal.avg_outcome ?? 0).toFixed(3)}%, avg_max_outcome=${Number(signal.avg_max_outcome ?? 0).toFixed(3)}%, queried_at=${signal.queried_at || 'n/a'}`
  : 'Pattern match: unavailable (lancer unreachable)';
const prompt = `You are a crypto market sentiment analyst for ${inp.pair}.\\n${trend}\\n${sigTxt}\\nRespond ONLY with JSON: {"sentiment":"Bullish|Bearish|Neutral","narrative":"<one short sentence>"}`;
return [{ json: { ...inp, signal, prompt } }];
""".strip()

PARSE_SENTIMENT_JS = """
// 解析 LLM JSON 输出；解析失败回退正则抽 Sentiment:（与原 analyst 输出契约兼容）。
const inp = $('BuildPrompt').item.json;
let sentiment = 'Neutral';
let narrative = '';
const content = $json?.choices?.[0]?.message?.content || '';
try {
  const o = JSON.parse(content);
  if (o && typeof o.sentiment === 'string') sentiment = o.sentiment;
  narrative = String(o.narrative || '');
} catch (e) {
  const m = content.match(/Sentiment:\\s*(Bullish|Bearish|Neutral)/);
  if (m) sentiment = m[1];
  narrative = content.slice(0, 500);
}
if (!['Bullish', 'Bearish', 'Neutral'].includes(sentiment)) sentiment = 'Neutral';
if (!content) narrative = narrative || 'LLM unavailable — neutral fallback';
return [{ json: { ...inp, sentiment, narrative } }];
""".strip()

BUILD_ORDER_JS = """
// OrderRequest 组包（schema 与原 consensus 完全一致）+ Iggy HTTP API 消息体（payload base64）。
const d = $('RiskDecide').item.json;
const token = $json.token ?? $json.access_token;
const order = {
  order_id: crypto.randomUUID(),
  pair: d.order.pair, side: d.order.side, price: d.order.price, quantity: d.order.quantity,
  signal_similarity: d.order.signal_similarity, signal_sentiment: d.order.signal_sentiment,
  time: d.order.time,
};
const body = {
  partitioning: { kind: 'messages_key', value: btoa(order.pair) },
  messages: [{ id: 0, payload: btoa(JSON.stringify(order)) }],
};
return [{ json: { body, order, token } }];
""".strip()

COMMIT_STATE_JS = """
// 决策落账 staticData（n8n 仅在整次执行成功后持久化 = 原 consensus 原子写语义的保守等价）。
const st = $getWorkflowStaticData('global');
let d;
try { d = $('RiskDecide').item.json; } catch (e) { d = $json; }
let order = d.order;
try { order = $('BuildOrder').item.json.order || order; } catch (e) { /* HOLD/BLOCKED 路径无 BuildOrder */ }
if (d.action === 'BUY' && order) {
  st.positions[d.pair] = { quantity: order.quantity, entry_price: order.price };
  st.cash -= order.quantity * order.price;
  st.trade_log.push({ ...order, status: 'EXECUTED', timestamp: Date.now() / 1000 });
} else if (d.action === 'SELL' && order) {
  st.cash += order.quantity * order.price;
  st.positions[d.pair] = { quantity: 0, entry_price: 0 };
  st.trade_log.push({ ...order, status: 'EXECUTED', pnl: d.pnl, timestamp: Date.now() / 1000 });
} else if (d.action === 'BLOCKED' && d.trade) {
  st.trade_log.push({ ...d.trade, timestamp: Date.now() / 1000 });
}
if (d.action === 'BUY') st.trades_buy = (st.trades_buy || 0) + 1;
if (d.action === 'SELL') st.trades_sell = (st.trades_sell || 0) + 1;
if (st.trade_log.length > 200) st.trade_log = st.trade_log.slice(-200);
st.updated_at = Date.now() / 1000;
// 展示指标（query-api /metrics 转 Prometheus，dashboard 用）：持仓市值/PnL 按最后已知价
const positionValue = {}, pnl = {};
for (const [pair, pos] of Object.entries(st.positions || {})) {
  if (pos.quantity > 0) {
    const px = (st.last_prices || {})[pair] ?? pos.entry_price;
    positionValue[pair] = pos.quantity * px;
    pnl[pair] = (px - pos.entry_price) * pos.quantity;
  }
}
return [{ json: {
  cycle_count: st.cycle_count || 0,
  state: {
    positions: st.positions, cash: st.cash, killswitch: !!st.killswitch,
    circuit_broken: !!st.circuit_broken, day_baseline: st.day_baseline, equity: st.equity,
    counters: {
      trades_buy: st.trades_buy || 0, trades_sell: st.trades_sell || 0,
      signals_checked: st.signals_checked || 0,
    },
    position_value_usd: positionValue, pnl_usd: pnl,
    trade_log: st.trade_log.slice(-50), updated_at: st.updated_at,
  },
} }];
""".strip()

# --- 节点构造 helper ---------------------------------------------------------

_nodes = []


def node(name, type_, type_version, parameters, *, position, credentials=None, on_error=None):
    n = {
        "id": f"slr-{len(_nodes) + 1:02d}",
        "name": name,
        "type": type_,
        "typeVersion": type_version,
        "position": position,
        "parameters": parameters,
    }
    if credentials:
        n["credentials"] = credentials
    if on_error:
        n["onError"] = on_error
    _nodes.append(n)
    return name


def http(name, method, url, *, position, json_body=None, query_params=None, headers=None,
         credentials=None, on_error="continueRegularOutput", timeout=None):
    p = {"method": method, "url": url, "options": {}}
    if timeout:
        p["options"]["timeout"] = timeout
    if json_body is not None:
        p.update({"sendBody": True, "specifyBody": "json", "jsonBody": json_body})
    if query_params:
        p.update({"sendQuery": True, "queryParameters": {"parameters": query_params}})
    if headers:
        p.update({"sendHeaders": True, "headerParameters": {"parameters": headers}})
    return node(name, "n8n-nodes-base.httpRequest", 4.2, p, position=position,
                credentials=credentials, on_error=on_error)


def code(name, js, position):
    return node(name, "n8n-nodes-base.code", 2, {"jsCode": js}, position=position)


GRAFANA_CRED = {"httpBasicAuth": {"id": "slr-grafana-auth", "name": "Grafana"}}
DASHSCOPE_CRED = {"httpHeaderAuth": {"id": "slr-dashscope-auth", "name": "DashScope"}}

# 混合模板（{{expr}}suffix）不带 = 前缀；纯表达式（整串 JS）才带 =
PROM = "{{ $env.SLR_PROM_URL || 'http://localhost:9090' }}"
LANCER = "{{ $env.SLR_LANCER_URL || 'http://localhost:8003' }}"
QUERY_API = "{{ $env.SLR_QUERY_API_URL || 'http://localhost:8009' }}"
IGGY_HTTP = "{{ $env.SLR_IGGY_HTTP_URL || 'http://127.0.0.1:3000' }}"
GRAFANA = "{{ $env.SLR_GRAFANA_URL || 'http://localhost:3001' }}"

# --- workflow 图 -------------------------------------------------------------

node("Schedule", "n8n-nodes-base.scheduleTrigger", 1.2,
     {"rule": {"interval": [{"field": "seconds", "secondsInterval": 60}]}}, position=[0, 0])
node("Webhook", "n8n-nodes-base.webhook", 2,
     {"path": "killswitch", "multipleMethods": True, "allowedMethods": ["GET", "POST"],
      "responseMode": "responseNode", "options": {}}, position=[0, 220])
code("KillSwitch", KILLSWITCH_JS, [220, 220])
node("RespondKillswitch", "n8n-nodes-base.respondToWebhook", 1.1,
     {"respondWith": "json", "responseBody": "={{ $json }}", "options": {}}, position=[440, 220])

code("Init", INIT_JS, [220, 0])
http("PromPrices", "GET", PROM + "/api/v1/query_range", position=[440, 0], query_params=[
    {"name": "query", "value": "crypto_price"},
    {"name": "start", "value": "={{ Math.floor(Date.now() / 1000) - 900 }}"},
    {"name": "end", "value": "={{ Math.floor(Date.now() / 1000) }}"},
    {"name": "step", "value": "60"},
])
http("PromPriceLag", "GET", PROM + "/api/v1/query", position=[660, 0], query_params=[
    {"name": "query", "value": "time() - max(timestamp(crypto_price))"}])
http("PromSignalLag", "GET", PROM + "/api/v1/query", position=[880, 0], query_params=[
    {"name": "query", "value": "time() - max(timestamp(lancer_queries_total))"}])
http("FetchBalance", "GET", QUERY_API + "/api/balance", position=[1100, 0])
node("SplitInBatches", "n8n-nodes-base.splitInBatches", 3,
     {"batchSize": 1, "options": {"reset": False}}, position=[1320, 0])

http("FetchSignal", "GET", LANCER + "/api/signals/{{ $json.pair }}", position=[1540, 0])
code("BuildPrompt", BUILD_PROMPT_JS, [1760, 0])
http("LLM", "POST", "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
     position=[1980, 0], credentials=DASHSCOPE_CRED, timeout=30000,
     json_body=('{"model":{{ JSON.stringify($env.SLR_LLM_MODEL || "qwen-plus") }},'
                '"temperature":0.3,"max_tokens":200,'
                '"response_format":{"type":"json_object"},'
                '"messages":[{"role":"system","content":"You are a crypto sentiment analyst. Respond only with JSON {\\"sentiment\\":\\"Bullish|Bearish|Neutral\\",\\"narrative\\":\\"...\\"}"},'
                '{"role":"user","content":{{ JSON.stringify($json.prompt) }}]}]}'))
code("ParseSentiment", PARSE_SENTIMENT_JS, [2200, 0])
node("RiskDecide", "n8n-nodes-base.code", 2, {"jsCode": DECIDE_JS}, position=[2420, 0])
node("Switch", "n8n-nodes-base.switch", 3.2, {
    "rules": {"values": [{
        "conditions": {
            "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "loose"},
            "conditions": [
                {"leftValue": "={{ $json.action }}", "rightValue": "BUY",
                 "operator": {"type": "string", "operation": "equals"}},
                {"leftValue": "={{ $json.action }}", "rightValue": "SELL",
                 "operator": {"type": "string", "operation": "equals"}},
            ],
            "combinator": "or",
        },
        "renameOutput": True, "outputKey": "trade",
    }]},
    "options": {},
}, position=[2640, 0])

http("IggyLogin", "POST", IGGY_HTTP + "/users/login", position=[2860, -110], on_error=None,
     json_body=('{"username":{{ JSON.stringify($env.SLR_IGGY_USER || "iggy") }},'
                '"password":{{ JSON.stringify($env.SLR_IGGY_PASSWORD || "iggy") }}}'))
code("BuildOrder", BUILD_ORDER_JS, [3080, -110])
http("PublishOrder", "POST", IGGY_HTTP + "/streams/crypto/topics/orders/messages",
     position=[3300, -110], on_error=None,
     headers=[{"name": "Authorization", "value": "Bearer {{ $json.token }}"}],
     json_body="={{ JSON.stringify($json.body) }}")
http("Annotate", "POST", GRAFANA + "/api/annotations", position=[3520, -110], credentials=GRAFANA_CRED,
     json_body=('{"dashboardUID":"crypto-live-ticks","tags":["slr","trade"],'
                '"text":{{ JSON.stringify("SLR " + $("RiskDecide").item.json.action + " " + $("BuildOrder").item.json.order.pair + " qty=" + $("BuildOrder").item.json.order.quantity + " @ $" + $("BuildOrder").item.json.order.price + " — " + ($("RiskDecide").item.json.narrative || "")) }}}'))

code("CommitState", COMMIT_STATE_JS, [3520, 110])
http("MirrorState", "POST", QUERY_API + "/api/state", position=[3740, 110],
     json_body="={{ JSON.stringify($json.state) }}")
node("IfNarrative", "n8n-nodes-base.if", 2.2, {
    "conditions": {
        "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "loose"},
        "conditions": [{
            "leftValue": "={{ ($json.cycle_count || 0) % 5 }}", "rightValue": 0,
            "operator": {"type": "number", "operation": "equals"},
        }],
        "combinator": "and",
    },
    "options": {},
}, position=[3960, 110])
http("AnnotateNarrative", "POST", GRAFANA + "/api/annotations", position=[4180, 40],
     credentials=GRAFANA_CRED,
     json_body=('{"dashboardUID":"crypto-live-ticks","tags":["slr","narrative"],'
                '"text":{{ JSON.stringify("SLR narrative: " + ($("RiskDecide").item.json.narrative || "(none)")) }}}'))

CONNECTIONS = {
    "Schedule": {"main": [[{"node": "Init", "type": "main", "index": 0}]]},
    "Webhook": {"main": [[{"node": "KillSwitch", "type": "main", "index": 0}]]},
    "KillSwitch": {"main": [[{"node": "RespondKillswitch", "type": "main", "index": 0}]]},
    "Init": {"main": [[{"node": "PromPrices", "type": "main", "index": 0}]]},
    "PromPrices": {"main": [[{"node": "PromPriceLag", "type": "main", "index": 0}]]},
    "PromPriceLag": {"main": [[{"node": "PromSignalLag", "type": "main", "index": 0}]]},
    "PromSignalLag": {"main": [[{"node": "FetchBalance", "type": "main", "index": 0}]]},
    "FetchBalance": {"main": [[{"node": "SplitInBatches", "type": "main", "index": 0}]]},
    "SplitInBatches": {"main": [
        [],  # done → 结束
        [{"node": "FetchSignal", "type": "main", "index": 0}],  # loop
    ]},
    "FetchSignal": {"main": [[{"node": "BuildPrompt", "type": "main", "index": 0}]]},
    "BuildPrompt": {"main": [[{"node": "LLM", "type": "main", "index": 0}]]},
    "LLM": {"main": [[{"node": "ParseSentiment", "type": "main", "index": 0}]]},
    "ParseSentiment": {"main": [[{"node": "RiskDecide", "type": "main", "index": 0}]]},
    "RiskDecide": {"main": [[{"node": "Switch", "type": "main", "index": 0}]]},
    "Switch": {"main": [
        [{"node": "IggyLogin", "type": "main", "index": 0}],   # trade (BUY/SELL)
        [{"node": "CommitState", "type": "main", "index": 0}],  # fallback (HOLD/BLOCKED)
    ]},
    "IggyLogin": {"main": [[{"node": "BuildOrder", "type": "main", "index": 0}]]},
    "BuildOrder": {"main": [[{"node": "PublishOrder", "type": "main", "index": 0}]]},
    "PublishOrder": {"main": [[{"node": "Annotate", "type": "main", "index": 0}]]},
    "Annotate": {"main": [[{"node": "CommitState", "type": "main", "index": 0}]]},
    "CommitState": {"main": [[{"node": "MirrorState", "type": "main", "index": 0}]]},
    "MirrorState": {"main": [[{"node": "IfNarrative", "type": "main", "index": 0}]]},
    "IfNarrative": {"main": [
        [{"node": "AnnotateNarrative", "type": "main", "index": 0}],  # true
        [{"node": "SplitInBatches", "type": "main", "index": 0}],     # false → loopback
    ]},
    "AnnotateNarrative": {"main": [[{"node": "SplitInBatches", "type": "main", "index": 0}]]},
}

WORKFLOW = {
    "id": "slr-trading-decision",
    "name": "SLR Trading Decision",
    "active": False,
    "settings": {"executionOrder": "v1"},
    "nodes": _nodes,
    "connections": CONNECTIONS,
    "pinData": {},
    "meta": {"templateCredsSetupCompleted": True},
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--output", default=str(ROOT / "workflows/trading-decision.json"))
    args = ap.parse_args()
    Path(args.output).write_text(
        json.dumps(WORKFLOW, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"built {args.output} ({len(_nodes)} nodes)")


if __name__ == "__main__":
    main()
