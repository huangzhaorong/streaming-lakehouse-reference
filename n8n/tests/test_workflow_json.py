"""trading-decision.json 结构门禁：
1) 可解析、id/name/active 正确；2) 必备节点齐全；3) connections 引用闭合；
4) RiskDecide jsCode 与 workflows/decide.js 逐字同步；5) 构建可重现（rebuild == committed）。
运行: PYTHONPATH=services/query-api/_deps python3 -m pytest n8n/tests/test_workflow_json.py -q
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WF = ROOT / "n8n/workflows/trading-decision.json"
DECIDE_JS = ROOT / "n8n/workflows/decide.js"
BUILD = ROOT / "n8n/build_workflow.py"

REQUIRED_NODES = {
    "Schedule", "Webhook", "KillSwitch", "RespondKillswitch",
    "Init", "PromPrices", "PromPriceLag", "PromSignalLag", "FetchBalance",
    "SplitInBatches", "FetchSignal", "BuildPrompt", "LLM", "ParseSentiment",
    "RiskDecide", "Switch", "IggyLogin", "BuildOrder", "PublishOrder",
    "Annotate", "CommitState", "MirrorState", "IfNarrative", "AnnotateNarrative",
}


def load():
    return json.loads(WF.read_text(encoding="utf-8"))


def test_basics():
    wf = load()
    assert wf["id"] == "slr-trading-decision"
    assert wf["name"] == "SLR Trading Decision"
    assert wf["active"] is False  # publish 由 ansible/CLI 控制，入库默认不激活
    assert wf["settings"]["executionOrder"] == "v1"


def test_required_nodes_present():
    wf = load()
    names = {n["name"] for n in wf["nodes"]}
    missing = REQUIRED_NODES - names
    assert not missing, f"缺节点: {missing}"
    ids = [n.get("id") for n in wf["nodes"]]
    assert all(ids) and len(set(ids)) == len(ids), "节点 id 缺失或重复"


def test_connections_reference_existing_nodes():
    wf = load()
    names = {n["name"] for n in wf["nodes"]}
    for src, conns in wf["connections"].items():
        assert src in names, f"connections 源节点不存在: {src}"
        for outputs in conns.values():
            for branch in outputs:
                for c in branch or []:
                    assert c["node"] in names, f"connections 目标节点不存在: {c['node']}"


def test_riskdecide_jscode_in_sync_with_decide_js():
    wf = load()
    node = next(n for n in wf["nodes"] if n["name"] == "RiskDecide")
    assert node["parameters"]["jsCode"] == DECIDE_JS.read_text(encoding="utf-8"), (
        "RiskDecide jsCode 与 decide.js 不同步 — 运行 python3 n8n/build_workflow.py 重新生成"
    )


def test_rebuild_deterministic():
    wf_committed = WF.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "rebuilt.json"
        subprocess.run([sys.executable, str(BUILD), "-o", str(out)], check=True, cwd=ROOT)
        assert out.read_text(encoding="utf-8") == wf_committed, "已提交 JSON 与重新构建不一致"


def test_credentials_referenced():
    wf = load()
    llm = next(n for n in wf["nodes"] if n["name"] == "LLM")
    assert llm["credentials"]["httpHeaderAuth"]["name"] == "DashScope"
    for name in ("Annotate", "AnnotateNarrative"):
        node = next(n for n in wf["nodes"] if n["name"] == name)
        assert node["credentials"]["httpBasicAuth"]["name"] == "Grafana"


def test_cycle_fetch_chain_precedes_pair_fanout():
    """C1 回归门禁：周期级 HTTP 抓取必须在 Init(pair fan-out) 之前——
    HTTP 节点会以响应替换输入条目，若放在 fan-out 之后，pair 条目会被覆盖。"""
    wf = load()
    conns = wf["connections"]

    def targets(src, out=0):
        return [c["node"] for c in (conns.get(src, {}).get("main", [[]])[out] or [])]

    chain = ["Schedule", "PromPrices", "PromPriceLag", "PromSignalLag", "FetchBalance", "Init", "SplitInBatches"]
    for a, b in zip(chain, chain[1:]):
        assert targets(a) == [b], f"{a} 的下游应为 [{b}]，实际 {targets(a)}"
    # fan-out 到 SplitInBatches 之间不得再出现 httpRequest 节点
    by_name = {n["name"]: n for n in wf["nodes"]}
    assert by_name["SplitInBatches"]["type"] == "n8n-nodes-base.splitInBatches"
    assert by_name["Init"]["type"] == "n8n-nodes-base.code"
    loop_targets = conns["SplitInBatches"]["main"][1]
    assert [c["node"] for c in loop_targets] == ["FetchSignal"]


def test_killswitch_webhook_wired():
    wf = load()
    conns = wf["connections"]
    assert "Webhook" in conns
    targets = [c["node"] for c in conns["Webhook"]["main"][0]]
    assert "KillSwitch" in targets
