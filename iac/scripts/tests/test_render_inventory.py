"""render_inventory_lib 门禁测试（真实 inventory.tmpl 驱动）。
运行: python3 -m pytest iac/scripts/tests/ -q
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "iac/scripts"))

import render_inventory_lib as ri  # noqa: E402

TMPL = (ROOT / "iac/terraform/environments/prod/inventory.tmpl").read_text(encoding="utf-8")

TF_OUTPUT = {
    "ansible_inventory": {
        "value": {
            "dataplane": [
                {"instance_name": "ecs-data-01", "ansible_host": "10.0.0.11", "ansible_user": "root"},
            ],
            "appplane": [
                {"instance_name": "ecs-app-01", "ansible_host": "10.0.0.12", "ansible_user": "root"},
                {"instance_name": "ecs-app-02", "ansible_host": "10.0.0.13", "ansible_user": "root"},
            ],
            "computeplane": [
                {"instance_name": "ecs-compute-01", "ansible_host": "10.0.0.14", "ansible_user": "root"},
            ],
        }
    }
}


def render(tf_output=TF_OUTPUT, tmpl=TMPL):
    return ri.render(ri.build_template_vars(tf_output), tmpl)


def test_groups_rendered_with_hosts():
    out = render()
    assert "[dataplane]\necs-data-01 ansible_host=10.0.0.11 ansible_user=root" in out
    assert "[appplane]\necs-app-01 ansible_host=10.0.0.12 ansible_user=root" in out
    assert "[computeplane]\necs-compute-01 ansible_host=10.0.0.14 ansible_user=root" in out
    assert "[cluster:children]\ndataplane\nappplane\ncomputeplane" in out


def test_order_preserved():
    out = render()
    app = out.split("[appplane]\n", 1)[1].split("\n\n", 1)[0]
    lines = [line for line in app.splitlines() if line.strip()]
    assert lines[0].startswith("ecs-app-01")
    assert lines[1].startswith("ecs-app-02")  # 顺序即语义，永不重排


def test_no_leftover_placeholders():
    assert ri.find_leftover(render()) == []


def test_leftover_detected():
    out = render(tmpl="[dataplane]\n${dataplane}\n[broken]\n${unknown_group}\n")
    assert ri.find_leftover(out) == ["${unknown_group}"]


def test_plain_dict_tf_output_also_works():
    """terraform output -json 顶层可能直接是值（无 {value:...} 包装）。"""
    tf = {"ansible_inventory": TF_OUTPUT["ansible_inventory"]["value"]}
    assert "[dataplane]" in render(tf_output=tf)
