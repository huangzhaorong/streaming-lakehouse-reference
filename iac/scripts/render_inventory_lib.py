#!/usr/bin/env python3
"""render-inventory 纯逻辑库（可测）。CLI 壳见 render-inventory.py。
模式承袭 adtrader-iac-infra/scripts/render_inventory_lib.py（精简：单环境、无共置双填）。

从 terraform output -json 构建 Ansible INI inventory 的模板变量并渲染。
顺序即语义：渲染顺序 = terraform output 列表顺序 = tfvars private_ips/hostnames 顺序，
永不重排（Fluss tablet id / ZK myid 等吃此顺序）。
"""
import re
from datetime import datetime

# TF 实例组键 → Ansible 组名（本环境同名直通）
GROUP_MAP = {
    "dataplane": "dataplane",
    "appplane": "appplane",
    "computeplane": "computeplane",
}


def _unwrap(node, default=None):
    """terraform output -json 的值可能带 {"value": ...} 包装，也可能已是裸值。"""
    if isinstance(node, dict) and "value" in node:
        return node["value"]
    return node if node is not None else default


def _render_host(instances):
    """实例列表 → inventory 主机行（含 ansible_user，自包含不依赖 group_vars）。顺序保持。"""
    lines = []
    for inst in instances:
        hostname = inst.get("instance_name", inst.get("ansible_host", "unknown"))
        ip = inst.get("ansible_host", "")
        lines.append(f"{hostname} ansible_host={ip} ansible_user={inst.get('ansible_user', 'root')}")
    return "\n".join(lines)


def build_template_vars(tf_data):
    """tf output JSON → 模板变量 dict。"""
    ansible_inv = _unwrap(tf_data.get("ansible_inventory", {}), {})
    vars_ = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    for tf_group, instances in ansible_inv.items():
        if tf_group not in GROUP_MAP:
            continue
        vars_[GROUP_MAP[tf_group]] = _render_host(_unwrap(instances, []))
    return vars_


def render(template_vars, template_text):
    """占位符替换渲染（${key}）。"""
    rendered = template_text
    for key, val in template_vars.items():
        rendered = rendered.replace(f"${{{key}}}", val)
    return rendered


def find_leftover(rendered):
    """返回未被填充的 ${...} 占位符列表（防模板/GROUP_MAP 漂移静默产出坏 inventory）。"""
    return sorted(set(re.findall(r"\$\{[a-z_]+\}", rendered)))
