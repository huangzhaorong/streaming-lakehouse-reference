# ============================================================
# 安全组模块 — 主资源定义
# ============================================================

# 创建安全组
resource "alicloud_security_group" "this" {
  security_group_name = var.sg_name
  description         = var.sg_description != "" ? var.sg_description : "Lakehouse security group - ${var.sg_name}"
  vpc_id              = var.vpc_id

  # 组内互通策略（可选）：Accept = 同组实例全端口互通（普通安全组默认值）。
  # atx_v2 显式设置——阿里云 API 拒绝 SG 自引用规则（T0 apply 实证
  # InvalidParamter.Conflict），组内互访靠本策略而非规则条目。
  # null（默认）= 不设置，沿用 provider/账号默认，旧环境行为不变。
  inner_access_policy = var.inner_access_policy

  tags = var.tags
}

# 创建安全组规则
# source_sg_id = "self" 为哨兵值：解析为本模块创建的 SG 自身 id（集群内互访自引用）。
# 现有环境（atx/lucia/atxvpc）从不传 "self"，行为逐字节不变。
resource "alicloud_security_group_rule" "this" {
  for_each = { for i, r in var.rules : i => r }

  security_group_id        = alicloud_security_group.this.id
  type                     = each.value.direction
  ip_protocol              = each.value.protocol
  port_range               = each.value.port_range
  cidr_ip                  = each.value.source_sg_id == "self" ? null : each.value.cidr_ip
  source_security_group_id = each.value.source_sg_id == "self" ? alicloud_security_group.this.id : each.value.source_sg_id
  description              = each.value.description
}