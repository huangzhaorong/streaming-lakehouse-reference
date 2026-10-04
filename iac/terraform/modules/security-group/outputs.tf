# ============================================================
# 安全组模块 — 输出
# ============================================================

output "security_group_id" {
  description = "安全组 ID"
  value       = alicloud_security_group.this.id
}

output "security_group_name" {
  description = "安全组名称"
  value       = alicloud_security_group.this.security_group_name
}