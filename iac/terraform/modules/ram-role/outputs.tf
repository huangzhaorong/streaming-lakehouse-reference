# ============================================================
# RAM 角色模块 — 输出
# ============================================================

output "role_name" {
  description = "RAM 角色名称"
  value       = var.role_name
}

output "role_arn" {
  description = "RAM 角色 ARN"
  value       = alicloud_ram_role.this.arn
}

output "policy_name" {
  description = "OSS 访问策略名称"
  value       = alicloud_ram_policy.oss_access.policy_name
}