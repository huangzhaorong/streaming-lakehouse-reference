# ============================================================
# RAM 角色模块 — 变量
# ============================================================

variable "role_name" {
  description = "RAM 角色名称"
  type        = string
}

variable "role_description" {
  description = "RAM 角色描述"
  type        = string
  default     = "Lakehouse OSS 访问角色"
}

variable "oss_bucket_names" {
  description = "授权访问的 OSS Bucket 名称列表"
  type        = list(string)
}

variable "tags" {
  description = "资源标签"
  type        = map(string)
  default     = {}
}