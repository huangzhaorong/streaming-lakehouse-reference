# ============================================================
# OSS Bucket 模块 — 变量
# ============================================================

variable "bucket_name" {
  description = "OSS Bucket 名称"
  type        = string
}

variable "region" {
  description = "阿里云地域"
  type        = string
}

variable "lifecycle_rules" {
  description = "生命周期规则列表（transitions: Standard→IA/Archive 冷数据分层）"
  type = list(object({
    id              = string
    prefix          = string
    expiration_days = number
    status          = optional(string, "Enabled")
    transitions     = optional(list(object({ days = number, storage_class = string })), [])
  }))
  default = []
}

variable "versioning_enabled" {
  description = "是否启用版本控制"
  type        = bool
  default     = false
}

variable "tags" {
  description = "资源标签"
  type        = map(string)
  default     = {}
}
variable "acl" {
  description = "Bucket ACL。private=默认；public-read 仅用于纯公开二进制的 artifacts 桶"
  type        = string
  default     = "private"

  validation {
    condition     = contains(["private", "public-read"], var.acl)
    error_message = "acl 只允许 private 或 public-read"
  }
}
