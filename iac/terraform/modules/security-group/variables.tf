# ============================================================
# 安全组模块 — 变量
# ============================================================

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "sg_name" {
  description = "安全组名称"
  type        = string
}

variable "sg_description" {
  description = "安全组描述"
  type        = string
  default     = ""
}

variable "rules" {
  description = "安全组规则列表"
  type = list(object({
    description  = string
    direction    = string # "ingress" | "egress"
    port_range   = string # "8081/8081" | "1/65535"
    protocol     = string # "tcp" | "udp" | "all"
    cidr_ip      = optional(string)
    source_sg_id = optional(string)
  }))
  default = []
}

variable "inner_access_policy" {
  description = "组内互通策略：Accept=同组实例全端口互通（普通安全组默认）；Drop=组内隔离；null=不设置沿用默认"
  type        = string
  default     = null

  validation {
    condition     = var.inner_access_policy == null || contains(["Accept", "Drop"], var.inner_access_policy)
    error_message = "inner_access_policy 只能是 Accept、Drop 或 null"
  }
}

variable "tags" {
  description = "资源标签"
  type        = map(string)
  default     = {}
}