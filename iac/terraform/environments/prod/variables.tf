# ============================================================
# SLR prod 环境 — 变量定义
# ============================================================

variable "region" {
  description = "阿里云地域"
  type        = string
  default     = "cn-beijing"
}

variable "vpc_name" {
  description = "已有 VPC 名称（data source 只读引用，不新建网络）"
  type        = string
}

variable "vswitch_cidr" {
  description = "已有交换机 CIDR（data source 只读引用）"
  type        = string
}

variable "image_name_regex" {
  description = "系统镜像正则（Alibaba Cloud Linux 3）"
  type        = string
  default     = "^aliyun_3_x64_20G_alibase"
}

variable "admin_cidrs" {
  description = "SSH 公网白名单 CIDR 列表（如办公网出口/32）。唯一公网入向"
  type        = list(string)
  default     = []
}

variable "ui_cidrs" {
  description = "可选：UI 端口（Grafana 3001 / n8n 5678 / Flink 8081 / Prometheus 9090）直连白名单。默认空 = 一律走 SSH 隧道"
  type        = list(string)
  default     = []
}

variable "ssh_public_key" {
  description = "SSH 公钥内容。tfvars 可用 file(\"~/.ssh/id_ed25519.pub\") 引用"
  type        = string
  default     = ""
}

variable "oss_bucket_name" {
  description = "湖仓桶（OSS-HDFS，控制台带外开通 HDFS 语义）"
  type        = string
  default     = "slr-lakehouse-prod"
}

variable "artifacts_bucket_name" {
  description = "部署工件桶（public-read：全为公开上游二进制 + iggy 提取件，无秘密）"
  type        = string
  default     = "slr-artifacts-prod"
}

variable "ram_role_name" {
  description = "ECS 绑定的 RAM 角色（OSS 免 AK，instance metadata 取凭证）"
  type        = string
  default     = "slr-oss-role"
}

variable "slr_instances" {
  description = <<-EOT
    ECS 实例组。固定两组：dataplane（Iggy/ZK/Fluss/Flink）、appplane（Python/n8n/监控）。
    private_ips/hostnames 顺序即语义（与 count 等长，禁止静默自动分配）。
  EOT
  type = map(object({
    instance_type               = string
    count                       = number
    disk_size                   = number
    data_disk_size              = optional(number, 0)
    data_disk_performance_level = optional(string, "PL1")
    private_ips                 = list(string)
    hostnames                   = list(string)
    hostname_prefix             = string
    internet_max_bandwidth_out  = optional(number, 100)
    charge_type                 = optional(string, "PostPaid")
    spot_strategy               = optional(string, "NoSpot")
    period                      = optional(number, 1)
  }))

  validation {
    condition = alltrue([
      for g in var.slr_instances :
      length(g.private_ips) == g.count && length(g.hostnames) == g.count
    ])
    error_message = "每组 private_ips/hostnames 长度必须等于 count——IP/主机名顺序绑定，禁止静默自动分配。"
  }
}

variable "tags" {
  description = "公共资源标签"
  type        = map(string)
  default = {
    Project   = "slr"
    ManagedBy = "terraform"
    Component = "streaming-lakehouse"
  }
}
