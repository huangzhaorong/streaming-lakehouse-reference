# ============================================================
# OSS Bucket 模块 — 输出
# ============================================================

output "bucket_name" {
  description = "Bucket 名称"
  value       = var.bucket_name
}

output "bucket_endpoint" {
  description = "内网 Endpoint"
  value       = "oss-${var.region}-internal.aliyuncs.com"
}

output "bucket_external_endpoint" {
  description = "外网 Endpoint"
  value       = "oss-${var.region}.aliyuncs.com"
}