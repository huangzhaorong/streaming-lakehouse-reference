# ============================================================
# SLR prod — Terraform 输出（render-inventory 与 Ansible group_vars 消费）
# ★ ansible_inventory 顺序 = tfvars private_ips 顺序，永不重排
# ============================================================

output "ansible_inventory" {
  description = "Ansible inventory 格式实例信息（dataplane/appplane 两组）"
  value = {
    for group_key, module in module.ecs_instances :
    group_key => [
      for i in range(length(module.private_ips)) :
      {
        instance_name = module.instances[i].instance_name
        ansible_host  = module.private_ips[i]
        ansible_user  = "root"
      }
    ]
  }
}

output "instance_registry" {
  description = "各组实例 ID 列表"
  value       = { for g, m in module.ecs_instances : g => m.instance_ids }
}

output "data_ip" {
  description = "数据面私网 IP（Python 服务 IGGY_HOST、Prometheus 抓 Flink 9249 等跨机地址）"
  value       = module.ecs_instances["dataplane"].private_ips[0]
}

output "app_ip" {
  description = "应用面私网 IP"
  value       = module.ecs_instances["appplane"].private_ips[0]
}

output "security_group_id" {
  value = module.security_group.security_group_id
}

output "bucket_lakehouse" {
  description = "湖仓桶（OSS-HDFS，控制台带外开通）"
  value       = module.oss_bucket.bucket_name
}

output "bucket_lakehouse_state" {
  description = "Flink state 备用桶（R1 回退路径用）"
  value       = module.oss_bucket_flink_state.bucket_name
}

output "bucket_artifacts" {
  description = "部署工件桶（public-read）"
  value       = module.oss_bucket_artifacts.bucket_name
}

output "oss_dls_endpoint" {
  description = "OSS-HDFS 区域级 dls endpoint（Flink fs.oss.endpoint / JindoFuse）"
  value       = "${var.region}.oss-dls.aliyuncs.com"
}

output "oss_internal_endpoint" {
  description = "OSS 内网 endpoint（R1 回退：普通 OSS 命名空间时用）"
  value       = "oss-${var.region}-internal.aliyuncs.com"
}

output "vpc_id" {
  value = data.alicloud_vpcs.main.ids[0]
}

output "vswitch_id" {
  value = data.alicloud_vswitches.main.ids[0]
}
