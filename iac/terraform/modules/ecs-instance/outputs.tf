# ============================================================
# ECS 实例模块 — 输出
# ============================================================

output "instances" {
  description = "实例详情 Map"
  value = {
    for i in range(var.instance_count) :
    i => {
      instance_id   = alicloud_instance.this[i].id
      instance_name = alicloud_instance.this[i].instance_name
      private_ip    = alicloud_instance.this[i].private_ip
      public_ip     = alicloud_instance.this[i].public_ip
      ansible_host  = alicloud_instance.this[i].private_ip
      ansible_user  = "root"
      host_group    = var.instance_group
    }
  }
}

output "private_ips" {
  description = "内网 IP 列表"
  value       = alicloud_instance.this[*].private_ip
}

output "instance_ids" {
  description = "实例 ID 列表"
  value       = alicloud_instance.this[*].id
}

output "data_disk_ids" {
  description = "数据盘 ID 列表"
  value       = alicloud_disk.data[*].id
}