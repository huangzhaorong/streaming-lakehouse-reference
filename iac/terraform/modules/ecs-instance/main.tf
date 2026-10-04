# ============================================================
# ECS 实例模块 — 主资源定义
# ============================================================

locals {
  # 主机名解析：显式 hostnames 列表优先（atx_v2 续编命名 kafka04/vw07 等，
  # 同时写入 instance_name 与 OS host_name，保证与 Ansible inventory 一致）；
  # 未传时保持原行为 "${prefix}-${index+1}"（atx/lucia/atxvpc 零影响）。
  names = length(var.hostnames) > 0 ? var.hostnames : [
    for i in range(var.instance_count) : "${var.hostname_prefix}-${i + 1}"
  ]
}

# 创建 ECS 实例
resource "alicloud_instance" "this" {
  count = var.instance_count

  instance_name = local.names[count.index]
  host_name     = local.names[count.index]

  image_id      = var.image_id
  instance_type = var.instance_type

  vswitch_id = var.vswitch_id

  security_groups = [var.security_group_id]

  private_ip = length(var.private_ips) > count.index ? var.private_ips[count.index] : null

  system_disk_category          = var.system_disk_category
  system_disk_size              = var.system_disk_size
  system_disk_performance_level = var.system_disk_performance_level

  instance_charge_type = var.charge_type
  period               = var.charge_type == "PrePaid" ? var.period : null
  period_unit          = var.charge_type == "PrePaid" ? var.period_unit : null

  spot_strategy = var.charge_type == "PostPaid" ? var.spot_strategy : "NoSpot"

  internet_max_bandwidth_out = var.internet_max_bandwidth_out
  internet_charge_type       = var.internet_max_bandwidth_out > 0 ? var.internet_charge_type : null

  user_data = var.user_data != "" ? var.user_data : null

  password = var.password != "" ? var.password : null

  key_name = var.key_name != "" ? var.key_name : null

  tags = merge(var.tags, {
    HostGroup = var.instance_group
    Index     = tostring(count.index + 1)
  })

  lifecycle {
    ignore_changes = [password]
  }
}

# 绑定 RAM 角色到实例
resource "alicloud_ecs_ram_role_attachment" "this" {
  count = var.ram_role_name != "" ? var.instance_count : 0

  instance_id   = alicloud_instance.this[count.index].id
  ram_role_name = var.ram_role_name
}

# 数据盘（可选）
resource "alicloud_disk" "data" {
  count = var.data_disk_size > 0 ? var.instance_count : 0

  disk_name = "data-${local.names[count.index]}"
  category  = var.data_disk_category
  size      = var.data_disk_size

  performance_level = var.data_disk_category == "cloud_essd" ? var.data_disk_performance_level : null

  # provider 1.289 实证（providers schema）：alicloud_instance 导出 availability_zone，
  # 无 zone_id 属性（原写法为潜伏 bug——旧 lakehouse TF 从未真正 apply 过所以未暴露）
  zone_id = alicloud_instance.this[count.index].availability_zone

  tags = merge(var.tags, {
    HostGroup = var.instance_group
    Index     = tostring(count.index + 1)
    DiskType  = "data"
  })
}

# 挂载数据盘到实例
resource "alicloud_disk_attachment" "data" {
  count = var.data_disk_size > 0 ? var.instance_count : 0

  disk_id     = alicloud_disk.data[count.index].id
  instance_id = alicloud_instance.this[count.index].id
}