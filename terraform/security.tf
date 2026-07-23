# --- SSH keypair -------------------------------------------------------------
# Created only when public_key_path is set; otherwise an existing keypair with
# name key_pair_name is reused.
resource "openstack_compute_keypair_v2" "this" {
  count      = var.public_key_path == "" ? 0 : 1
  name       = var.key_pair_name
  public_key = file(pathexpand(var.public_key_path))
}

locals {
  key_pair = var.public_key_path == "" ? var.key_pair_name : openstack_compute_keypair_v2.this[0].name
}

# --- Security group ----------------------------------------------------------
resource "openstack_networking_secgroup_v2" "cluster" {
  name        = "${var.vm_prefix}-cluster"
  description = "IoT k3s cluster: SSH, k3s API, web NodePort and intra-cluster traffic"
}

# Allow everything between members of this group (flannel VXLAN, kubelet, Kafka,
# etc.) for both IP families.
resource "openstack_networking_secgroup_rule_v2" "intra_v4" {
  direction         = "ingress"
  ethertype         = "IPv4"
  remote_group_id   = openstack_networking_secgroup_v2.cluster.id
  security_group_id = openstack_networking_secgroup_v2.cluster.id
}

resource "openstack_networking_secgroup_rule_v2" "intra_v6" {
  direction         = "ingress"
  ethertype         = "IPv6"
  remote_group_id   = openstack_networking_secgroup_v2.cluster.id
  security_group_id = openstack_networking_secgroup_v2.cluster.id
}

# ICMPv6 (neighbour discovery) is mandatory for IPv6 to function at all.
resource "openstack_networking_secgroup_rule_v2" "icmp_v6" {
  direction         = "ingress"
  ethertype         = "IPv6"
  protocol          = "ipv6-icmp"
  remote_ip_prefix  = "::/0"
  security_group_id = openstack_networking_secgroup_v2.cluster.id
}

resource "openstack_networking_secgroup_rule_v2" "icmp_v4" {
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "icmp"
  remote_ip_prefix  = "0.0.0.0/0"
  security_group_id = openstack_networking_secgroup_v2.cluster.id
}

# Publicly reachable TCP ports (SSH, k3s API, web NodePort) for both families.
resource "openstack_networking_secgroup_rule_v2" "public_v6" {
  for_each          = toset([for p in var.public_tcp_ports : tostring(p)])
  direction         = "ingress"
  ethertype         = "IPv6"
  protocol          = "tcp"
  port_range_min    = tonumber(each.value)
  port_range_max    = tonumber(each.value)
  remote_ip_prefix  = "::/0"
  security_group_id = openstack_networking_secgroup_v2.cluster.id
}

resource "openstack_networking_secgroup_rule_v2" "public_v4" {
  for_each          = toset([for p in var.public_tcp_ports : tostring(p)])
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = tonumber(each.value)
  port_range_max    = tonumber(each.value)
  remote_ip_prefix  = "0.0.0.0/0"
  security_group_id = openstack_networking_secgroup_v2.cluster.id
}
