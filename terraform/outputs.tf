locals {
  all_nodes = concat([openstack_compute_instance_v2.server], openstack_compute_instance_v2.agent)

  # A family is usable cluster-wide only when every node has an address for it.
  cluster_has_v6 = alltrue([for n in local.all_nodes : length(n.network[0].fixed_ip_v6) > 0])
  cluster_has_v4 = alltrue([for n in local.all_nodes : length(n.network[0].fixed_ip_v4) > 0])

  ip_family         = local.cluster_has_v6 ? (local.cluster_has_v4 ? "dual" : "ipv6") : "ipv4"
  ip_family_primary = "ipv6" # only consulted for "dual"; IPv4 is NAT-only here, so IPv6 leads.

  # Connection address per node: IPv6 when available, the NAT IPv4 otherwise.
  conn_addr = { for n in local.all_nodes :
    n.name => local.cluster_has_v6 ? n.network[0].fixed_ip_v6 : n.network[0].fixed_ip_v4
  }
  server_addr = local.conn_addr[openstack_compute_instance_v2.server.name]
}

output "server_ip" {
  description = "Address of the k3s control-plane node."
  value       = local.server_addr
}

output "agent_ips" {
  description = "Addresses of the k3s agent nodes."
  value       = [for a in openstack_compute_instance_v2.agent : local.conn_addr[a.name]]
}

output "ip_family" {
  description = "Detected cluster IP family: ipv4, ipv6 or dual."
  value       = local.ip_family
}

output "ssh_server" {
  description = "Ready-to-use SSH command for the server node."
  value       = "ssh ${var.ssh_user}@${local.server_addr}"
}

output "web_ui" {
  description = "URL of the web UI once the stack is deployed (deploy_stack = true)."
  value       = local.cluster_has_v6 ? "http://[${local.server_addr}]:30080" : "http://${local.server_addr}:30080"
}

output "web_ui_dns" {
  description = "Hostname-based URL of the web UI (only when enable_dns = true)."
  value       = var.enable_dns ? "http://${var.dns_name == "@" ? var.dns_zone : "${var.dns_name}.${var.dns_zone}"}:30080" : null
}

output "bootstrap_hint" {
  description = "How to watch the automated bootstrap progress."
  value       = "ssh ${var.ssh_user}@${local.server_addr} 'tail -f /var/log/iot-bootstrap.log' — done when /var/log/iot-bootstrap.done exists."
}
