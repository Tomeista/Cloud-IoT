# Optional: publish an AAAA record for the server into the DHBW dynamic-DNS
# zone via RFC2136 + TSIG. Enable with enable_dns = true and set the dns_*
# variables (the TSIG key/secret come from your .env). The record points at the
# k3s server node, so the web UI is reachable by name instead of raw IPv6.
provider "dns" {
  update {
    server        = var.dns_server
    port          = var.dns_port
    key_name      = var.dns_key_name
    key_algorithm = var.dns_key_algorithm
    key_secret    = var.dns_key_secret
  }
}

resource "dns_aaaa_record_set" "web" {
  count     = var.enable_dns ? 1 : 0
  zone      = "${var.dns_zone}."
  name      = var.dns_name
  addresses = [openstack_compute_instance_v2.server.network[0].fixed_ip_v6]
  ttl       = var.dns_ttl
}
