# Writes an Ansible inventory (and a plain host list) describing the cluster,
# so the nodes provisioned here can be configured straight away.
resource "local_file" "ansible_inventory" {
  filename = "${path.module}/generated-inventory.yml"
  content = yamlencode({
    all = {
      children = {
        iot_cluster = {
          vars = {
            ansible_python_interpreter = "/usr/bin/python3"
            ip_family                  = local.ip_family
            ip_family_primary          = local.ip_family_primary
          }
          children = {
            k3s_server = {
              vars = { k3s_role = "server" }
              hosts = {
                (local.server_addr) = { ansible_user = var.ssh_user }
              }
            }
            k3s_agent = {
              vars = {
                k3s_role        = "agent"
                k3s_server_host = local.server_addr
              }
              hosts = { for a in openstack_compute_instance_v2.agent :
                local.conn_addr[a.name] => { ansible_user = var.ssh_user }
              }
            }
          }
        }
      }
    }
  })
}
