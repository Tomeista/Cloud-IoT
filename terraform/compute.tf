# Shared k3s join token. Baked into both server and agent cloud-init so agents
# never need to read it back from the server (avoids a boot-ordering race).
resource "random_password" "k3s_token" {
  length  = 40
  special = false
}

# k3s server (control-plane) node. cloud-init installs k3s, builds the images
# and — when deploy_stack is true — installs the Helm release and Flink job.
resource "openstack_compute_instance_v2" "server" {
  name            = "${var.vm_prefix}-server"
  image_id        = var.image_id
  flavor_name     = var.flavor_name
  key_pair        = local.key_pair
  security_groups = ["default", openstack_networking_secgroup_v2.cluster.name]

  network { name = var.network_name }

  user_data = templatefile("${path.module}/cloud-init/server.sh.tftpl", {
    token          = random_password.k3s_token.result
    repo_url       = var.repo_url
    repo_branch    = var.repo_branch
    image_registry = var.image_registry
    namespace      = var.k8s_namespace
    cluster_cidr   = var.cluster_cidr
    service_cidr   = var.service_cidr
    deploy_stack   = var.deploy_stack
  })

  timeouts { create = "10m" }
}

# k3s agent (worker) nodes. They build the same images and join using the
# server's IPv6 address and the shared token.
resource "openstack_compute_instance_v2" "agent" {
  count           = var.agent_count
  name            = "${var.vm_prefix}-agent-${count.index + 1}"
  image_id        = var.image_id
  flavor_name     = var.flavor_name
  key_pair        = local.key_pair
  security_groups = ["default", openstack_networking_secgroup_v2.cluster.name]

  network { name = var.network_name }

  user_data = templatefile("${path.module}/cloud-init/agent.sh.tftpl", {
    token          = random_password.k3s_token.result
    repo_url       = var.repo_url
    repo_branch    = var.repo_branch
    image_registry = var.image_registry
    server_ip      = openstack_compute_instance_v2.server.network[0].fixed_ip_v6
  })

  timeouts { create = "10m" }
}
