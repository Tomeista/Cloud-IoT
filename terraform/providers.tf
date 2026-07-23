terraform {
  required_version = ">= 1.3"

  required_providers {
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    dns = {
      source  = "hashicorp/dns"
      version = "~> 3.4"
    }
  }
}

# Authentication comes from clouds.yaml (the entry named by var.os_cloud). Put
# clouds.yaml in this directory, in ~/.config/openstack/, or point
# OS_CLIENT_CONFIG_FILE at it. The secret stays in that file, never in the code.
# To authenticate from OS_* environment variables instead, set os_cloud = null.
provider "openstack" {
  cloud = var.os_cloud
}
