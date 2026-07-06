variable "image_id" {
  # Ubuntu Server 24.04
  default = "7842eb53-0ac7-4677-9160-2466371b4302"
}

variable "flavor_name" {
  default = "mb1.large"
}

variable "key_pair" {
  default = "Dennis 2025"
}

variable "network_name" {
  default = "DHBWv6"
}

variable "vm_prefix" {
  default = "pfisterer-gridflex" # please choose a prefix for your VMs, e.g., "groupname-"
}
