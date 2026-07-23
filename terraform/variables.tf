# --- Authentication ----------------------------------------------------------

variable "os_cloud" {
  description = "Cloud entry in clouds.yaml to authenticate with. Set to null to use OS_* environment variables instead."
  type        = string
  default     = "openstack"
}

variable "image_id" {
  description = "Glance image ID to boot. Default is Ubuntu Server 24.04 on the DHBW cloud."
  type        = string
  default     = "7842eb53-0ac7-4677-9160-2466371b4302"
}

variable "flavor_name" {
  description = "Instance flavor. k8s.node gives 4 vCPU / 8 GB / 50 GB — the 50 GB root disk is needed to build images and run Kafka/Flink/SeaweedFS (the Cinder volume quota is 0, so a larger flavor is the only way to get more disk)."
  type        = string
  default     = "k8s.node"
}

variable "network_name" {
  description = "Name of the tenant network to attach instances to (canonical name is DHBWV6)."
  type        = string
  default     = "DHBWV6"
}

variable "vm_prefix" {
  description = "Prefix for all VM and resource names, e.g. \"iot\" -> iot-server."
  type        = string
  default     = "iot"
}

variable "agent_count" {
  description = "Number of k3s agent (worker) nodes. One server node is always created."
  type        = number
  default     = 2
}

variable "ssh_user" {
  description = "Default login user of the image (Ubuntu cloud images use \"ubuntu\")."
  type        = string
  default     = "ubuntu"
}

variable "key_pair_name" {
  description = "Name of the OpenStack keypair injected into the VMs. Created from public_key_path when that is set, otherwise it must already exist in the project."
  type        = string
  default     = "iot-key"
}

variable "public_key_path" {
  description = "Path to an SSH public key. If non-empty, a keypair named key_pair_name is created from it. Leave empty to reuse an existing keypair uploaded via the dashboard."
  type        = string
  default     = ""
}

variable "public_tcp_ports" {
  description = "TCP ports opened to the world on every node: SSH, k3s API, web NodePort."
  type        = list(number)
  default     = [22, 6443, 30080]
}

# --- Automated k3s + application bootstrap (cloud-init) -----------------------

variable "deploy_stack" {
  description = "If true, the server node also builds images, installs the Helm release and submits the Flink job — a single apply brings up the whole stack. Set false to only provision the k3s cluster."
  type        = bool
  default     = true
}

variable "repo_url" {
  description = "Public Git URL cloned on every node to build the images. Push your latest changes before applying."
  type        = string
  default     = "https://github.com/Tomeista/Cloud-IoT"
}

variable "repo_branch" {
  description = "Branch to check out when building images."
  type        = string
  default     = "main"
}

variable "image_registry" {
  description = "Image name prefix. Kept local (imported into containerd), so no external registry is used. Must match Helm's imageRegistry."
  type        = string
  default     = "iot-monitoring"
}

variable "k8s_namespace" {
  description = "Namespace the Helm release is installed into."
  type        = string
  default     = "iot-monitoring"
}

# --- DNS (optional, RFC2136 + TSIG) ------------------------------------------

variable "enable_dns" {
  description = "Publish an AAAA record for the server into the DHBW dynamic-DNS zone."
  type        = bool
  default     = false
}

variable "dns_server" {
  description = "RFC2136 (dynamic DNS) server address."
  type        = string
  default     = "141.72.5.244"
}

variable "dns_port" {
  description = "RFC2136 server port."
  type        = number
  default     = 53
}

variable "dns_zone" {
  description = "DNS zone to write into, e.g. sXXXXXX-at-student-dhbw-mannheim-de.users.dhbw.site (no trailing dot)."
  type        = string
  default     = ""
}

variable "dns_name" {
  description = "Record name within the zone. \"@\" writes the zone apex itself."
  type        = string
  default     = "@"
}

variable "dns_key_name" {
  description = "TSIG key name (the USER_KEY from your .env, e.g. user-key-...)."
  type        = string
  default     = ""
}

variable "dns_key_secret" {
  description = "TSIG key secret (the SECRET_KEY from your .env). Keep it out of Git."
  type        = string
  default     = ""
  sensitive   = true
}

variable "dns_key_algorithm" {
  description = "TSIG algorithm."
  type        = string
  default     = "hmac-sha512"
}

variable "dns_ttl" {
  description = "TTL (seconds) for the DNS record."
  type        = number
  default     = 300
}
