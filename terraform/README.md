# Terraform — IoT k3s cluster on the DHBW OpenStack cloud

Provisions the Ubuntu VMs for the IoT monitoring stack: one k3s **server**
(control plane) and `agent_count` **agents** (workers), plus a security group
opening the ports the stack needs.

With `deploy_stack = true` (the default) **a single `terraform apply` brings up
everything**: cloud-init installs k3s (shared token, no ordering race), every
node clones the repo and builds the container images locally, imports them into
containerd, and the server installs the Helm release and submits the Flink job.
No external registry is involved — each node builds its own images, which
sidesteps cross-node image distribution on the IPv6 network.

Set `deploy_stack = false` to only provision the bare k3s cluster and deploy
manually via [../docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md).

> The `example/` folder is the original course template kept for reference —
> this directory is the project's own configuration.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.3
- OpenStack credentials via **clouds.yaml** (download it from the dashboard under
  *API Access*). Place it in this directory (gitignored), in
  `~/.config/openstack/`, or point `OS_CLIENT_CONFIG_FILE` at it. `os_cloud` in
  `terraform.tfvars` must match the entry name inside the file (default
  `openstack`). The secret stays in that file, never in the Terraform code.

  > Alternative: set `os_cloud = null` and export the standard `OS_*`
  > environment variables instead — the provider reads them natively.

## Usage

> **Before applying with `deploy_stack = true`:** push your latest changes to
> `repo_url`/`repo_branch` and make sure the repo is **public** — every node
> clones it to build images. A private repo makes the bootstrap fail.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # then edit
terraform init
terraform plan
terraform apply
```

After `apply`:

```bash
terraform output            # server_ip, agent_ips, web_ui, ssh_server, ...
```

The bootstrap keeps running on the VMs after `apply` returns (images build,
Helm installs, Flink job submits — a few minutes). Watch it:

```bash
ssh ubuntu@<server_ip> 'tail -f /var/log/iot-bootstrap.log'
# finished when /var/log/iot-bootstrap.done exists
kubectl -n iot-monitoring get pods   # on the server (KUBECONFIG=/etc/rancher/k3s/k3s.yaml)
```

Then open the `web_ui` URL from `terraform output`.

## What gets created

| Resource                    | Purpose                                            |
| --------------------------- | -------------------------------------------------- |
| `openstack_compute_keypair` | SSH key (only if `public_key_path` is set)         |
| `openstack_networking_secgroup` + rules | SSH (22), k3s API (6443), web NodePort (30080), ICMP, full intra-cluster |
| `openstack_compute_instance` × (1 + `agent_count`) | Ubuntu 24.04 k3s nodes         |
| `local_file`                | `generated-inventory.yml`                          |

## Notes

- **IPv6**: the `DHBWv6` network is IPv6-first (IPv4 is NAT-only). Outputs and
  the inventory pick the IPv6 address automatically when available.
- **Ports**: to expose more NodePorts, add them to `public_tcp_ports` in
  `terraform.tfvars`.
- **Teardown**: `terraform destroy` removes everything created here.
- If you rotate the application credential, just replace `clouds.yaml` — no
  Terraform change needed.
```
