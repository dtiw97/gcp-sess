# gcp-sess course

A tracer bullet for taking tbm-sim to GCP: Go service → container → Kubernetes → Agones → GCP.
Each commit is one small step; `git log -p` is the course log.

Status: ✅ done · 🔄 in progress · ⬜ not started

| # | Module | Status | Notes |
|---|---|---|---|
| 1 | Health, shutdown, config, JSON logs | 🔄 | [01-health-shutdown.md](01-health-shutdown.md) |
| 2 | WebSocket room + tick (mini tbm-sim) | ⬜ | |
| 3 | Container image | ⬜ | |
| 4 | Registry and CI (GitHub Actions build + push) | ⬜ | |
| 5 | Local Kubernetes with kind: Deployment, Service, probes | ⬜ | |
| 6 | Config and resources: ConfigMap, Secret, requests/limits | ⬜ | |
| 7 | Helm | ⬜ | |
| 8 | GCP by hand, then Terraform | ⬜ | |
| 9 | ArgoCD | ⬜ | |
| 10 | GCP edge: load balancer, ingress, TLS | ⬜ | |
| 11 | Session per pod, the hard way | ⬜ | |
| 12 | Agones: GameServer, Fleet, Allocation | ⬜ | |
| 13 | Allocator / lobby: how a client finds its pod | ⬜ | |
| 14 | Failure drills: kill pods, drain nodes, roll out mid-session | ⬜ | |

## Why this order

- Session per pod (11) comes before Agones (12): feel the pain of a plain Deployment first, then see what Agones fixes.
- The WebSocket room (2) comes early because there's no session to test without it.
- Agones alone doesn't route clients to a pod, hence the allocator (13).

## Context

- tbm-sim today is an offline Docker Compose appliance. It runs 1 room and up to 9 clients per server, which maps onto Agones' one-session-per-GameServer model.
- GTech's shared GKE clusters are private (no node public IPs), and its Helm library chart templates Deployments, not GameServers. Practise in a personal sandbox project first.
