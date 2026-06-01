---
name: feedback-orbstack-ports
description: OrbStack routes outgoing TCP from containers with published ports through its own NAT proxy (172.18.0.x), breaking database connections. Fix by removing port mappings and using OrbStack DNS.
metadata:
  type: feedback
---

On OrbStack (Mac), containers with published ports (`host:container`) have their outgoing TCP connections routed through OrbStack's internal NAT proxy at `172.18.0.x` instead of going directly to other containers. This causes them to hit a shadow/proxy PostgreSQL instance with no data.

**Why:** OrbStack applies different iptables/eBPF routing for containers with published ports vs those without. Containers without port mappings use direct Docker compose network routing.

**How to apply:** Never publish ports for internal services (timescaledb, redis, etc.) that only need container-to-container access. Use OrbStack's `.orb.local` DNS for Mac access instead of port publishing. Access Grafana at `grafana.orb.local:3000` instead of `localhost:3000`.
