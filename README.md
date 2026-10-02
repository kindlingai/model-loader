# model-loader

Fetches large model files from the WAN once and fans them out to every box on
the LAN, with a near-zero idle footprint on boxes that are just serving what
they already have.

Every node runs the same binary in one of two roles:

- **root** — has WAN egress; downloads catalog entries from their origin
  (Hugging Face, plain HTTP, ...) into the local store.
- **peer** — no WAN required; fetches missing files from whichever LAN node
  (root or another peer) already reports them ready.

A node can be both at once — "root" is just a peer with WAN fetch turned on,
which is what makes a single box a complete, self-contained setup.

Once a node holds a file, it serves it to the rest of the LAN too, so the
root's bandwidth doesn't scale with fleet size: peers reseed each other.

See [docs/design.md](docs/design.md) for the full design and
[docs/protocol.md](docs/protocol.md) for the wire format.

## Status

Core packages, CLI, and all three deploy targets (k8s, docker compose,
spark-os) are in place and tested. Not yet run against a real model catalog
end-to-end.

## Layout

```
cmd/model-loader/   CLI entrypoint
internal/catalog/   desired-state parsing (catalog.yaml)
internal/store/     local content store, manifests, integrity
internal/source/    WAN source plugins (http, huggingface)
internal/transfer/  resumable range-fetch engine
internal/discovery/ peer discovery backends (static, k8s DNS)
internal/server/    LAN-facing HTTP server (status/manifest/blob)
internal/daemon/    reconcile loop tying it together
deploy/k8s/         Kustomize manifests
deploy/compose/     docker-compose.yml
deploy/spark-os/    systemd unit + env template for kindling-spark-os
```
