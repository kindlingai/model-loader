# docker compose deployment

Single-box by default: one container runs `-mode=both`, acting as its own
root and peer. For a multi-box LAN, run this compose file on every box and
set:

- `MODEL_LOADER_MODE=root` on the box that should WAN-fetch catalog entries
  (or `both` if that box should also serve a local workload), `peer`
  everywhere else.
- `MODEL_LOADER_PEERS` to a comma-separated `host:port` list of the other
  boxes (static discovery -- compose has no service-discovery DNS of its
  own).

```sh
cp catalog.yaml catalog.yaml.local   # edit in your models
MODEL_LOADER_MODE=both docker compose up -d
curl http://localhost:7762/status
```
