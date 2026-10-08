# Design

## Goals

- Single download from WAN per model, fanned out to every box on the LAN.
- Minimal RAM footprint on every node; near-zero idle RSS on peers that are
  only serving files they already hold.
- Deployable the same way on Kubernetes, docker compose, and
  [kindling-spark-os](https://github.com/kindlingai/kindling-spark-os)
  (plain systemd).
- Two roles, root and peer, and a single box can run both at once.

## Non-goals (for now)

- Cross-revision block-level dedup.
- Full BitTorrent-style multi-source parallel chunk swarming. One source per
  file at a time is enough at home/small-fleet scale; revisit only if a real
  deployment shows root or peer bandwidth is actually the bottleneck.
- A consensus protocol for which root fetches a new catalog entry. Two roots
  racing to WAN-fetch the same new entry is handled with a soft check-LAN
  before pulling, not solved.

## Roles

A node's role is a run-time flag, not a different binary: `--mode=root`,
`--mode=peer`, or `--mode=both`. "Both" means: behave as a peer (serve what
you have, pull what you don't from the LAN first) and additionally be
eligible to WAN-fetch catalog entries nobody on the LAN has yet. This is what
lets a single box be a complete, self-contained deployment.

## Catalog

`catalog.yaml` is the fleet-wide desired-state file: every model the fleet
might want, pinned to a source and revision. It is supplied to each node by
whatever deploys it (a k8s ConfigMap, a bind-mounted file in compose, a file
dropped by kindling-spark-os's site layer) — model-loader does not sync the
catalog itself, only the model data the catalog describes.

Each node additionally has a small local selection of which catalog entries
it actually wants to materialize (`wanted: ["*"]` or an explicit list), so a
node with a smaller disk doesn't have to hold the whole fleet's catalog.

## Store

The materialized file tree is the canonical store — there is no separate
permanent chunk-blob layer sitting alongside it. For each `(model, revision)`
a manifest records every file's size, whole-file sha256, and (optionally)
per-64MiB-segment sha256 so a partially downloaded file can be verified and
safely re-served in segments before it's complete.

Transfer and resume happen at HTTP Range granularity against the real file.
This is simpler than a content-addressed blob store and avoids doubling disk
usage, while still giving resumability, integrity checking, and the ability
to serve a file to a peer before it's 100% downloaded.

On-disk layout:

```
<store>/manifests/<model>/<revision>.json
<store>/models/<model>/<revision>/<relative-path>
<store>/state/<model>/<revision>.json   # per-file download progress, not for consumers
```

## Network protocol

Every node, root or peer, runs the same small HTTP server (see
[protocol.md](protocol.md)):

- `GET /status` — this node's per-model state and progress. Replaces a
  shared-filesystem status file (home-ops's `catalog-status.json`) with
  something that works without NFS, so compose and systemd deployments get
  the same "wait until ready" contract k8s consumers already use.
- `GET /manifest/{model}/{revision}` — the manifest, once this node has it.
- `GET /blob/{model}/{revision}/{path}` — Range-aware file serving, limited
  to byte ranges the node has already verified.

## Source selection

For each file a node needs, it asks its known peers' `/status` and picks, in
order: a LAN peer that reports the file ready, then a configured root, then
(root mode only) the file's WAN source. This makes every node that finishes a
download an additional source for everyone else — root bandwidth stays flat
as the fleet grows, without needing a tracker or piece-level swarm.

## Discovery

Pluggable per deployment target, because each one already has a natural
answer:

- **static** — a fixed list of peer addresses (env var or config). Works
  everywhere, zero dependencies. Default for docker compose.
- **k8s** — a headless Service's DNS answers list every pod. Default for
  Kubernetes.
- **mentat** (planned) — on kindling-spark-os, query the local `mentatd`'s
  peer table instead of reinventing discovery; that daemon already solves
  LAN peer discovery there.

## RAM minimization

- Go's stdlib `net/http`, goroutine-per-connection, small in-memory state
  (a model/revision status map) — idle RSS in the low single-digit megabytes,
  no process-recycling tricks needed the way a long-lived Python interpreter
  holding onto a WAN client library needed them.
- Serving a file uses `io.Copy` from an `*os.File`, which Go's `net/http`
  turns into `sendfile(2)` for a plain TCP connection — no per-request
  buffering of file contents in the heap.
- Downloads still apply the technique home-ops proved necessary in
  production: periodic `fsync` + `fadvise(..., POSIX_FADV_DONTNEED)` on the
  destination file, so a multi-hundred-GB transfer doesn't balloon page-cache
  resident size. This matters even though page cache is reclaimable, because
  on unified-memory boxes (DGX Spark) it still competes with GPU-visible RAM.
- At most 1–2 concurrent transfers per node by default — home-ops hit real
  OOMs at 4 concurrent HF downloads.

## V1 scope

- Sources: plain HTTP(S) and Hugging Face Hub resolve. A root reads
  `HF_TOKEN` from its environment for gated or private Hub repos. S3/OCI
  later.
- Discovery: static list and k8s DNS. mentat and LAN broadcast are
  fast-follows once the core loop is proven.
- No cross-revision dedup, no multi-source parallel fetch of one file.

## Deployment targets

One repo, one binary, three deploy trees under `deploy/`:

- `deploy/k8s/` — Kustomize: a root DaemonSet/Deployment and a peer
  DaemonSet, a headless Service for discovery, a ConfigMap for the catalog.
- `deploy/compose/` — a `docker-compose.yml` with role and peer list set by
  environment variables.
- `deploy/spark-os/` — a systemd unit and `/etc/spark/model-loader.env`
  template, following kindling-spark-os's existing convention (binary under
  `/opt/kindling/model-loader/`, unit conditional on its env file existing).

## Relationship to home-ops

home-ops's existing `k8s/models/{catalog.yaml,sync.py,mirror.py}` is not
touched by this project. Its store layout is intentionally not replicated
here (see the architecture discussion that preceded this design); adopting
model-loader there later means a one-time re-materialization into the new
layout, which is pure cache and carries no data-loss risk, not a format
migration.
