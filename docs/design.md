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
- Full BitTorrent-style swarming (piece trading, tit-for-tat, a tracker).
  Segments of one file do come from several nodes at once (see "Source
  selection"), because with partial selections no single peer may hold a
  whole file; but sources are chosen from each node's reported segments,
  not negotiated.
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
The selection file is re-read on every reconcile, and a missing one selects
nothing: a node no one has picked models for yet still serves `/status`.

### Partial selection

A node can also hold only part of a revision. `partial` entries in the
selection name a model, an optional revision (empty means every catalog
revision of that name), and the pieces to hold:

```yaml
wanted: ["small-model"]
partial:
  - model: glm-5.3
    revision: abc1234
    include: ["*.json", "dense/**"]   # whole files, by glob
    exclude: ["dense/vision-*"]
    ranges:                           # byte ranges of single files
      - path: experts/exlr8-layer-003.k4.safetensors
        ranges:
          - {offset: 0, length: 4096}
          - {offset: 1048576, length: 65536}
```

- Globs match manifest paths. `*` matches inside one path segment and `**`
  matches any number of segments.
- The files an entry names are those `include` matches and `exclude` does
  not, plus every file in `ranges`. With no `include` and no `ranges`, the
  entry names every file but its excludes. A file in `ranges` is never
  excluded.
- A model that `wanted` matches is held whole, whatever `partial` says.
- A byte range rounds out to whole segments, so a segment hash verifies
  every byte the node holds. Segments are 64 MiB unless the catalog
  gives a file smaller ones (see "Small segments").
- A file the node holds only in part is a sparse file at its full size.
  Every held segment is at its original offset, so a reader can use the
  absolute offsets from the file's own index. Bytes outside held segments
  are holes and read as zeros; only the ranges asked for are meaningful.
- A file is checked against its whole-file sha256, and marked complete,
  once every one of its segments is held.
- `-verify-on-start` re-hashes every held segment of each model once when
  the process starts, and fetches again any segment that no longer matches.

### Small segments

A range rounds out to whole segments, and 64 MiB is far more than gr8's
expert halves (0.2-0.8 MB). The half rule interleaves the ranks, so
every 64 MiB of a width file holds halves of every rank, and a rank's
halves would hold every width file in full. A catalog entry can give
some files smaller segments:

```yaml
models:
  - name: glm-5.3-exlr8
    source: {type: huggingface, repo: kindlingai/glm-5.3-exlr8-k2-k3-k4}
    revision: main
    segment_sizes:
      - {path: "experts/*.k?.safetensors", size: 65536}
```

- The first rule whose glob (as in `partial`) matches a file's path sets
  its segment size: a multiple of 4 KiB from 4 KiB to 64 MiB. Other
  files have 64 MiB segments.
- Only a root reads the rules, when it makes the manifest; every other
  node takes `segment_size` from the manifest. So every catalog in the
  fleet need not agree, but a root's rules must not change for a revision
  once its manifest is published: pin a new revision instead.
- A file of small segments has no `segment_sha256` in the manifest (the
  release's 821 GB of width files in 64 KiB segments would be 12 million
  of them). Its segments are verified as pull-through ones are: a root
  records the sha256 of each it fetched over TLS from the origin, and a
  peer checks each it copies against the sha256 its source sends with it.
  The whole file is checked against the origin's sha256 once every
  segment is held, as before. A root that downloads such a file whole
  checks it whole, and vouches for a segment by hashing it when asked.
- With 64 KiB segments a request for one rank's halves holds about 1.3x
  their bytes (a half is not aligned to segments, so each holds part of
  a segment at each end).

What is per segment and what is not, so a state file stays small at that
scale:

- In memory a node keeps one byte per segment for held, and one for trust
  once any segment of the file is not `manifest`: 25 MB for 12 million
  segments.
- On disk the state lists held segments as runs (`held`, see "Store
  contract for consumers") and trust as runs (`trust_runs`). One rank's
  halves of the whole release are about 30 MB of state, which loads in
  under a second.
- The sha256 of each small segment is in a side file,
  `<store>/hashes/<model>/<revision>.d/<path>.sha256`: 32 bytes per
  segment at its index, written and synced before the segment is marked
  held. It is model-loader's own, not part of the store contract.
- Use times, eviction, disk reservations and in-use marks are per chunk
  of 64 MiB, not per segment. Eviction removes the held segments of a
  chunk that nothing pins together; a pinned segment in it stays.
- Consecutive segments of one chunk, from the same sources, are fetched
  as one range of at most 256 segments. A source vouches for each with
  one sha256 per segment (see `X-Segment-SHA256` in
  `docs/protocol.md`). A source that vouches for none (an older node)
  is asked for the run again segment by segment.
- A file of small segments saves its state at most once a second while
  its segments land, and once at the end. The state on disk holds fewer
  segments than the node does in between, never more.
- A blob request reads what is held from the state the node keeps in
  memory, not the state file.

### Pull-through

A root downloads a model its selection wants whole from the origin first,
and publishes the manifest only when it is done, as before. For any other
catalog model it publishes a manifest at once, from the origin's file list,
the first time a peer (or its own partial selection) asks for it. That
manifest has no segment hashes. When a peer asks the root for a range it
does not hold, the root fetches the segments that range touches from the
origin, stores them, and serves them.

- Two requests for the same segment at once cause one fetch from the
  origin. The fetch finishes even if the request that started it goes away.
- At most `-pull-workers` segments (default 4) are fetched from origins at
  once. The source HTTP client keeps connections alive, so range GETs reuse
  them instead of each opening a connection (kindlingai/gr8#147).
- A root that holds a file (a NAS with the full release) serves it with no
  WAN traffic. A root takes a segment it does not hold from a LAN node that
  holds it before it goes to the origin.

Each held segment records how it was verified (its trust mode):

- `manifest`: its sha256 matched the manifest. Always so when the manifest
  has segment hashes for the file.
- `origin`: a root fetched it from the origin over TLS, for a file with no
  segment hashes, and recorded its sha256. A peer that copies it from the
  root checks it against that sha256 (`X-Segment-SHA256`) and keeps the
  root's trust mode.
- `requester`: as `origin`, and a consumer's XXH64 (seed 0) for every
  range inside it matched. gr8 has one for every expert half in its index
  slices. Give it as `xxh64` on a selection range; a peer passes it on to
  the root in `X-Range-XXH64`, and the root checks it before it serves the
  range. A range that does not match is dropped and fetched once more, from
  the origin only (the first copy may have come from a LAN node with a bad
  copy); failing again, the request fails.

Once every segment of a file is held it is checked against the origin's
sha256. If it fails, every segment not verified against the manifest is
dropped and fetched again. If segments the manifest verified are left,
the manifest disagrees with itself: nothing of the file is served, and the
model is in error until the manifest changes. If it matches, every segment becomes
`manifest`: the bytes are the origin's, and a consumer checksum that does
not match them is not applied. A file with no origin digest and no segment
hashes is complete once every segment is held, as nothing more is known to
check it against.

A consumer checksum given after its range was fetched is checked against
the held bytes on the next pass, once; one that fails drops what it covers.

### Requests

The selection file is static. A local consumer says what it needs at run
time with a named request (`PUT /requests/{name}`, see `docs/protocol.md`):
gr8 works out a rank's pieces with `g6load fetch-plan`, puts them as a
request at boot, puts a new one when the rank or the TP width changes, and
waits until it is ready. Requests and the selection file both apply on one
node: every reconcile adds the pieces of every request to the selection's.

- A replaced request fetches only what the node does not hold yet. A
  change from TP=4 to TP=3 keeps the pieces both widths need.
- A request pins its pieces: while it exists they are wanted, and eviction
  removes only pieces nothing wants.
- Requests are saved under the store, so they hold across restarts.
- A request whose ranges do not fit the manifest is `invalid`; it does not
  stop the node fetching the rest of the model. (The same holds for a bad
  partial entry in the selection file.)
- Putting or releasing a request starts a reconcile at once, not at the
  next tick.
- A file that fails (a consumer checksum that does not match, say) does not
  stop the other files of the model; the model reports the error.
- A request's checksums apply even when the selection file wants the whole
  model.

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
<store>/state/<model>/<revision>.json   # per-file progress: what is held
<store>/hashes/<model>/<revision>.d/    # sha256 of small segments, per file
<store>/requests/<name>.json            # saved local requests
```

### Store contract for consumers

A consumer such as gr8 reads weights in place from the store, with no
model-loader code and no network: only the files below. What this section
names is stable; any other file or field is model-loader's own and may
change.

Paths:

- `<model>` is the catalog entry's `name`, and `<revision>` its `revision`
  as the catalog writes it: a branch, tag or commit. It may contain a
  slash (`refs/pr/1`).
- `<store>/models/<model>/<revision>/<path>` is the file at manifest path
  `<path>`.
- `<store>/manifests/<model>/<revision>.json` is the manifest (as `GET
  /manifest`, see `docs/protocol.md`). Its `files` list every file of the
  revision, held or not, with `path`, `size` and `segment_size`.
- `<store>/state/<model>/<revision>.json` says what is held. Its `files`
  have, per file `path`: `complete`, `held` and `whole_failed`. `held`
  lists the held segments as half-open index ranges, sorted:
  `[[0, 4], [9, 10]]` holds segments 0-3 and 9. A manifest file the
  state does not list is not held. A file of 64 MiB segments also has
  `segments_done`, one bool per segment, as before this field existed;
  it says the same as `held`. A file of smaller segments does not have
  it: read `held`.

Manifest and state are replaced by a rename once written, so a reader
never sees half of one.

Revision to commit: the manifest's `commit` is the commit the origin
resolved the revision to when the manifest was made, and every file is of
that commit (a root fetches by the commit, not by the branch). A consumer
reads it from the manifest. It is resolved once: a branch that moves later
does not change the store's copy; pin a new revision in the catalog to
follow it. Peers copy the root's manifest, `commit` included. Two roots
that each make the manifest of a branch may resolve it to different
commits, so pin commits in the catalog of a fleet with several roots.
`commit` is empty for an origin with no commits (plain HTTP), and in a
manifest an older model-loader made.

Complete:

- A file is complete when its state has `complete: true` and not
  `whole_failed`: it is held whole and matched its whole-file sha256 (or
  every segment hash, with no sha256 to check).
- A revision is complete when every file of its manifest is: `/status`
  then reports it `ready`.
- A range `[offset, offset+length)` of a file is verified on disk when the
  file is complete, or when it is not `whole_failed`, every segment from
  `offset / segment_size` to `(offset+length-1) / segment_size` is in
  `held`, and not every segment of the file is (a file with every segment
  held is not used until its whole-file check passes). A
  partial request (or `partial` entry) is ready when every one of its
  ranges and whole files is; `GET /requests/{name}` reports the same, and
  also checks the request's `xxh64`s, which a consumer reading offline
  checks itself.

Bytes a segment the state marks held covers are on disk: a segment is
marked held only once written and verified, and an evicted one is marked
not held before its bytes are freed. If saving the state fails, the old
state file is removed, as it may mark held a segment since found bad;
until a save succeeds the revision reads as holding nothing. Eviction removes only pieces nothing
on the node wants, so a consumer keeps what it reads pinned with a request
or a selection entry while model-loader runs.

Sparse files: a file held only in part is a sparse file at its full size,
every held segment at its original offset, so a reader uses the absolute
offsets from the file's own index (a safetensors header, gr8's index
slices). Bytes outside held segments read as zeros.

Permissions: model-loader is the only writer. Under `models/`,
`manifests/` and `state/` it creates every file 0644 and every directory
0755 (it sets its umask to 022), owned by the user it runs as, so a
consumer reads them as any user, from a read-only mount. The local API
socket, `model-loader.sock`, is 0660 in model-loader's group (see
"Deployment targets").

### Disk budget and eviction

GLM-5.3 and GLM-5.3-Flash together are close to the SSD size of a box.
Spread over the boxes, with each deleting what it does not need, both fit.
`-disk-budget` (bytes such as `3T`, or a share of the store's filesystem
such as `85%`) sets the most the store may hold. Without one nothing is
evicted or removed.

The store's use is the disk space its model files take: the blocks the
filesystem allocated to them. So it counts what is really on disk, bytes a
failed transfer wrote included, and holds across restarts.

Before a fetch, the node reserves room for every segment it still has to
fetch, counted in the filesystem blocks it touches. Fetches of the same
segment share one reservation, and one that joins a fetch already let in
is not checked again. A root's download from its origin reserves what of
the revision is not on disk yet before it writes; a file whose progress
does not hold is started over, and its old bytes freed, first.
If the store's use plus every reservation would go over the budget, it
frees space until it fits: first bytes no held segment covers, in files
nothing is writing or serving (what a failed transfer left; a segment
counts as not held only if the saved state agrees), then held segments,
least recently used first (a segment is used when it is fetched or served
to a peer; a file of small segments counts use per 64 MiB chunk). Freed space is measured again, not assumed. If the fetch cannot
fit after freeing everything it may, the model is in error with the
reason, and is tried again at the next tick. A root pulling through for a
peer makes room the same way, and the segments the pull needs (its range
and what its checksums span) cannot be evicted to make it. At every tick
the node also frees down to the budget (it may have been lowered) and
counts what it could evict, for `/status`.

A segment may be evicted only when all of these hold:

- Nothing on this node wants it: no request and no selection entry names
  it. Those pieces are pinned. The pins are read again just before
  eviction acts, and a request change waits for that, so a request
  acknowledged before eviction is never overtaken by it. If the wants
  cannot be worked out, nothing is evicted.
- Nothing is serving it to a peer, nor writing its file, at that moment.
- The fleet keeps a copy. A node that can fetch the model from its origin
  (a root) may always evict, since the origin has it. A peer may evict only
  a segment that a root whose catalog names the revision holds (`cataloged`
  in `/segments`), or that a node with a lower ID holds.
  So of the peers holding a segment no root holds, the one with the lowest
  ID never evicts it, and two peers evicting at once cannot both drop the
  last copy. Each peer's `/segments` says what it holds; a peer that does
  not answer is taken to hold nothing.

An evicted segment's blocks go back to the filesystem (a hole punched in
the file, on Linux and macOS; elsewhere the space stays charged); a file
with nothing left held is removed. Eviction forgets only the consumer
checksums over the evicted segment, so a request for another range of the
file stays ready.

A revision the catalog no longer names, and that no saved request or
selection entry on this node names, is retired. At every tick, with a
budget, its segments are evicted by the same fleet rule, except that no
node can fetch it again, roots included: a node drops a retired segment
only if a node with a lower ID holds it. Once nothing of it is held its
files, state and manifest are removed (only its own files: a revision
named `refs/pr/1` lives under the directory of one named `refs`). So the
lowest-ID holder keeps one copy of a retired revision, until removed by
hand: another node's catalog may still name it, and as discovery can miss
nodes no node can tell that every catalog in the fleet dropped it. Without
a budget nothing is evicted or removed.

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

Before it fetches a model's files, a node asks every known peer which
segments it holds (`GET /segments/{model}/{revision}`), at most 8 at once
across all models. For each segment it
needs, it then tries, in order:

1. the plain peers that hold the segment (and plain peers too old for
   `/segments` whose `/status` reports the revision ready);
2. the roots that hold it;
3. peers that did not answer the question (an older root, or a timeout);
4. the other peers, in case they gained the segment since they were asked;
5. (root mode only) the file's WAN source.

Within the first two groups each segment starts at a different node, so the
segments of one file come from several nodes at once. Every segment
transfer holds one of the node's `-max-concurrent-transfers` slots, so the
limit applies to segments across all files and models. A source that fails
outright (a peer that went away mid-copy, a bad hash) is dropped for the
rest of the file and the segment moves on to its next source; a source that
does not hold a range answers 416 and is skipped for that segment only.

This makes every node that holds a segment an additional source for
everyone else. Root bandwidth stays flat as the fleet grows, and a model
spread across the peers with no single copy of any file can still be built
on a new node without the root.

## Discovery

Pluggable per deployment target, because each one already has a natural
answer:

- **static** — a fixed list of peer addresses (env var or config). Works
  everywhere, zero dependencies. Default for docker compose.
- **k8s** — a headless Service's DNS answers list every pod. Default for
  Kubernetes.
- **mentat** — on kindling-spark-os, read the local `mentatd`'s peer table
  (`GET /status` on its HTTP side port, `-mentat-url`, default
  `http://127.0.0.1:6380`) instead of reinventing discovery; that daemon
  already solves LAN peer discovery there. Every box mentatd holds alive is
  a peer, at its port on the first of its addresses that takes a
  connection there: its `link_ip` (the address mentatd's working link to
  it uses), its `node_ip`, then its `addrs`. The choice is kept for 30 s.
  Each box publishes a record through its own mentatd every 5 s
  (`PUT /records/model-loader`, kindlingai/mentat#4): its port, its roles,
  and its holdings, the revisions it holds `ready` or `partial`, sorted, so
  an unchanged record costs mentatd nothing. Other boxes read it on that
  box's row of their own mentatd's `/status`, and gr8 can too. A box with
  no record, such as one whose mentatd predates records, is taken to be at
  `-mentat-port`. A record mentatd has no room for (`413`) goes again with
  half its holdings, marked `truncated`, until it fits. The record is
  deleted on shutdown. `/segments` still says which segments a box holds;
  the holdings are a summary. `-peers` adds boxes that run
  no mentatd, such as a NAS root. Callers that overlap share one `/status`
  fetch, and its answer serves for a second after it arrives, so a box
  mentatd finds dead drops out within a second plus a fetch's time (at
  most 5 s).

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
- Discovery: static list, k8s DNS, and mentat. LAN broadcast is a
  fast-follow.
- No cross-revision dedup.

## Deployment targets

One repo, one binary, three deploy trees under `deploy/`:

- `deploy/k8s/` — Kustomize: a root Deployment and a peer DaemonSet, a
  headless Service for discovery, a ConfigMap for the catalog.
  `examples/gr8` adds gr8 on one node, with a NAS outside the cluster as
  the root. `components/mentat` switches the peers to mentat discovery on
  the host network, for nodes that run mentatd; `examples/gr8-mentat` is
  gr8 with it.
- `deploy/compose/` — a `docker-compose.yml` with role and peer list set by
  environment variables, `gr8.yml` to add gr8 on the same box, and env
  files for a NAS root and for a Spark peer.
- `deploy/spark-os/` — a systemd unit and `/etc/spark/model-loader.env`
  template, following kindling-spark-os's existing convention (binary at
  `/usr/local/bin/model-loader`, unit conditional on its env file
  existing), with mentat discovery, and `gr8.override.yml` for gr8's own
  compose file.

Each target gives gr8 the same two things:

- The store, mounted read-only in gr8's container. Only model-loader
  writes it. Its files are 0644 and its directories 0755.
- The local request API: the unix socket `model-loader.sock` in the store,
  so the same read-only mount carries it (a read-only mount does not stop
  a process opening a socket). The socket is mode 0660, and model-loader
  runs in group 1000, gr8's, so gr8 can open it and other users cannot.
  The deploys keep it off TCP, where it needs `MODEL_LOADER_LOCAL_TOKEN`
  (docs/protocol.md, "Local request API").

gr8 starts once `model-loader -check unix:PATH` passes, that is once the
socket answers a request. A TCP check of `-listen` would not show that: the
local API opens after it, and a socket left by a dead process is still a
file. Compose waits on a health check, k8s on an init container, and
spark-os's compose override on a one-shot container, as compose cannot wait
on a systemd unit.

## Relationship to home-ops

home-ops's existing `k8s/models/{catalog.yaml,sync.py,mirror.py}` is not
touched by this project. Its store layout is intentionally not replicated
here (see the architecture discussion that preceded this design); adopting
model-loader there later means a one-time re-materialization into the new
layout, which is pure cache and carries no data-loss risk, not a format
migration.
