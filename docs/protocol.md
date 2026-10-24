# Wire protocol

Plain HTTP, no TLS, on a private LAN by default — the same trust model
kindling-spark-os's `mentatd` uses (a shared secret over broadcast, not
encryption). Default port `7762`.

## `GET /status`

Returns this node's view of every catalog entry it is tracking.

```json
{
  "node_id": "spark-01",
  "roles": ["peer"],
  "models": [
    {
      "model": "nvidia/nemotron-3-nano-30b-a3b",
      "revision": "abc1234",
      "state": "downloading",
      "bytes_total": 20834697216,
      "bytes_done": 1048576,
      "source": "peer:nas",
      "updated_at": "2026-10-02T12:00:00Z"
    }
  ]
}
```

`state` is one of `pending`, `downloading`, `partial`, `ready`, `error`. A
consumer waiting for a model to be usable polls this endpoint (or the
equivalent init-container pattern) for `state == "ready"` on the revision it
needs, the same contract home-ops's `wait-model` init containers use today
against `catalog-status.json` — just reachable over HTTP instead of a shared
mount.

A model the node's selection names only in part has `"selection":
"partial"`. Its `bytes_total` and `bytes_done` count the wanted segments,
not the whole revision. Once every wanted piece is held and verified its
`state` is `partial`, not `ready`: `ready` always means the whole revision.

## `GET /manifest/{model}/{revision}`

Returns the manifest for a model/revision this node already has (fully or
partially). 404 if this node has never started it — ask a different peer.

```json
{
  "model": "nvidia/nemotron-3-nano-30b-a3b",
  "revision": "abc1234",
  "files": [
    {
      "path": "model-00001-of-00004.safetensors",
      "size": 5368709120,
      "sha256": "...",
      "segment_sha256": ["...", "..."],
      "segment_size": 67108864
    }
  ]
}
```

A root publishes a manifest for pull-through the first time one is asked
for, for a catalog model it does not want whole. Its files have
`segment_size` but no `segment_sha256`. Each segment is then verified as
described under `/blob`.

## `GET /segments/{model}/{revision}`

Which segments of each file this node holds, verified, as half-open ranges
of segment indices. A file it holds nothing of is left out, and a revision
it has no manifest for answers with no files. A 404 means a node too old to
have this endpoint. A fetching node then reads its `/status`: a plain peer
that reports the revision `ready` holds every segment; otherwise what it
holds is unknown. An answer for another model or revision, or with a file
or range outside the manifest, is ignored, as is any answer over 16 MiB.

```json
{
  "model": "glm-5.3",
  "revision": "abc1234",
  "roles": ["peer"],
  "files": {
    "experts/exlr8-layer-003.k4.safetensors": {
      "segments": 12,
      "held": [[0, 4], [9, 10]]
    }
  }
}
```

Here the node holds segments 0-3 and 9 of a 12-segment file. `roles` are
the node's roles, as in `/status`. A fetching node asks every peer once per
reconcile pass and prefers plain peers that hold a segment to roots that
hold it.

## `GET /blob/{model}/{revision}/{path}`

Serves the file at `path`, honoring `Range` requests (`Accept-Ranges: bytes`).
Only byte ranges whose covering segments have been verified are served; a
request for an unverified range gets `416 Range Not Satisfiable` rather
than partial/unverified data.

On a root, a range it does not hold yet is fetched from the model's origin
first (pull-through; see `docs/design.md`). A failure there, such as an
origin error or a checksum mismatch, is a `502` with the reason in the
body.

Request header:

- `X-Range-XXH64: offset+length=xxh64,...`: a consumer's XXH64 (seed 0, 16
  hex digits) for ranges inside the requested one. A root checks each
  against the bytes before it serves them. A check that spans more than
  the requested range still applies: the root fetches every segment it
  spans first. At most 4096 entries, each at most 1 GiB.
- `X-No-Pull: 1`: serve only what this node holds; do not fetch from the
  origin. A root sends it when it fills its own pull from LAN nodes, so
  two roots never wait on each other's pulls.

Response headers, for a request of exactly one segment that this node
holds a sha256 for (the requester's manifest may have no `segment_sha256`
for it, as one from a pull-through root has none):

- `X-Segment-SHA256`: the sha256 this node holds for the segment. The
  requester checks the bytes against it.
- `X-Segment-Trust`: how this node verified it: `manifest`, `origin` or
  `requester`.

## Local request API

A local consumer (gr8) tells model-loader which pieces it needs with a
named request. The API is served apart from the LAN API, on
`-local-listen`: by default the unix socket `model-loader.sock` in the
store directory; `host:port` serves it over TCP instead, and an empty value
turns it off. It has no authentication: anything that can reach it can pin
and release pieces, so keep it on the socket or on localhost.

### `PUT /requests/{name}`

Create the request `name`, or replace it. `name` is 1 to 128 of
`[A-Za-z0-9._-]`, starting with a letter or digit. The body names pieces
of one catalog model revision, as an entry of the selection file's
`partial` list does, plus a priority:

```json
{
  "model": "glm-5.3",
  "revision": "abc1234",
  "include": ["config.json", "dense/*.safetensors"],
  "exclude": [],
  "ranges": [
    {"path": "experts/layer-03.safetensors",
     "ranges": [{"offset": 1048576, "length": 4194304, "xxh64": "9f2c0e5b7a1d3c44"}]}
  ],
  "priority": 10
}
```

- `revision` may be left out when the catalog has one revision of `model`.
- `xxh64` is optional: the consumer's XXH64 (seed 0) of exactly that range.
  The request is ready only once every one matches (see `docs/design.md`).
- `priority`: models with a higher-priority request start first and take
  transfer slots first. It does not stop a transfer already running.
  Default 0, as for the selection file.

Answers `200` with the request's status (below), or `400` for a body that
is not well formed or names no catalog model revision. Unknown fields are
an error.

Replacing a request keeps every piece it shares with the old one: only
the new pieces are fetched. Pieces only the old request named stay on disk
until eviction removes them (they are no longer pinned).

### `GET /requests/{name}`

The request's status:

```json
{
  "name": "gr8-rank0",
  "model": "glm-5.3",
  "revision": "abc1234",
  "include": ["config.json"],
  "ranges": [ ... ],
  "priority": 10,
  "created": "2026-10-24T15:00:00Z",
  "updated": "2026-10-24T15:05:00Z",
  "state": "downloading",
  "bytes_total": 21474836480,
  "bytes_done": 5368709120,
  "dir": "/var/lib/model-loader/models/glm-5.3/abc1234",
  "error": ""
}
```

- `state`: `pending` (no manifest yet), `downloading`, `ready` (every
  piece held and verified), or `invalid` (the manifest shows a file or
  range the request names does not exist; it will never be ready, and the
  rest of the model is fetched as usual).
- `bytes_total` and `bytes_done` count the request's pieces, rounded out to
  whole segments.
- `dir` holds the revision's files by their manifest paths. A piece is at
  its original offset in its file; bytes outside the pieces held read as
  zeros.
- `error`: the last failure fetching the model (it is retried), or why
  the request is invalid.

With `?wait=DURATION` (such as `?wait=5m`, at most 30m) it answers once the
request is ready (`200`) or invalid (`422`), or the wait is over (`504`,
with the status then). 404 if there is no such request.

### `GET /requests`

Every request's status, highest priority first.

### `DELETE /requests/{name}`

Release the request: `204`, or `404` if there was none. Its pieces are no
longer pinned.

Requests are saved in `requests/` in the store, one file each, and hold
across restarts of model-loader. A pinned piece is one any request or the
selection file names; eviction never removes one.

## Errors

Standard HTTP status codes. `503` means "ask someone else" (overloaded or
mid-verification), not "this file doesn't exist here."
