# Wire protocol

Plain HTTP, no TLS, on a private LAN by default — the same trust model
kindling-spark-os's `mentatd` uses (a shared secret over broadcast, not
encryption). Default port `7762`.

## `GET /status`

Returns this node's view of every catalog entry it is tracking.

```json
{
  "node_id": "spark-1",
  "roles": ["peer"],
  "models": [
    {
      "model": "nvidia/nemotron-3-nano-30b-a3b",
      "revision": "abc1234",
      "state": "downloading",
      "bytes_total": 20834697216,
      "bytes_done": 1048576,
      "source": "peer:spark-0",
      "updated_at": "2026-10-02T12:00:00Z"
    }
  ]
}
```

`state` is one of `pending`, `downloading`, `ready`, `error`. A consumer
waiting for a model to be usable polls this endpoint (or the equivalent
init-container pattern) for `state == "ready"` on the revision it needs,
the same contract home-ops's `wait-model` init containers use today against
`catalog-status.json` — just reachable over HTTP instead of a shared mount.

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

## `GET /blob/{model}/{revision}/{path}`

Serves the file at `path`, honoring `Range` requests (`Accept-Ranges: bytes`).
Only byte ranges whose covering segments have been verified against the
manifest's `segment_sha256` are served; a request for an unverified range
gets `416 Range Not Satisfiable` rather than partial/unverified data.

## Errors

Standard HTTP status codes. `503` means "ask someone else" (overloaded or
mid-verification), not "this file doesn't exist here."
