# docker compose deployment

`docker-compose.yml` runs model-loader on one box. Unset, it runs
`-mode=both`: its own root and peer, holding every model in `catalog.yaml`.

```sh
docker compose up -d
docker compose exec model-loader model-loader -check unix:/var/lib/model-loader/model-loader.sock
curl http://localhost:7762/status
```

It uses the host's network, so other boxes reach it at the box's LAN
address, and its node ID is the box's hostname.

## With gr8

`gr8.yml` adds gr8 on the same box:

```sh
docker compose -f docker-compose.yml -f gr8.yml up -d
```

- gr8 mounts the store read-only and asks the local model-loader for the
  pieces its rank needs through the socket `model-loader.sock` in the store.
- gr8 starts once model-loader is healthy, that is once that socket answers
  (`model-loader -check`).
- The socket is mode 0660 in group 1000, gr8's group (model-loader runs as
  `0:1000`), so gr8 can open it. The local API has no authentication, so it
  is never on a TCP port.
- `GR8_ENV` points at gr8's own `docker/.env` (MODEL, PEERS, ...).
- gr8 reads the store path and socket from `MODEL_CACHE` and
  `MODEL_LOADER`, which kindlingai/gr8#180 adds. Earlier gr8 releases ignore
  them and fetch their own weights.

## A NAS root and Sparks as peers

The NAS holds the master copy and fetches from Hugging Face once; the Sparks
fetch from each other first, then from the NAS.

- On the NAS: `cp nas.env.example .env`, then `docker compose up -d`.
- On each Spark: `cp spark.env.example .env`, set the peers, then
  `docker compose -f docker-compose.yml -f gr8.yml up -d`. On
  kindling-spark-os, use `deploy/spark-os` instead.

Every box and the NAS use the same `catalog.yaml`. `HF_TOKEN` is needed on
the NAS only, and only for gated or private repos.
