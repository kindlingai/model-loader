# spark-os deployment

model-loader as a systemd service in kindling-spark-os, next to mentatd, with
gr8 in its container reading the store. kindling-spark-os's overlay build
takes this directory; nothing here runs on its own.

## On the kindling-spark-os side

1. Take the binary from the published image,
   `ghcr.io/kindlingai/model-loader:<version>` pinned by digest, the same way
   kindling-spark-os takes mentatd (`COPY --from=... /usr/local/bin/model-loader`),
   and stage it at `/usr/local/bin/model-loader` in the rootfs.
2. Copy `model-loader.service` to
   `overlay/etc/systemd/system/model-loader.service` and enable it (a
   `multi-user.target.wants` symlink, as for the other overlay units).
3. Ship a catalog at `/etc/model-loader/catalog.yaml`, the path the unit
   uses unless `MODEL_LOADER_CATALOG` says otherwise. Every box and the NAS
   list the same models.

## On each box

1. Install spark-os with `--mounts /var/lib/model-loader`, so the store is
   on the DGX OS disk and lasts across boots. The unit waits for that mount
   (`RequiresMountsFor=`); a store elsewhere needs a drop-in that waits for
   its own.
2. Write `/etc/spark/model-loader.env` from `model-loader.env.example`.
   Until it is there the unit does not start (`ConditionPathExists`), rather
   than fail.

The example env makes the box a peer: it finds the other boxes through
mentatd, adds the NAS root (`MODEL_LOADER_PEERS`), holds only what gr8 asks
for (`MODEL_LOADER_SELECTION` names a file that is not there), and keeps the
store under `MODEL_LOADER_DISK_BUDGET`. The NAS runs `deploy/compose` with
`nas.env.example`. Every box runs model-loader on the same port (7762):
mentatd does not say which port each box uses yet (kindlingai/mentat#4).

## gr8

`gr8.override.yml` goes with gr8's own `docker/compose.yaml`:

    docker compose -f docker/compose.yaml -f gr8.override.yml up -d

It mounts `/var/lib/model-loader` read-only in the gr8 container, tells gr8
where the store and the local model-loader socket are, and starts gr8 once
that socket answers. The socket is mode 0660 in group 1000, gr8's group
(the unit's `Group=1000`), so gr8 can open it and other users cannot.
