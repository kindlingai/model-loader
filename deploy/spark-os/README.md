# spark-os deployment

Integration contract for running model-loader as a systemd service inside
kindling-spark-os, modeled on that repo's `mentatd.service` conventions. This
directory is consumed by kindling-spark-os's overlay build, not run directly
from here.

To wire it in on the kindling-spark-os side:

1. Take the binary from the published image,
   `ghcr.io/kindlingai/model-loader:<version>` pinned by digest, the same way
   kindling-spark-os takes mentatd (`COPY --from=... /usr/local/bin/model-loader`),
   and stage it at `/usr/local/bin/model-loader` in the rootfs.
2. Copy `model-loader.service` to `overlay/etc/systemd/system/model-loader.service`
   and enable it (`overlay/etc/systemd/system/multi-user.target.wants/model-loader.service`
   symlink, matching how other overlay units are enabled).
3. Ship a catalog at the path the unit defaults to, `/etc/model-loader/catalog.yaml`
   (or override via `MODEL_LOADER_CATALOG` in the env file).
4. At install/first-boot time, write `/etc/spark/model-loader.env` from
   `model-loader.env.example` in this directory, filling in per-box values
   (`MODEL_LOADER_MODE`, `MODEL_LOADER_PEERS`, ...). The unit's
   `ConditionPathExists=/etc/spark/model-loader.env` means the service simply
   doesn't start on a box where this file hasn't been provisioned yet, instead
   of failing.

The unit intentionally has no spark-os-specific logic beyond the env-to-flag
shell wrapper in `ExecStart=`. The binary and catalog are identical to the k8s
and docker-compose deploy targets; only this glue differs.
