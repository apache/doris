# Cloud query spill to remote object storage

## Prerequisites and configuration

Remote spill is opt-in and available only in cloud mode. Configure a working object-storage
storage vault before running queries that need remote spill. Doris does not create a vault
for spill. An HDFS vault cannot be used for this path.

In `be.conf`, set:

```properties
spill_storage_type = remote
# Optional: a storage vault ID, not its display name.
spill_s3_storage_vault_id = <object-storage-vault-id>
```

Restart the BE after changing these two settings. `spill_storage_type=local` remains the
default. The supported values are `local` and `remote`. The `spill_s3_*` settings configure
the remote spill implementation. The remote filesystem is the object-storage filesystem
used by the configured vault, not necessarily AWS. Provider support and credentials come from
that vault; this setting does not add support for another vault/filesystem type.

If the vault ID is omitted, a cloud instance without storage vaults uses its latest object
filesystem. In storage-vault mode, Doris selects the object-storage vault with the largest
numeric ID. Changing the default vault does not change this selection. Adding a newer vault
can move **new** spill files to it; existing files retain their original filesystem for reads
and cleanup. Pin an explicit vault ID when this automatic selection is not desired.

Queries still need to enable the spill operators appropriate to their workload. Selecting a
remote backend alone does not force every query to spill. External/native writers that require
local paths continue using `spill_storage_root_path`, even when query spill uses remote storage.
Do not remove local spill paths if such writers are used.

## Capacity and upload control

- `spill_storage_limit_bytes`: mutable, default `0` (unlimited remote capacity). A positive
  value limits retained query spill data **per BE**, not per query, bucket or cluster. Each
  local spill path has a separate limit. This is a workload/cost guardrail, not a measurement
  of free space in the object store. Failed deletions remain charged until GC succeeds.
- `spill_storage_limit`: applies only to local paths. Percentage limits cannot describe
  the capacity of a remote bucket.
- `spill_file_part_size_bytes`: mutable, default 1 GiB. A remote writer uses at least
  `s3_write_buffer_size` as its part threshold, avoiding mostly empty upload buffers.
  New writers observe changes; a writer keeps its initial threshold.
- `spill_s3_max_inflight_upload_bytes`: mutable, default 256 MiB. Backpressure counts submitted
  spill upload buffers, both queued and running, across writers of the BE's spill manager.
  Uploads from unrelated load jobs do not throttle spill merely because they share the S3 upload
  thread pool. Each S3 writer owns its upload progress; spill aggregates read-only weak views
  without retaining writers or injecting a shared counter into them. Idle partial buffers
  are excluded so they cannot block all writers. This is a soft threshold: concurrent appends
  can overshoot it, and final buffers must still be submitted to finish a part.
- `spill_s3_read_coalesce_bytes`: mutable, default 8 MiB, further bounded by the query's
  `spill_buffer_size_bytes`. `0` disables coalescing. A single larger serialized block is
  still read whole.

An append copies data to an in-memory upload buffer; full buffers are submitted asynchronously.
It does not create a local staging file. Closing/rotating a part waits for its uploads and
publishes the object. A cancelled/failed unfinished part is not completed.

## Cleanup and operational requirements

Objects are stored under:

```text
<vault-prefix>/spill/<random-BE-process-id>/<query-id>/<spill-file>/<part-number>
```

The process ID is opaque and contains no BE IP address or port. Restarting the BE changes this
root. Local spill paths keep their existing layout and startup recovery.

Releasing a `SpillFile` or a local query context enqueues physical cleanup instead of deleting
files on the query teardown thread. Writer teardown remains synchronous: if the file still has
an unfinished writer, discarding it waits for submitted uploads without publishing the unfinished
part, before queuing deletion. This can still delay file/query teardown; GC does not take
ownership of unfinished writers. Reservations and retained-object statistics decrease after
successful physical deletion. A failed delete is retried in later GC rounds; query-directory
cleanup also removes empty local directories and residual external-writer files.

Shutdown does not start an additional deletion/retry pass. It still joins a GC pass already
running, and outstanding I/O writers must remain alive until their callbacks finish. After a
restart, local leftovers are recovered by startup GC. Remote leftovers are **not** scanned by
another BE: configure a bucket lifecycle rule to expire keys under `<vault-prefix>/spill/`
and abort incomplete multipart uploads. Choose an expiration longer than the longest query
that may still read those objects; an aggressive rule can delete live spill data. Keep the
rule for the old prefix when changing an existing deployment from address-based object keys.

`SHOW DATA` exposes best-effort retained remote spill bytes through the `__remote_spill__` row.
It excludes uncommitted reservations and is not an authoritative billing metric. Query profiles
and audit logs expose remote spill traffic separately from local spill. During a delete outage,
inspect the pending-deletion metric and warning logs containing query ID, filesystem ID and path.
