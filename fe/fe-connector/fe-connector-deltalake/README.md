# Delta Lake connector

The native read-only connector uses Delta Kernel 4.4.1 to load HMS/filesystem tables
and plan native BE Parquet scans. It fixes one snapshot for a statement's schema,
partitions, files and metadata COUNT. Kernel internal API use stays in
`DeltaKernelCompat`.

## Basic table statistics

`getTableStatistics` reports logical rows and the sum of active data-file sizes.
The row count is `sum(numRecords - deletionVector.cardinality)` over **all** active
files at the requested version. A missing file record count makes the total row
count unknown (`-1`); an empty active-file set has zero rows. File sizes remain
available when some record counts are missing. Collection reads log/checkpoint
metadata, not Parquet data or deletion-vector bitmap objects.

On a cold lookup, the connector schedules collection and immediately returns the
current version checksum's `tableSizeBytes`, if available. A checksum has no total
row count; only `numFiles == 0` proves zero rows. Older checksums are not used for
this fallback. The FE can estimate rows from size and Doris row width. Compression,
variable-width values and deletion vectors can make that estimate inaccurate.
Ordinary queries use the FE's standard external-table row-count cache, including
Delta reads through a Hive catalog. The FE caches estimates by table and may
reuse one after a new Delta commit or after background collection finishes.
After `external_cache_refresh_time_minutes` (10 minutes by default), an access
triggers an asynchronous refresh that can pick up the completed count.

The connector caches statistics by user, database/table/path, table UUID and
snapshot version. Cached tasks use the shared `CatalogMetaCache` ownership and invalidation
rules. At most one load starts per retained entry. The plugin shares two background
workers and a queue of 64 tasks; saturation returns the fallback and is retryable,
never running collection on the query thread. Queued tasks retain coordinates, not
snapshots or AddFile lists. Each worker restores the plugin classloader and storage
authentication through `DeltaLakeRuntime.execute`, reloads the pinned version and
checks its table UUID. Failures are logged and retried on a subsequent lookup.
REFRESH, eviction and catalog close cancel the removed task; a late completion
cannot replace a newly cached task. Cancellation of remote IO still depends on the
filesystem's interrupt/timeout behavior.

| Catalog property | Default | Meaning |
| --- | --- | --- |
| `meta.cache.deltalake.statistics.enable` | `true` | Enable versioned statistics collection/cache. |
| `meta.cache.deltalake.statistics.ttl-second` | `1800` | Expire after access; `-1` disables time expiry. |
| `meta.cache.deltalake.statistics.capacity` | `1000` | Maximum entries. |
| `meta.cache.deltalake.statistics.max-weight` | unset | Optional estimated-byte limit using the shared cache framework. |

`enable=false`, `ttl-second=0` or `capacity=0` disables the statistics cache and
background collection, leaving the checksum fallback. ALTER CATALOG rebuilds the
catalog with the new settings. `REFRESH TABLE`, DATABASE and CATALOG invalidate
statistics in the same scope as snapshots. Existing latest-version TTL rules still
control when a new query discovers an external commit.

Basic row/size statistics are part of M1. Column NDV and histograms are not provided.
Optimizer estimates never feed exact COUNT pushdown: that path independently
requires complete unfiltered metadata and shares only the row-count aggregation
logic. Statistics improve planning inputs, but do not by themselves establish
performance parity with Hive.

## Small-table scan scheduling

With external-table batch mode enabled, Delta checks the current checksum's file
count, then a complete count already observed in the statement, then a cached
count for the same user/table identity/version. A known count below
`num_files_in_batch_mode` uses synchronous split planning; large or unknown counts
keep streaming. The scheduling lookup never starts or waits for statistics and
never enumerates files. Background statistics retain one additional file counter,
including when row statistics are missing. Normal exhaustive scans also publish
their complete active-file count into the same cache entry without starting a
statistics task. Partial scans and partition-pruned replay counts cannot establish
the table total. Reports update only the entry captured before scanning, so late
completion cannot resurrect an invalidated entry. A cold snapshot without a checksum
can still stream until a complete count is available. This decision is local
to the Delta plugin, including Delta tables read through an HMS gateway; it does
not change the shared FE row-count cache or BE scanner scheduling.

## Verification

The standard FE UT reactor includes Delta and its metastore module. Run
`./run-fe-ut.sh --run 'DeltaLake*Test,DeltaKernel*Test'` for the connector suites,
and the FE/Hive statistics tests for engine integration. The connector tests cover cold lookups, concurrent
single-flight, user/version/UUID/path isolation, missing/invalid statistics,
checksum version matching, deletion vectors, V2 checkpoints, retry, invalidation
and close. Eight concurrent scans share one cached real checkpoint/DV snapshot;
half close iterators early while the other scans must enumerate all files.

`DeltaLakePlanningScaleTest` is opt-in through `deltalake.planning.table`, using the
retained million-file planning fixture documented in the Delta p2 README. It also
checks asynchronous statistics in a bounded test JVM.

Run `run-regression-test.sh --run -d external_table_p0/deltalake
-s test_deltalake_statistics` against the optional Delta fixtures. The suite checks
size-based and unknown CBO estimates with collection disabled, and exact COUNT
through filesystem, HMS and Hive gateway catalogs, including after a table refresh.
The optional benchmark checks refreshed estimates against the fixture manifest.
