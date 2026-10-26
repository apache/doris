# Lance cache diagnostics and scalar index selection

## Cache misses in query profiles

The `LanceReader / LanceSessionCache` profile group contains:

| Counter | Meaning |
| --- | --- |
| `LanceSessionIndexCacheMisses` | Increase in the BE-wide Lance Session index-cache miss counter during this reader's dataset-open windows. |
| `LanceSessionMetadataCacheMisses` | Increase in the BE-wide Lance Session metadata-cache miss counter during the same windows, including dataset opening. |

The reader takes a baseline before opening its dataset and publishes the difference
when closing it. Reopening a dataset adds another observation window; closing a
reader twice does not count the same window twice. `LanceSessionCacheStatsAvailable`
is `true` only after statistics have been collected successfully for every observed
window. When it is `false`, zero counters do not establish that the caches had no misses.

The Session is shared by all readers in one BE. These counters include activity from
concurrent readers and are **not query-exclusive miss counts**. Overlapping windows
must not be summed to obtain a query's exact misses or a cluster-wide miss total.
`LanceSessionCacheStatsScope` records this limitation in the profile. An isolated
query can still have several readers in one BE. For an exact query total, isolate
the query and compare each BE's Session metrics before and after it, rather than
summing reader windows. These counters do not measure the FE's separate metadata cache.
`LanceIndexPartitionCacheMissLoads` remains a separate, narrower execution metric.

## Automatic scalar index selection

`enable_lance_scalar_index_auto_selection` is a startup BE configuration, enabled
by default. Restart the BE after changing it. There is no FE session variable.

FE retains its existing coverage-based grouping and assigns each task an explicit,
disjoint fragment domain. In BE, lance-c can replace the grouping index with a
BTree segment whose known coverage contains the **entire** task domain. It uses
the current snapshot's BTree page lookup (page min/max and null counts), without
Doris column statistics or `ANALYZE`. It estimates matching pages for equality,
range, small IN lists, IS NULL, and supported AND/OR combinations:

```text
estimated work = matching index pages * rows per index page
```

This is a conservative page-based estimate, not an exact candidate count or a
latency measurement. Intersected AND predicates may still match different values
inside the same page. Cache residency and the cost of fetching projected columns
are not modeled. Only a strictly cheaper estimate replaces the grouping index;
ties retain it. If the best estimate is at least the task's physical row count,
BE scans that domain with the complete filter instead of searching an index.

Each selection examines at most eight segments and an aggregate 8 MiB of lookup
files; a lookup has at most 65,536 pages, an IN list at most 256 values, and
one estimate at most 1,000,000 page/predicate comparisons. Lookup
batches share the bounded Session metadata cache. Probing does not search index
leaf pages or materialize candidate row IDs. Unknown file sizes, unsupported
expressions/formats, probe errors, or an unestimable grouping index retain the
existing segment path. Bitmap and LabelList remain supported on that path, but
are not cost-ranked. Uncovered fragments keep their existing fallback scans.

`LanceScalarIndexAutoCandidates`, `LanceScalarIndexAutoChanged`,
`LanceScalarIndexAutoEstimatedPages`, `LanceScalarIndexAutoEstimatedRows`, and
`LanceScalarIndexAutoFullScans` expose runtime decisions in Profile. Estimates
are accumulated over tasks. `EXPLAIN`'s `lanceGroupingIndex` identifies FE's task
grouping, which can differ from the runtime index.

Setting the BE option to `false` retains the previous segment behavior and avoids
these probes. The `lance_fragments_per_split` debug override still bypasses
index-segment grouping. Vector/FTS index selection and external-search TVF filter
semantics are unchanged.

## Foyer memory capacity

`lance_data_cache_memory_capacity_bytes` is a startup BE configuration, independent
of `lance_data_cache_read_block_size_bytes` and the disk capacity. Restart the BE
after changing it.

| Value | Behavior |
| --- | --- |
| `0` (default) | Use one configured read block, preserving the previous behavior. With the default block size this is 1 MiB. |
| Positive | Set the Foyer memory tier's capacity in bytes. It must be at least one read block. |
| Negative | Invalid when the Lance data cache is enabled. |

For example, `lance_data_cache_memory_capacity_bytes=10737418240` allows a 10 GiB
memory tier while retaining 1 MiB read blocks. This capacity is shared by Lance
readers in one BE and is separate from index and metadata cache capacities. Cache
bookkeeping and in-flight reads can consume additional memory. When
`enable_lance_data_cache=false`, the Foyer configuration is ignored.
