# Read-Ahead / Hole-Fill IO overlap diagnostics

This opt-in trace answers whether foreground reads and background hole filling successfully fetch the same object bytes, and shows the ordering of those reads and fragment submissions. It also separates foreground writeback submission costs and background queue-scan costs when a longer fragment-merge delay reduces duplicate reads but increases query latency. It observes the existing IO paths; it changes no planning, cache admission, merging or cancellation decisions.

## Capture

| BE config | Default | Meaning |
| --- | --- | --- |
| `enable_read_io_trace` | `false` | Dynamically enable interval and lifecycle records in separate JSONL files. |
| `read_io_trace_dir` | Empty | Output directory; empty uses `<BE log directory>/read_io_trace/`. Startup setting. |
| `read_io_trace_flush_bytes` | `1048576` (1 MiB) | Wake the writer when the pending batch reaches this size. Startup setting. |
| `read_io_trace_flush_interval_ms` | `1000` | Flush a smaller pending batch on this interval. Startup setting. |

Records no longer enter `be.INFO`. The calling thread serializes each event and appends it to a memory buffer. One lazily started background thread swaps out the batch under a short lock, then writes it outside the lock. The 1 MiB batch amortizes file writes; the 1 second interval makes small captures visible promptly. There is no cumulative event limit, queue capacity limit, or capacity-based dropping. `read_io_trace_max_events` has been removed.

Start with one isolated representative query. Enable the switch **before** the query starts and leave it enabled until the read-ahead, hole-fill and async-write queues have drained. Then disable it and wait for `doris_read_io_trace_pending_bytes` to reach zero before copying the trace. Disabling stops new records; pending records still flush. Keep the usual before / query-returned / drained metric snapshots and BE process identity. The trace does not sample GETs or depend on Scanner Profile flushing, so asynchronous tails remain observable while enabled.

Files are named `read_io_trace.<pid>-<start-micros>.<index>.jsonl`. Normally one file is appended throughout a BE process; an actual file IO error starts a new file on recovery. Keep all files for the process, including those from earlier enabled intervals, then filter by Query ID in the analyzer. Files are neither rotated by size nor automatically deleted. Graceful shutdown drains the writer; the fast `_exit` path also flushes already submitted records. Writes reach the OS without per-batch `fsync`; a crash can lose the pending tail.

`doris_read_io_trace_events` counts successfully written events; `doris_read_io_trace_pending_bytes` includes the batch being written. `doris_read_io_trace_dropped_events` reports real writer failures or submissions after shutdown, not capacity drops. Each batch ends with a cumulative `read_io_trace_status` checkpoint, allowing the analyzer to detect missing records and reported write failures. File IO errors are also reported in the normal warning log. Serialization and extra cache probes still cost time; disable this diagnostic for performance measurements. The disabled event path performs no JSON formatting, interval bookkeeping, cache-coverage probes or diagnostic ID allocation.

Analyze complete logs, not lines prefiltered by Query ID (which would hide sequence gaps):

```bash
python3 tools/read_io_trace/analyze.py /path/to/read_io_trace/*.jsonl --query-id '<query-id>' > overlap.json
```

For a complete isolated capture, reconcile the successful GET sum with the drained-minus-before `s3_file_reader_bytes_read` delta. Run without a query filter when the metric delta includes other query IDs or background reads:

```bash
python3 tools/read_io_trace/analyze.py /path/to/read_io_trace/*.jsonl --expected-s3-bytes 123456789 > overlap.json
```

The analyzer accepts JSONL, `.gz` files and legacy `be.INFO` traces. It deduplicates identical records from repeated files, checks sequences and flush checkpoints, reports reconciliation failures, and exits with status 2 on capture warnings. `s3_bytes_reconciled: null` means no metric delta was supplied; a warning-free file alone does not prove that tracing started early enough or covers the entire asynchronous tail. Records contain object URIs and Query IDs, but no payloads or credentials; handle them like other query diagnostics.

## Reading the result

All offsets and lengths are **bytes**, with half-open intervals `[offset, offset + size)`. Each BE process and Query ID is analyzed separately, then each immutable cloud object URI is unioned independently. Objects must remain immutable during capture; use this tool for native cloud segment reads in one object-store namespace. Synchronous query GETs, read-ahead GETs, hole-fill GETs and other GETs retain separate source labels.

| Output | Meaning |
| --- | --- |
| `successful_get_bytes`, `unique_bytes` | Successful, full GET bytes and their object-interval union. |
| `duplicate_bytes` | `successful_get_bytes - unique_bytes`; every extra transfer counts once, including third and later reads. |
| `within_source_duplicate_bytes` | Extra reads within foreground (sync + read-ahead), within hole-fill, and within other IO. |
| `cross_source_duplicate_bytes` | Additional copies between those source groups. This plus the within-source values equals `duplicate_bytes`. |
| `foreground_hole_fill_shared_bytes` | Unique bytes read by both foreground and hole-fill. This identifies their overlap; it is already included in the duplicate accounting, not an additional amount to add. |
| `remote_miss_available_bytes` | Sum of downloaded/inflight block coverage observed just before whole-range remote misses. Snapshot evidence of partial-hit rereads, not a disjoint category of `duplicate_bytes`. |
| `examples` | Exact overlapping GET intervals, start/end times, range/task IDs and related lifecycle records. Examples are bounded; byte totals use every captured successful GET. |

`unlinked_async_get_requests` counts successful async GETs with no range/task identity, for example after enabling tracing in the middle of an existing query. Their intervals still contribute to overlap totals, but their lifecycle cannot be linked reliably.

For example, foreground reads `[0, 1048576)` and two hole-fill GETs each read `[131072, 1048576)`. Total successful bytes are `2883584`, unique bytes `1048576`, duplicate bytes `1835008`: `917504` between foreground and hole-fill, plus `917504` within hole-fill. Summing all pairwise intersections would overcount.

An example with `foreground_completed_before_hole_get` and a later `range_writeback` proves those bytes were read before they were submitted to writeback. A matching `fragment_ignored_active` shows which task ignored the late fragment; the overlapping successful GETs establish the redundant transfer. `overlapping_get_lifetimes` demonstrates concurrently outstanding overlapping reads. The reverse order is reported separately. A missing consumption event alone is not proof of a timing cause: the range may be unconsumed or the capture incomplete.

## Events and boundaries

| Event | Observation point |
| --- | --- |
| `s3_get` | Immediately after each `S3FileReader` call to `get_object`, including its explicit retries. Includes requested range, returned bytes, result, attempt, start/end times and owning range/task ID. Only successful full responses enter interval totals. |
| `range_read_done` | Cached-reader call completed, before publishing the read-ahead buffer to consumers. `remote_bytes` distinguishes cache hits from remote reads; it is not another physical GET. |
| `range_writeback` | First consumption submitted a remote-read range for block dispatch. Its ID matches the foreground GET's parent ID. |
| `hole_queued`, `fragment_merged`, `fragment_ignored_active` | Initial partial fragment, accepted queued fragment or ignored active fragment. `id` names the receiving task; `parent_id` names the supplying range. |
| `hole_active`, `hole_plan` | Activation and the fixed complement/coalescing plan that the worker will read. The GET's parent ID names this task. |
| `hole_done` | Task exited; outcome distinguishes pre-read skip, planning/read failure, write submission and unsuccessful handoff. Submission is **not** proof of disk persistence. |
| `range_writeback_done` | Foreground block dispatch finished. Start/end and complete/partial submission times complement `range_writeback`. |
| `hole_submit` | One foreground partial-fragment submission returned, including merges, deduplication and rejection. Carries the supplying range ID, receiving task ID when known, result and stage timings. |
| `hole_queue_scan` | One locked worker scan, with queue length, entries examined, delay/capacity skips and discarded entries. Process-scoped: one scan can visit tasks from several queries. |
| `remote_miss_coverage` | Extra read-only probes just before a whole-range miss. `disk_bytes` and `inflight_bytes` may overlap; `available_bytes` is their union. |

The coverage probe observes resident downloaded intervals (including irregular block boundaries) and indexed inflight buffers, clipped to the requested range and EOF. It creates no cache cells, loads no on-disk metadata, touches no LRU entries and does not change the read decision. It can undercount downloaded blocks whose metadata has not yet been loaded. Cache and inflight snapshots are taken sequentially, so they are evidence of observed coverage, not a transactional snapshot of the GET's entire lifetime. Diagnostic inflight lookups also increment the index's existing lookup/hit counters.

The trace covers calls visible to `S3FileReader`, not HTTP retries hidden inside an object-storage SDK, protocol overhead or all bytes transferred by failed requests. Failed/short attempts are reported separately. A task can merge fragments from another query; the fragment retains that query's identity, while the task's GETs retain its creator's Query ID. Per-query totals therefore do not measure cross-query sharing; isolate queries for this experiment. Process-level/background IO with unknown Query ID is reported as `unknown`, not silently attributed to the foreground query.

Unconsumed prefetch or newly fetched cache bytes may increase unique bytes without any overlap. This tool quantifies duplicate successful reads; it does not equate all hole-fill traffic with waste or all unique traffic with useful query consumption. Queue eviction and disk persistence outcomes remain covered by the existing cache diagnostics, not this range trace.

## Diagnosing a longer merge window

Start with the Profile: `ReadAheadCompleteBlockSubmitTime` and `ReadAheadPartialBlockSubmitTime` are disjoint children of `ReadAheadWritebackTime`. They measure foreground submission, including allocation/copy/admission, and exclude background GET and persistence. These two timers are available with tracing disabled; the detailed diagnostics below require the existing `enable_read_io_trace` switch.

Use the same capture command for both delay settings, keeping pending capacity and other options equal where possible. The analyzer adds `queries[].timings` and `process_diagnostics[].hole_queue_scan`; existing byte/overlap accounting is unchanged. Timing summaries include sample count, cumulative nanoseconds, p50/p95/p99/max and the slowest examples with timestamps and identities. Old captures report **zero samples**, not zero cost, for newly added events.

| Observation | Fields / output | What to check |
| --- | --- | --- |
| Range dispatch | `range_writeback`: `complete_submit_ns`, `partial_submit_ns`, `lifecycle_trace_ns` | Which branch accounts for the increased foreground writeback time? Remaining time includes dispatch and bookkeeping. |
| Partial-block admission | `hole_submit`: `queue_lock_wait_ns`, `queue_lock_hold_ns`, `queue_size` | Is time spent waiting for the manager mutex, or working under it? Timings sum the initial lookup and any subsequent admission attempts; queue size is the last snapshot. |
| Cache probe and allocation | `cache_probe_ns`, `allocation_ns` | Does cache/inflight probing or allocating a tracked block dominate instead? Allocation measures the block buffer, not task-object construction. |
| Fragment merging | `fragment_lock_wait_ns`, `fragment_lock_hold_ns`, `copy_ns`, `copied_bytes` | Does the per-block lock or actual copy dominate? Copy bytes include initial fragments and merged fragments, including copies into a losing admission candidate. |
| Worker queue scan | `duration_ns`, `queue_size`, `scanned`, `delayed`, `capacity_waits`, `discarded` | Does a longer queue cause repeated scans mostly skipping tasks still inside the delay? `by_queue_size` groups the same scans by queue length. |
| Checks inside a scan | `discard_check_ns`, `capacity_check_ns` | Time in epoch/accepting/inflight checks, and in checking async-writer spare capacity, including any nested lock wait. |
| Diagnostic overhead | `lifecycle_trace_ns`; BE delta `doris_read_io_trace_record_time_ns` | The per-submit field measures existing fragment/queue lifecycle recording; the BE counter covers serialization and memory append for all records, including contention on the trace writer. Background file writes are excluded. |
| Remote latency | `successful_get_by_source` | Compare S3 GET distributions by source alongside IO counts and bytes; fewer requests need not imply lower foreground latency. |

`range_writeback` contains partial submissions; fragment-lock hold time contains merged-copy and fragment lifecycle recording. Queue-scan duration contains both check timers. **These nested times must not be added together.** All durations are elapsed thread time and include scheduling delays; they are neither CPU time nor query wall time. The BE trace-record counter overlaps caller timings and is not an extra cost to add to them.

Scan start/end timestamps are captured while holding the manager mutex. The scan summary is serialized only after releasing it, possibly after the existing deadline wait wakes; its recorded duration excludes that wait and condition-variable mutex reacquisition. One event describes the whole scan, so there is no per-entry logging. Detailed timing still reads clocks per checked entry and can perturb scheduling. Submission completion events likewise serialize after manager/fragment locks have been released; their own serialization is outside their recorded duration, but remains inside the enclosing foreground call/Profile timer.

With `--query-id`, process diagnostics retain scans intersecting that query's observed event time envelope, including its recorded asynchronous tail. They include other queries using the same manager and are not attributed to the selected query. Compare the slowest submissions' timestamps with scan timestamps, queue lengths and check costs before attributing high lock wait to scanning. The analysis reports overlapping scans in full rather than clipping their individual timing fields. Capture isolated queries and include drained tails for the cleanest comparison.

## Tests

```bash
python3 -m unittest discover -s tools/read_io_trace -p 'test_*.py' -v
./run-be-ut.sh --run --filter='ReadIOTrace*:ReadAheadMetricsTest.*:S3FileReaderTest.*:FileRangeReadSchedulerTest.*:PartialBlockWritebackManagerTest.*:AsyncCachedRemoteFileReaderTest.*:RangeCacheWritebackTest.*' -j100
```
