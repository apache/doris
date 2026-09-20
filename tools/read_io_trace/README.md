# Read-Ahead / Hole-Fill IO overlap diagnostics

This opt-in trace answers whether foreground reads and background hole filling successfully fetch the same object bytes, and shows the ordering of those reads and fragment submissions. It observes the existing IO paths; it changes no planning, cache admission, merging or cancellation decisions.

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
| `remote_miss_coverage` | Extra read-only probes just before a whole-range miss. `disk_bytes` and `inflight_bytes` may overlap; `available_bytes` is their union. |

The coverage probe observes resident downloaded intervals (including irregular block boundaries) and indexed inflight buffers, clipped to the requested range and EOF. It creates no cache cells, loads no on-disk metadata, touches no LRU entries and does not change the read decision. It can undercount downloaded blocks whose metadata has not yet been loaded. Cache and inflight snapshots are taken sequentially, so they are evidence of observed coverage, not a transactional snapshot of the GET's entire lifetime. Diagnostic inflight lookups also increment the index's existing lookup/hit counters.

The trace covers calls visible to `S3FileReader`, not HTTP retries hidden inside an object-storage SDK, protocol overhead or all bytes transferred by failed requests. Failed/short attempts are reported separately. A task can merge fragments from another query; the fragment retains that query's identity, while the task's GETs retain its creator's Query ID. Per-query totals therefore do not measure cross-query sharing; isolate queries for this experiment. Process-level/background IO with unknown Query ID is reported as `unknown`, not silently attributed to the foreground query.

Unconsumed prefetch or newly fetched cache bytes may increase unique bytes without any overlap. This tool quantifies duplicate successful reads; it does not equate all hole-fill traffic with waste or all unique traffic with useful query consumption. Queue eviction and disk persistence outcomes remain covered by the existing cache diagnostics, not this range trace.

## Tests

```bash
python3 -m unittest discover -s tools/read_io_trace -p 'test_*.py' -v
./run-be-ut.sh --run --filter='ReadIOTrace*:S3FileReaderTest.*:FileRangeReadSchedulerTest.*:PartialBlockWritebackManagerTest.*:AsyncCachedRemoteFileReaderTest.*:RangeCacheWritebackTest.*' -j100
```
