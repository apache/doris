# Async File Cache Write Microbenchmark

## Scope

`async_file_cache_write_microbench` measures the asynchronous cache-write components added below
`CachedRemoteFileReader`. It uses a real filesystem-backed `BlockFileCache` and production
`InflightWriteBufferIndex`, admission control, bounded locked FIFO queue, worker pool, `get_or_set`,
`append`, and `finalize` implementations. Its deterministic in-memory remote reader removes S3 and
network latency from the comparison.

This complements the existing `file_cache_microbench`:

- The existing S3 read mode is useful for a complete remote-read environment, but its results also
  contain object-store and network latency.
- The existing direct `get_or_set` mode measures the lower-level cache lookup path, but does not
  exercise asynchronous buffer publication, queue admission, background workers, or persistence.
- The asynchronous benchmark connects those production components and verifies the final cache
  state, while keeping the source read deterministic.

The measured reader flow is:

```text
SyntheticRemoteFileReader
  -> CachedRemoteFileReader
  -> inflight-buffer lookup
  -> BlockFileCache probe
  -> foreground remote fill
  -> InflightWriteBufferIndex publication
  -> AsyncCacheWriteService admission and locked FIFO queue
  -> worker get_or_set, append, and finalize
  -> inflight entry cleanup
```

## Benchmark groups

The benchmark groups are:

- `reader`
  - Starts every case with an empty cache.
  - Issues concurrent, non-overlapping cold reads through `CachedRemoteFileReader`.
  - Compares forced synchronous writes with asynchronous writes using the same source data and
    requested ranges.
  - Validates returned bytes and verifies that the complete aligned block range is readable from
    the final cache state.
  - Separates caller-visible foreground time from the time needed to drain accepted asynchronous
    writes.
- `service`
  - Submits unique real buffers directly to `AsyncCacheWriteService` from concurrent producers.
  - Measures the drop-oldest queue with 1, 4, and 16 workers by default, including allocation,
    inflight publication, queue admission, worker consumption, `get_or_set`, `append`, `finalize`,
    and completion cleanup.
  - Verifies every persisted task in the final `BlockFileCache`.
    Accepted-but-not-persisted tasks must equal `evicted`.
  - Includes a deliberately bounded saturated case. `accepted`, `rejected`, and `evicted`
    distinguish rejection when no queued victim exists from replacement of the oldest queued task.
- `index`
  - Measures `InflightWriteBufferIndex::lookup` independently from disk writes.
  - `sharded_miss` spreads absent keys across shards.
  - `sharded_hit` spreads pre-populated keys across shards.
  - `hot_key_hit` sends all producers to one key to expose the upper bound of lock contention.

The integrated reader and service groups cover index insertion and conditional removal. The index
group intentionally measures only lookup contention so it does not duplicate those flows.

## Disk baseline

Run the installed wrapper instead of invoking the binary directly when comparing machines. With
the default `RUN_FIO=auto`, the wrapper checks whether `fio` is installed and, if available, runs
these baselines on the same filesystem before starting the cache benchmark:

- `seqwrite_qd1`: direct 1 MiB sequential writes at queue depth 1, showing single-stream device
  throughput and latency.
- `randwrite_qd16`: direct 1 MiB random writes at queue depth 16, showing concurrent device
  throughput and latency for a workload closer to writes spread across cache files.

The fio files are created in a unique directory next to `--cache_path`, use `--unlink=1`, and are
not placed in `/tmp`. Direct I/O avoids filling the page cache immediately before the cache
benchmark. These fio results describe the storage environment; they are not numerically equivalent
to cache `persisted_mib_per_sec`, because production cache files use buffered writes without an
`fsync` or `fdatasync` in the measured completion path.

Environment controls for the wrapper are:

| Variable | Default | Meaning |
| --- | --- | --- |
| `RUN_FIO` | `auto` | Run when fio exists. Use `1` to require fio or `0` to skip it. |
| `FIO_SIZE` | `1G` | Address space used by each direct-I/O fio case. |
| `FIO_RUNTIME` | `5` | Measured seconds for each fio case, after a one-second ramp. |

## Defaults and repetitions

The defaults represent small reads that cause full 1 MiB cache-block writes:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `block_size` | 1 MiB | Cache alignment and persisted bytes per reader miss |
| `request_size` | 64 KiB | Bytes returned by each caller read |
| `reader_operations` | 128 | Cold blocks in each synchronous or asynchronous reader case |
| `service_task_size` | 1 MiB | Payload in each direct service task |
| `service_operations` | 256 | Attempts in each service case |
| `producer_threads` | 16 | Concurrent readers or submitters |
| `reader_workers` | 16 | Workers in the asynchronous reader comparison |
| `worker_counts` | 1, 4, 16 | Worker scaling points in the service group |
| `backpressure_pending_tasks` | 64 | Pending-task limit in the saturated service case |
| `index_operations_per_thread` | 100,000 | Lookups performed by each index producer |
| `index_key_count` | 4,096 | Keys in each sharded index case |
| `repetitions` | 5 | Repeated executions of every selected case |

Each repetition clears and drains the real cache before every reader or service case. The process
prints the one-based `repetition` on every `RESULT` line. Use the median across repetitions as the
primary comparison and retain the minimum-to-maximum range to expose scheduler, filesystem, page
cache, and background writeback noise. The first repetition is intentionally retained instead of
being silently treated as warm-up.

For formal comparisons, use the same Release build, host, cache filesystem, arguments, and idle
machine state. Five repetitions are the default for a quick comparison; increase
`--repetitions` when the min-to-max spread is large.

## Build and run

Performance numbers should come from a Release build. Build the standalone benchmark and its
wrapper with:

```bash
./build.sh --be --file-cache-microbench -j100
```

Run all groups, the fio baseline, and five repetitions with:

```bash
./output/be/bin/run-async-file-cache-write-microbench.sh \
    --benchmark_mode=all \
    --cache_path=./output/async_file_cache_write_microbench \
    --producer_threads=16 \
    --worker_counts=1,4,16 \
    --repetitions=5 2>&1 | tee ./output/async_file_cache_write_microbench.log
```

> `--cache_path` is benchmark-owned and must not exist or must be an empty directory. The benchmark
> rejects a non-empty path instead of clearing it, and removes only the directory it accepted on
> exit unless `--keep_cache` is set. Always use a dedicated path under `output/`; never point it at
> an existing cache or data directory.

Invoke `output/be/lib/async_file_cache_write_microbench` directly only when the fio baseline is not
wanted. Use `--help` to adjust operation counts, request and task sizes, queue limit, worker counts,
repetitions, and cache retention.

## Result fields

Each measured case emits one machine-readable `RESULT` line:

- Identity and shape: `benchmark`, `variant`, `repetition`, `producers`, `workers`, and
  `operations`.
- Admission and correctness: `accepted`, `rejected`, `evicted`, and `persisted`. A case fails
  instead of printing a successful result if returned data is wrong or if drop-oldest admission
  does not satisfy `accepted - persisted = evicted`.
- Timing: `foreground_seconds` ends when producer or reader calls return; `drain_seconds` is the
  remaining background completion time; `total_seconds` includes both.
- Rates: `foreground_ops_per_sec` measures caller or submitter completion.
- Queue contention: `queue_lock_wait_p99_us` and `queue_lock_hold_p99_us` expose the service's
  rolling P99 FIFO mutex acquisition and critical-section time.
- Memory: `peak_buffer_bytes` samples tracked async-write payload memory alongside peak pending,
  queued, and inflight counts.
  `persisted_mib_per_sec` divides verified bytes by total time and does not claim durable-media
  completion.
- Foreground latency: `avg_us`, `p50_us`, `p95_us`, `p99_us`, and `max_us`.
- Queue shape: `peak_pending`, `peak_queued`, and `peak_inflight` are sampled high-water marks.
  `pending` includes queued and active accepted tasks. `inflight` can briefly exceed `pending`
  because a producer publishes its buffer before admission and conditionally removes it after a
  rejection.
- Index-only rates: `elapsed_seconds` and `operations_per_sec` replace write-specific rate fields.

Use reader `foreground_ops_per_sec` and latency to quantify caller benefit from asynchronous
writes. Use service `persisted_mib_per_sec`, `drain_seconds`, and peak gauges together to determine
whether workers or admission are limiting progress. Use the saturated case's rejection and eviction
ratios to validate bounded overload behavior, and compare sharded versus hot-key index results to
quantify lock-contention sensitivity.

## Out of scope

This benchmark does not measure S3 or network latency, complete scanner/query throughput,
cache-hit read throughput, restart recovery, multiple-cache-disk balancing, or durable `fsync`
throughput. It is also not a replacement for BE unit and regression tests: it validates data and
final cache coverage to reject invalid performance samples, but its primary purpose is controlled
performance comparison.
