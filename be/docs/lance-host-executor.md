# Lance host execution

`enable_lance_host_executor` defaults to `true`. Doris installs host services
before opening the shared Lance session or any query reader. Setting this
startup option to `false` provides a rollback path after restarting the BE.

The host mode covers the BE Lance-C read path. Its byte reads use Doris storage callbacks.
A single `new_current_thread` Tokio reactor is driven on a Doris-owned service
thread. Tokio remains a dependency; its workers and blocking pool are not the
execution resources for a Doris reader.

This boundary follows ClickHouse's Vortex integration: the embedded library queues
work for execution on the host's threads. Lance's
storage adapters additionally need an explicit policy for native I/O engines and
for dependencies that otherwise construct another runtime.

## Execution resources

| Work | Doris execution resource |
| --- | --- |
| Query scan, decode and execution-plan stream polls | The query's `ScannerScheduler`, with cooperative help from the waiting caller |
| Blocking Doris FileReader/FileSystem operations | A `ThreadPoolToken` on `ExecEnv::s3_file_system_thread_pool()` |
| Network readiness, timers and Tokio join/cancel notifications | One Doris `LanceReactor` service thread driving the threadless reactor |
| Shared background/cache CPU work without a query, and deferred destruction | A bounded Doris `LanceBackground` pool |

`lance_background_thread_pool_size` limits the BE-wide background CPU and cleanup
pool. It defaults to `2`, creates workers on demand, and must be positive. The
single `LanceReactor` worker drives control events, not these cleanup jobs.

`lance_io_parallelism` is a positive startup setting, default `64`. Doris passes
it as the SDK's `io_parallelism` storage option and uses it to bound each reader's
submitted I/O queue jobs, capped by `max_s3_file_system_thread_num` (default `64`).
The I/O pool is shared with other BE filesystem work; each reader does not own
that many threads. Set these options in `be.conf` and restart the BE:

```properties
lance_cpu_parallelism = 8
lance_io_parallelism = 64
lance_background_thread_pool_size = 2
```

An explicit `io_parallelism` storage option can override the SDK budget. The
host queue and shared I/O pool still enforce their limits. For host stores,
precedence is explicit storage option, then `LANCE_IO_THREADS`, then `64`;
Doris supplies an explicit option, so the BE setting takes precedence over the
environment variable. Limits are resolved when opening a store and included
in dataset/cache identity. Zero, negative, malformed and overflowing values
are rejected. This is an I/O concurrency budget, not a Tokio thread count.

CPU work uses the query's `ScannerScheduler`. The positive startup setting
`lance_cpu_parallelism` defaults to eight submitted CPU queue jobs per reader,
with cooperative help from its waiting caller. Neither the
I/O setting nor the background-pool size changes the query's CPU scheduler.

The query CPU path uses the existing scheduler and its workload-group limits.
Host jobs attach the query's `ResourceContext`. Jobs without a query, including
shared cache services, use the process services. Focused tests that do not
initialize `ExecEnv` use a Doris-owned fallback I/O pool.

The Rust SDK propagates the executor across spawned tasks and DataFusion plan
nodes. Each plan stream has a bounded handoff to a host-driven pump, so a
DataFusion control task does not execute a long filter or decode poll on the
reactor. The local Tokio patch redirects blocking submissions, including those
inside dependent libraries, instead of starting Tokio blocking workers.

## Storage and callback thread ownership

| Entry point | Host-mode execution |
| --- | --- |
| Lance-C async scan completions | A serial queue drained on host I/O workers; no `lance-c-dispatcher` thread |
| Local `file` / `file+uring` reads | Doris LocalFileReader on host I/O workers; no SDK uring pool |
| Hugging Face HTTP and Xet reads | Xet wraps the already driven host reactor, including its host blocking executor; no `hf-xet` worker runtime |
| GooseFS local page cache | The portable local page store uses host-dispatched file operations, retaining the cache even when uring was requested |
| Pairwise Hamming calculations | Balanced SIMD CPU chunks on the operation executor, without a nested Rayon pool |
| Raw data-file cache | Doris CachedRemoteFileReader / File Cache; the Lance-C Foyer integration is removed |

The runtime checks are specific to host mode. Standalone Lance-C users that do
not register host services retain the upstream execution choices. The local I/O
fallbacks preserve read and cache behavior but do not retain native io_uring
execution. Pairwise Hamming uses the operation executor's CPU budget. Its
synchronous caller helps ready CPU children while joining bounded result slots,
without requiring a Tokio completion bridge. Results keep their original ordering;
64-bit and wider binary hashes retain their batch SIMD kernels. Small inputs or a
single-worker budget run directly. The complete pairwise result can still require
quadratic memory; this change does not add Rust allocation accounting. These SDK
utility functions do not introduce a Hamming SQL operator into Doris.

Xet normally rejects a current-thread Tokio runtime. The local dependency patch
allows one only when it has an explicitly installed host blocking executor and
both the network and timer drivers. Ordinary current-thread runtimes retain the
upstream rejection. Xet's automatic external-runtime path does not initialize its
standalone system monitor or logging service.

Async callback delivery remains serial and installs each completion's error in
the delivery thread's TLS. Rejected host jobs must still deliver accepted
completions exactly once, outside SDK wake locks. Runtime stop waits for callback
delivery and deferred destruction before stopping the control reactor. An accepted
async scan also reports cancellation if its task is rejected before its first poll.

This boundary does not cover FE JNI metadata runtimes or SDK-only write/index
maintenance APIs. In particular, index training, mem-WAL FTS merge and optional
SDK-native storage engines are not invoked by the BE reader. They must be audited
again before exposing them through this read path.

## Host storage

`LanceFileSystem` supplies retained open/read_at/close/list callbacks to
`lance_dataset_open_with_storage_v1`. The SDK installs this object store before
DatasetBuilder loads its manifest. There is no post-open swap or native-storage
fallback. The supplied version still pins the dataset snapshot.

```text
Lance scanner / index / manifest access
  -> read-only host ObjectStore (Lance-C)
  -> operation's host I/O queue
  -> LanceFileSystem
  -> Doris FileReader
       -> CachedRemoteFileReader / Doris File Cache (remote immutable data/*.lance)
       -> S3FileReader / HdfsFileReader / HttpFileReader / LocalFileReader
```

Manifests, indices and deletion files use Doris readers but bypass the byte cache.
Lance Session index/metadata object caches remain shared. Their namespaces include
storage origin/configuration and read-cache policy, while physical byte-cache keys
include the origin/configuration and canonical object URI. Cache namespaces do not
contain plaintext credentials. Retained SDK index readers follow the currently
attached task for cancellation and query attribution, rather than the first query
that opened them. Host storage descriptors and strings are copied; retained context
and open file handles remain alive until their last SDK owner releases them.

BE `enable_file_cache`, query `enable_file_cache`, `file_cache_base_path`, and the
existing Doris cache capacity/eviction controls apply. Remove obsolete
`enable_lance_data_cache` and `lance_data_cache_*` settings from BE configuration.
Supported BE schemes are local file, S3-compatible storage/OSS, HDFS and HTTP range
reads. HTTP directory listing and backends without a Doris reader return explicit
errors. The Xet/GooseFS safeguards above remain for SDK consumers/tests; these
backends do not bypass Doris storage when opened by this BE adapter.

## Reader flow

Each `LanceTableReader` keeps one executor across its splits. `prepare_split`,
`get_block`, `read_by_row_ids`, `abort_split` and `close` enter a thread-bound
executor scope. Schema discovery uses a temporary executor.

Ready work is queued in the reader's CPU or I/O queue and bounded jobs
are submitted to the host. Doris bounds CPU queue jobs by `lance_cpu_parallelism`; the
I/O queue uses `min(lance_io_parallelism, max_s3_file_system_thread_num)`.
The shared scheduler and I/O pool enforce their aggregate execution limits. A driver runs a bounded number of polls before
resubmitting remaining work. The synchronous caller helps ready CPU children
while waiting. This guarantees progress even when every scanner worker is
waiting for children submitted to the same pool. I/O normally runs through
the shared I/O token; the caller can help if dispatch is unavailable.

When work is not ready, the caller parks until a completion or wakeup unparks
it. This retains the synchronous `TableReader` API while giving the host
control over the execution threads. A running CPU closure or blocking system
call cannot be preempted by cancellation.

## Query ownership and control-reactor work

A shared SDK `ScanScheduler` must not retain the query that created it. Its
long-lived dispatch loop uses process services, while each submitted I/O task
captures the submitting operation executor. The task's file reads, post-read
processing and nested submissions consequently follow the current request.
Normal completion, rejection and cancellation must all return the shared I/O
slot exactly once, allowing later queries to reuse the scheduler.

Legacy full-text index loading uses the same scoped spawning as modern reads.
Reading token/document/posting files and constructing document mappings therefore
cannot escape into a raw Tokio task. V1 batch reads and range-take tasks also
preserve their operation executor. Plan-stream pumps keep ordinary decoder and
filter polls on the host CPU path.

Tokio still drives timers, readiness and join/abort notifications. A destructor
may acquire an SDK lock, so a rejected task cannot be destroyed synchronously
inside its wake callback. A short asynchronous reactor handoff submits
actual destruction to the host cleanup queue without running destructors inline
on the wake callback stack. Deferred buffer/future destruction
and rejected async-completion callbacks run on `LanceBackground`, rather than
occupying `LanceReactor`.

During shutdown Doris closes ordinary task admission, drains existing I/O and
background work, and keeps cleanup admission open. `lance_runtime_stop` waits
for serial callbacks and the deferred-destruction fence while the reactor and
background workers remain alive. Doris then joins the reactor and shuts down
`LanceBackground`. No additional Lance cleanup thread pool is created.

## C ABI and lifetime

`lance_runtime_install_host_services_v1` installs process-owned dispatch before
SDK use. The host runs `lance_runtime_drive` on one service thread and calls
`lance_runtime_stop` after closing readers and sessions. The service context
must outlive all SDK calls and the reactor driver. Registration and shutdown
are process scoped; the driver rejects concurrent duplicate starts.

A submit callback consumes a task and must eventually invoke its run callback
or its discard callback exactly once. Process-service queue `0` is CPU, `1` is
blocking I/O, and `2` is deferred destruction. Queue `2` must remain admissible
through `lance_runtime_stop`, including when ordinary submissions are rejected. It must not execute work inline from a
wake callback. Dropping queued native jobs cancels their Rust tasks. The Tokio join
bridge reports cancellation instead of treating a discarded async-task Runnable as
a panic. Future destruction is deferred outside SDK wake locks. Query executors use weak
references in queued drivers so a host job cannot dereference a freed reader.

`LanceHostExecutorOptionsV1` supports retained query context, host submission,
and optional readiness-only notification. Without submission callbacks,
standalone callers can still drive the queues cooperatively with
`lance_host_executor_run`. Unregistered standalone SDK users retain the legacy
runtime for compatibility.

Enter and leave scopes on the same thread in LIFO order. Keep the same executor
throughout a live scanner or exported Arrow stream. Close SDK objects and leave
all scopes before freeing the executor. `ExecEnv::destroy` stops query work,
closes the shared Lance session, stops the Lance I/O token and background jobs,
and finally stops the reactor before releasing execution resources.

## Observability and validation

The scanner profile reports `LanceHostExecutorMode` (`host` or `disabled`) and
`LanceHostExecutorTasks`, the cumulative child-task poll count at the last
`get_block` call. Poll count is not a count of completed logical tasks.

Rebuild third-party artifacts before compiling against this ABI:

```sh
bash thirdparty/build-thirdparty.sh -j 8 lance_c
bash thirdparty/test/lance-prefilter-patch-test.sh thirdparty/src
```

The normal build entry point refreshes an older installed ABI. Patch-cache
regressions cover the C bindings, Rust SDK and Tokio source archives. The native
`thirdparty/test/lance-host-runtime-test.cpp` checks a real Arrow-stream scan
through host storage callbacks against a fixed host-thread count. The isolated SDK test
checks scans, filters, take, vector/full-text search and HTTP/DNS reads.
Storage regressions separately exercise HF backend initialization, a local Xet
streaming round trip, local uring URI reads, a real GooseFS page-cache round trip
and Hamming calculations against a fixed set of host threads. Callback tests cover
serial delivery, rejection and shutdown. Tokio hook tests verify external execution
and rejected task cancellation. The BE test compares rows across splits and early aborts.

This change does not add Rust allocation accounting or enforce a Doris query
memory limit. Host execution and allocator integration are separate work.
