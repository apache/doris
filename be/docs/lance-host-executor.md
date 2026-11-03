# Lance host executor

`enable_lance_host_executor=true` opts a BE into cooperative execution of
operation-owned Lance scan and decoder tasks. The default is `false`.

Rebuild the Lance-C third-party artifacts before compiling against the new ABI:

```sh
bash thirdparty/build-thirdparty.sh -j 8 lance_c
```

`build.sh` refreshes Lance-C automatically when the installed header predates
this interface. Prebuilt compilation images must include the patched artifacts.

Each `LanceTableReader` owns one executor across its splits. The executor has
queues but no worker threads. `prepare_split`, `get_block`, `read_by_row_ids`,
`abort_split` and `close` enter a thread-bound Lance-C executor scope. Schema
discovery uses a temporary executor with the same lifetime rules.

During a synchronous Lance-C call, the calling scanner thread polls the root
future and executes ready child tasks from that reader's queues. When no work
is ready, it parks until an I/O completion or a task wakeup unparks it. This
avoids a deadlock in which every scanner thread waits for child work submitted
to the same saturated pool. It also avoids a second CPU pool for these tasks.

The Rust SDK patch propagates the operation context through task polls and
DataFusion execution-plan streams. It redirects operation-owned scan/decoder
spawns and `spawn_cpu`; Tokio remains the reactor for network readiness and
timers, and runs join notifications and shared background I/O services.
Third-party DataFusion operators and unrelated SDK background services are
not a promise of complete host scheduling. This change does not add Rust
allocation accounting or enforce a Doris query memory limit.

## C API

`LanceHostExecutorOptionsV1` includes an ABI version, structure size, an optional
retained callback context and an optional readiness callback. The callback only
notifies the host that queue 0 (CPU) or queue 1 (blocking I/O) has ready work;
it must not block, throw, reenter the SDK or execute tasks inline. A host with
its own asynchronous integration can schedule `lance_host_executor_run` on
appropriate workers. Blocking I/O must not occupy a bounded CPU pool.

Doris currently passes a null readiness callback and uses cooperative caller
execution. It does not submit nested work to `ScannerScheduler`. A future
asynchronous reader can use the readiness callback without changing the ABI.

Enter and leave scopes on the same thread, in LIFO order. Keep one executor
for the entire lifetime of live scanners and exported streams; do not change
executors midway through a stream. Close scanners, streams and dataset handles,
leave all scopes, and stop host callbacks before freeing the executor. Closing
retires queued work; pending future destruction and foreign-context release
may complete on the Tokio control runtime. Context release callbacks must be
thread safe and must not require a particular caller thread.

## Observability and tradeoffs

The scanner profile reports `LanceHostExecutorMode` (`cooperative` or `disabled`)
and `LanceHostExecutorTasks`, the cumulative number of child-task polls completed
by the reader at the last `get_block` call. A poll is not necessarily a complete
logical task. The counter can show that a scan reached the injected path.

Cooperative execution bounds these child tasks by the active Doris scanner
threads. It may reduce CPU parallelism for a single split, so enabling the flag
requires workload-specific latency and throughput measurements. Cancellation
still follows the existing synchronous reader boundaries; it does not interrupt
a CPU closure or an in-flight blocking system call.

## Validation

The patch includes Rust tests for nested tasks, context restoration, panic and
abort behavior, and callback-context lifetime. The BE test compares rows with
the feature disabled and enabled across fragment changes and early aborts.

Run the source-cache regression with both pinned archives available:

```sh
bash thirdparty/test/lance-prefilter-patch-test.sh thirdparty/src
```

It verifies fresh extraction, repeated application, Foyer-only upgrades, stale
C/Rust patch fingerprints and failed-patch markers. The existing Foyer and
upstream prewarm APIs remain covered by the same test.
