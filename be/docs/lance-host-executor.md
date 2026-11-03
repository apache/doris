# Lance host execution

`enable_lance_host_executor` defaults to `true`. Doris installs host services
before opening the shared Lance session or any query reader. Setting this
startup option to `false` provides a rollback path after restarting the BE.

In the Doris read path, Lance-C creates no Tokio worker or blocking-I/O threads, and
`spawn_cpu` does not use the SDK CPU pool. Tokio remains a library dependency:
a single `new_current_thread` reactor is driven on a Doris-owned service
thread. The execution resources belong to Doris; this is not a removal of
Tokio APIs from the Lance ecosystem.

## Execution resources

| Work | Doris execution resource |
| --- | --- |
| Query scan, decode and execution-plan stream polls | The query's `ScannerScheduler`, with cooperative help from the waiting caller |
| Blocking file, DNS and Foyer disk operations | A `ThreadPoolToken` on `ExecEnv::s3_file_system_thread_pool()` |
| Network readiness, timers and Tokio join/cancel notifications | One Doris `LanceReactor` service thread driving the threadless reactor |
| Shared background/cache CPU work without a query | A bounded Doris `LanceBackground` pool |

The query CPU path uses the existing scheduler and its workload-group limits.
Host jobs attach the query's `ResourceContext`. Jobs without a query, including
shared cache services, use the process services. Focused tests that do not
initialize `ExecEnv` use a Doris-owned fallback I/O pool.

The Rust SDK propagates the executor across spawned tasks and DataFusion plan
nodes. Each plan stream has a bounded handoff to a host-driven pump, so a
DataFusion control task does not execute a long filter or decode poll on the
reactor. The local Tokio patch redirects blocking submissions, including those
inside dependent libraries, instead of starting Tokio blocking workers.

## Reader flow

Each `LanceTableReader` keeps one executor across its splits. `prepare_split`,
`get_block`, `read_by_row_ids`, `abort_split` and `close` enter a thread-bound
executor scope. Schema discovery uses a temporary executor.

Ready work is queued in the reader's CPU or I/O queue and bounded drivers
are submitted to the host. Doris allows up to eight drivers per reader and
queue; the shared scheduler and I/O pool enforce the global execution limits. A driver runs a bounded number of polls before
resubmitting remaining work. The synchronous caller helps ready CPU children
while waiting. This guarantees progress even when every scanner worker is
waiting for children submitted to the same pool. I/O normally runs through
the shared I/O token; the caller can help if dispatch is unavailable.

When work is not ready, the caller parks until a completion or wakeup unparks
it. This retains the synchronous `TableReader` API while giving the host
control over the execution threads. A running CPU closure or blocking system
call cannot be preempted by cancellation.

## C ABI and lifetime

`lance_runtime_install_host_services_v1` installs process-owned dispatch before
SDK use. The host runs `lance_runtime_drive` on one service thread and calls
`lance_runtime_stop` after closing readers and sessions. The service context
must outlive all SDK calls and the reactor driver. Registration and shutdown
are process scoped; the driver rejects concurrent duplicate starts.

A submit callback consumes a task and must eventually invoke its run callback
or its discard callback exactly once. It must not execute work inline from a
wake callback. Dropping queued native jobs cancels their Rust tasks; future
destruction is deferred outside SDK wake locks. Query executors use weak
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
with Foyer enabled against a fixed host-thread count. The isolated SDK test
checks scans, filters, take, vector/full-text search and HTTP/DNS reads without
additional SDK threads. Tokio hook tests verify external execution and rejected
task cancellation. The BE test compares rows across splits and early aborts.

This change does not add Rust allocation accounting or enforce a Doris query
memory limit. Host execution and allocator integration are separate work.
