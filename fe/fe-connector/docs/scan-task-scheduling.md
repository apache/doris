# Cooperative external scan scheduling

A statement obtains its stage schedulers through
`ConnectorSession.getTaskScheduler`. The engine owns the task groups and closes
them with the statement. Connectors submit neutral `Runnable` tasks through this
SPI; they do not import FE executor classes.

The two stages have independent workers:

- `SPLIT` advances scan producers and assigns ranges to the BE split queues.
- `METADATA` performs independent operations such as cold Manifest reads.

A producer may need metadata tasks to finish. Keeping those tasks out of the
producer pool avoids a pool full of parents waiting for children queued behind
them. A task must not block waiting for children submitted to its own executor.

## Admission and continuation

Each executor has a concurrent FIFO ready queue containing task groups. A
worker claims a task from the next group, republishes that group if it still
has pending work, and then executes the task outside queue synchronization.
This allows several workers to serve one query and gives other ready queries
an opportunity at subsequent task boundaries. It does not preempt a running
task or guarantee equal CPU time.

`tryExecute` never waits for space. A full group queue registers the supplied
`onSpace` callback and returns `false`; the caller retains the task until it
can retry. Reuse the same callback identity and remove it when the task is
cancelled or no longer needs admission. Failures are delivered through the
failure callback rather than silently dropping work.

A serial producer can use `tryExecuteContinuation` for bounded further work.
The group still returns behind other ready groups, but a worker need not wake
another worker just to execute its own serial continuation. Initial tasks,
external admission and recovery from backpressure retain the normal wakeup
protocol. Split production yields after at most 16 source batches or after
observing 1 ms elapsed between batches. One blocking batch can exceed that
interval; the bound is cooperative.

## Iceberg Manifest reads

Cold Manifest prefetch uses a bounded window and a statement-wide read budget.
Within that window, completed reads can be consumed before a slower earlier
Manifest. First-split production does not wait for speculative prefetch when
one Manifest already supplies output; subsequent BE demand enables further
prefetch. Filtered or rewrite-scoped scans must still advance while no output
has been found.

Closing a scan stops new submissions and wakes completion waiters. Load failures
propagate through the corresponding future. A running shared cache load is not
interrupted just because one waiting scan stops. Authentication and plugin
thread-context class loading remain scoped around each actual metadata read.
A scheduled reader that finds another query already loading the same Manifest
observes that shared completion and returns its metadata worker immediately.
The completion callback only publishes the bounded window entry; it does not
read another Manifest or run split production on the loading query's thread.
Returned asynchronous tasks remain owned by their task group until terminal
completion, so closing the group still wakes the waiting producer. Closing a
scan detaches its observer without cancelling the shared cache load. Cache
publication continues to use the original scope/key generation fence.
Cached partition values stay immutable: adapt a file task to the pinned scan
schema at the scan boundary. Select the format from each file range rather
than planning files merely to infer a table-wide format.

The captured scheduler exposes its closed state even after query cleanup removes
the statement registry. A Manifest reader waiting for another scan's read permit
checks this state at most 100 ms apart and rechecks after admission. Cancellation
therefore stops that waiter without releasing another scan's permit or starting
another remote read. This does not interrupt an already running shared cache load.
Hive checks the same captured state between directory requests, including when
empty partitions are skipped within one iterator call.

## Configuration and limits

| Setting | Default | Meaning |
|---|---:|---|
| FE `external_scan_split_threads` | 64 | Worker threads in the producer scheduler |
| FE `external_scan_metadata_threads` | 64 | Worker threads in the metadata scheduler |
| FE `external_scan_task_queue_capacity` | 128 | Pending tasks per query in each stage |
| Session `iceberg_manifest_load_concurrency` | 8 | Manifest read budget per query and window bound per scan; 1 uses sequential loading |

These task queues are distinct from `split_queue_capacity`, which bounds the
produced Split buffer. Queue capacity does not reserve that many running
threads. Worker counts limit the respective pools, and the query read budget
also covers caller-assisted Manifest loading.

Workers are ordinary threads. Synchronous remote calls occupy workers until
they return; FIFO admission cannot recover a worker blocked inside an SDK.
Effective remote timeouts and dependency fault isolation are separate concerns.

Planning-only attempts also run their query-finish callbacks when statement
execution returns. This releases batch Split producers and metadata leases for
EXPLAIN and planning failures even when no coordinator was registered. Normal
coordinator unregistration remains idempotent. Deferred Arrow Flight queries keep
those callbacks until their retained executor finalizes the query after consumption.

Backend assignment has its own policy-state lock. First-split readiness and stop
notifications use a separate short-held lock, so an already converted range can
wake the planner while another lane is still assigning its batch. The policy's
mutable load-balancing state remains serialized.

The engine captures task-group cancellation on the statement executor before
asynchronous work starts. KILL and timeout close those groups even before a
coordinator exists; a group captured after cancellation is immediately closed.
Cancellation listeners run outside their registry operation and are claimed once.
This stops queued work and wakes future waiters without interrupting a running
shared cache loader. Normal completion retains the separate read-lease cleanup.

### Consumer completion notification

Each backend has a bounded split queue and its own data-ready wait channel. A consumer registers before checking queue contents and terminal state, then waits only when neither is available. Successful enqueue, producer completion, failure and cancellation signal that channel. The empty queue is not periodically polled to discover EOF: otherwise finishing just after a poll begins adds 100 ms despite completed production. Producers avoid the channel monitor when no consumer waits; callbacks that resume producers run after releasing the consumer channel monitor. Cancellation signals consumers before closing SDK resources.

### Initial delivery and producer lookahead

`split_initial_batch_size` (default 64) bounds the first Split fetch per backend, also capped by the BE request size. Subsequent fetches use the BE request size. The bound is applied before compatible file ranges are compacted into RPC envelopes. It is independent of `split_batch_size` (producer batch size) and `split_queue_capacity` (buffered batches per backend). Producers continue listing files and preparing splits before BE consumption, yielding only for task fairness and configured queue backpressure. A small initial RPC therefore does not suspend useful directory prefetch while BE starts.

Actual Manifest readers retain a lease on the exact read-table generation until the callable exits. Query cleanup releases the statement owner, while an already running shared reader keeps its FileIO alive. Future cancellation is not evidence that the callable has finished.

## Batch mode switch

`enable_external_table_batch_mode` governs both partition-batch and file-streaming
planning. The engine checks it before calling provider capability or file-count
estimation methods. Setting it to `false` selects synchronous split planning for
connectors that otherwise support batch mode. Connectors without a batch or
streaming implementation keep their synchronous path regardless of this switch.
