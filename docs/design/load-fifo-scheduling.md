# Load FIFO and bitmap stage scheduling

Foreground load work shares the existing memtable flush workers in each resource
domain (the default pool or a workload-group pool). An outer FIFO schedules one
ready task per transaction turn. Within that transaction, the worker chooses the
highest nonempty stage queue:

| Priority | Work |
|---|---|
| P0 | Commit/publish bitmap computation and tablet finalization |
| P1 | Write-end bitmap reconciliation |
| P2 | Write-time segment bitmap work, including its existing file close |
| P3 | Memtable flush, including duplicate-key loads |

Stages do not compete across transactions. A duplicate-key load receives its
normal FIFO turn even while another transaction has commit bitmap work.

## Queue invariant

`LoadTaskQueue` contains a map from transaction ID to four FIFO queues, and an
outer FIFO of IDs. Only transactions with queued tasks exist in the map. All
submission, dispatch, and cancellation operations use `ThreadPool::_lock`.

On submission, insert the transaction in the outer FIFO only when its map entry
is first created (empty to nonempty). On dispatch, pop one transaction, take one
task from its highest priority queue, and immediately requeue the transaction if
it still has tasks; otherwise erase its map entry. Unlock before executing work.
There is no membership boolean and no per-load concurrency cap. Multiple workers
can execute the same transaction concurrently. The ordinary token API retains its
existing scheduling and concurrency semantics.

Cancellation removes only tasks belonging to the cancelled token. Other tokens
for the same transaction keep their tasks and FIFO position. Destroy removed
callbacks outside the pool lock. Writer/tablet token completion remains separate
from transaction scheduling, so waiting for one writer does not wait for a load.

FIFO provides dispatch fairness, not equal CPU time or preemption. A new load
waits for a currently occupied worker. A long tablet commit task can occupy a
worker longer than a flush task.

## Integration and dependencies

The separate load-bitmap, cloud tablet-bitmap, and high-priority flush pools are
removed. Background compaction/schema-change callers retain the general bitmap
executor. `TaskWP_CALC_DBM_TASK` and `SyncDeleteBitmapThreadPool` stay separate.

A flush enqueues its segment bitmap work at P2 and returns. Subsequent dispatches
for that transaction prefer bitmap work to additional flushes. With slow bitmap
computation, the shared workers drain fewer memtables; existing memtable memory
limits and write-side waits propagate pressure to ingestion. This change adds no
new bitmap-byte budget or admission controller.

A cloud P0 tablet task previously waited for child segment work. Scheduling those
children onto the same bounded worker pool could deadlock. A bitmap token created
inside a load worker therefore executes its callbacks synchronously, preserving
the existing tablet lock scope. The parent performs child calculations and
finalization in one P0 step. This reduces per-tablet segment parallelism, while
different tablets still run concurrently. Local publish waits outside the shared
pool and submits P0 segment work, including the single-segment case. Transient
publish writers also classify bitmap work as P0.

Normal writer tokens are created before flush runs, so their P2 submissions stay
asynchronous. During flush-worker cleanup, a writer may lose its last reference.
Cancelling its P2 token first removes queued work, then joins only already-running
leaf calculations, which do not wait for shared-pool work. A worker cannot join
its own token. Ordinary tokens still forbid same-pool shutdown/waits.

Bitmap tokens count submitted and completed callbacks to report cancellation if
pool shutdown discards work. Each callback captures its own ResourceContext.
Tokens retain the selected workload group until their underlying pool token is
released. A retry whose group has been dropped uses the default domain; a racing
shutdown is reported through submission/completion status.

## Transaction identity and resource domains

The scheduling key is `txn_id` within a pool, shared by every writer/tablet for
that transaction. Ordinary load retries preserve this key. Group commit uses its
merged backend transaction. Explicit multi-statement transactions schedule write
work under each subtransaction ID and commit work under the parent transaction
ID. This patch does not add a parent/subtransaction mapping to the write protocol,
so it does not claim parent-level fairness across simultaneous write statements.

The cloud transaction cache remembers the write-stage workload group. Commit
restores the first available local owner for the tablet, including subtransaction
cache entries. Missing/empty cache entries use the default domain. A commit that
combines subtransactions written in different groups is not split across groups.
No persistent metadata or wire format changes are introduced.

## Configuration and verification

Shared workers use existing flush sizing and adaptive controls. The default pool
is registered once with the adaptive controller. The removed pools' configuration
keys remain parseable but no longer size a separate pool:
`calc_delete_bitmap_for_load_max_thread`,
`calc_tablet_delete_bitmap_task_max_thread`, and
`high_priority_flush_thread_num_per_store`. `is_high_priority` no longer selects
a separate flush pool. `calc_delete_bitmap_max_thread` still sizes background
bitmap work. Existing queue/execution metrics and bitmap timing logs remain; this
patch adds no per-stage metrics.

Unit coverage is added for FIFO fairness, stage ordering, empty-load reactivation,
token cancellation isolation, multi-worker execution without a per-load cap,
nested bitmap execution, and cleanup. Compilation and test execution are skipped
at the requester's direction. Before rollout, run BE tests and concurrent MOW/DUP,
partial-update, row-binlog, cancellation, retry, and workload-group deletion tests.
