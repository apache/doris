# BE Exec Module — Review Guide

Review changes by their owning module first, then apply the cross-cutting checks. Add new recurring
failure patterns under the corresponding module instead of building one flat checklist.

## Cross-Cutting Execution Rules

### Operator Lifecycle

Tasks move through `INITED → RUNNABLE → BLOCKED → FINISHED → FINALIZED`.

- [ ] `SharedState` source/sink dependencies connected through `inject_shared_state()`?

### Memory Reservation and Spill

- [ ] Memory-heavy operators use `_try_to_reserve_memory()` before materializing large structures?
- [ ] `_memory_sufficient_dependency` wired in where pressure should block, not overrun?
- [ ] `revoke_memory()` preserves the existing spill path?

### Dependency Concurrency

- [ ] Default readiness preserved? Source starts blocked; sink starts ready
- [ ] `set_ready()` fast-path precheck vs `is_blocked_by()` lock-first asymmetry respected?
- [ ] New `Dependency` subclasses pair `block()` / `set_ready()` on every path?
- [ ] `CountedFinishDependency::add()` and `sub()` under `_mtx`?

### Atomics

- [ ] Relaxed atomics only for statistics; lifecycle/stop flags use at least acquire/release?

### Asynchronous BRPC Lifecycle

Async RPC completion callbacks may synchronously start another RPC. Review them as reentrant code,
even when the surrounding queue permits only one network request to be in flight.

#### Checkpoints

- [ ] Request, response, and `brpc::Controller` remain alive until brpc invokes the completion closure?
- [ ] `AutoReleaseClosure` has an external callback owner while the callback result is still needed?
      Do not replace its `weak_ptr` with strong ownership that can retain `QueryContext` and other
      large query-scoped objects after cancellation.
- [ ] A callback that starts RPC-B avoids resetting or overwriting RPC-A's callback, Controller,
      response, or `std::function` target while callback-A is still on the stack? Prefer per-RPC
      callback state when reentrant sends are possible.
- [ ] `Controller::Reset()` is called only after the previous RPC and all code reading its result
      have finished?
- [ ] Completion releases `request_attachment()` promptly? Runtime-filter attachments can contain
      large serialized Bloom filters. If a callback can start another RPC with the same Controller,
      clear the old attachment before invoking the callback, never after it.
- [ ] Failure before the RPC is submitted releases any attachment and does not leave a callback
      owner retaining its Controller indefinitely?
- [ ] Transport failure, response application status, timeout, and special statuses such as
      `END_OF_FILE` preserve the intended cancellation and cleanup semantics?

## Exchange Module

Apply these checks to remote exchange, local exchange, broadcast paths, `ExchangeSinkBuffer`,
`Channel`, `VDataStreamRecvr`, and every `SenderQueue` implementation.

### EOF and Termination

- [ ] When the peer returns `END_OF_FILE`, does the sender stop or turn off the correct channel and
      release queued regular and broadcast blocks without treating a normal receiver close as an
      unrelated transport failure?
- [ ] Does every EOF path signal all affected dependencies? Check sink queue dependencies, source
      dependencies, local-channel dependencies, broadcast dependencies, and finish dependencies;
      missing one signal can leave the query blocked forever.
- [ ] Do RPC failure, response-status failure, callback-owner expiration, cancellation, and close
      leave `rpc_channel_is_idle` and channel turn-off state consistent, so no queue waits for a
      callback that will never run?
- [ ] Does receiver-side EOS decrement the correct sender exactly once, handle duplicate or stale
      packets safely, and make the source ready when the final sender finishes?
- [ ] Can a receiver created with zero senders, or closed before dependency wiring, still make its
      source dependency ready instead of waiting forever?

### Sender-Side Memory and Backpressure

- [ ] Is memory growth bounded for regular, broadcast, local, and multi-block queues? Check both
      byte limits and block/count limits; do not rely on eventual RPC completion as the only bound.
- [ ] Is every enqueue/accounting or holder `acquire()` paired with a dequeue, drop, EOF, failure,
      cancellation, or teardown release on all exits?
- [ ] When consumption drops below the configured limit, is the corresponding upstream dependency
      made ready promptly? Do not wait for the whole queue to become empty unless that hysteresis is
      intentional and documented.
- [ ] Are `_total_queue_size`, memory counters, queue capacities, and
      `BroadcastPBlockHolderMemLimiter` updated under the correct lock without underflow or double
      release when one broadcast block is shared by multiple channels?
- [ ] After a receiver EOF or RPC failure discards buffered data, are both memory accounting and
      upstream backpressure released?

### Stream Receiver Memory and Backpressure

- [ ] Does each `SenderQueue` enforce `_sender_queue_mem_limit` and the receiver-wide exchange
      buffer limit for both serialized remote `PBlock`s and local `Block`s?
- [ ] When a remote queue exceeds its limit and retains the RPC `done` callback as backpressure, is
      that callback invoked exactly once after consumption, cancellation, close, destruction, or a
      deserialize/error path? A missed callback leaves the remote sender permanently blocked.
- [ ] Are `add_blocks_memory_usage()` and `sub_blocks_memory_usage()` balanced on normal reads and
      every failure path, and does dropping below the limit promptly set the local-channel
      dependency ready?
- [ ] Does `_source_dependency` become ready when data arrives, the last sender reaches EOS, or the
      queue is cancelled/closed, and become blocked only when the queue is empty with live senders?
- [ ] For merging receivers with one queue per sender, are memory limits and dependency signals
      correct per queue and in aggregate, without one quiet sender stalling ready senders?

### Exchange RPC Callbacks

- [ ] A completion callback that sends the next queued packet uses separate per-RPC callback,
      Controller, response, and handler state?
- [ ] The callback remains alive until its result is handled without making an in-flight RPC retain
      the whole query after cancellation?
- [ ] Normal and broadcast branches implement the same failure, EOF, queue-accounting, and
      dependency-release behavior?

## Runtime Filter Module

Runtime-filter producers, local mergers, and the global merge node coordinate across parallel
instances and recursive CTE stages. A callback often represents only the producer that sent the
RPC, not every producer waiting for the result.

### Locking and Deadlock Prevention

Manager, producer, merger, dependency, query-cancellation, and RPC callback code execute on
different threads and can call back into one another. A `recursive_mutex` prevents only same-thread
self-deadlock; it does not prevent cross-thread lock inversion.

- [ ] Is there one documented lock order among the manager map lock, `GlobalMergeContext::mtx`,
      producer/consumer `_rmtx`, merger locks, and dependency locks? New code must not introduce a
      reverse acquisition path.
- [ ] Does manager code release its map/context lock before calling a producer or consumer method
      that takes `_rmtx`, if producer/consumer code can call back into the manager? Review the full
      call graph, not only the locks visible in the changed function.
- [ ] Does producer code avoid holding `_rmtx` while entering manager/merger code when another path
      can hold a manager/merger lock and call `set_synced_size()`, `signal()`, `publish()`, or another
      producer method?
- [ ] Is any synchronous RPC, `brpc::Join()`, dependency wait, DNS/stub lookup, or other blocking
      operation performed while holding a lock needed by its completion callback? The callback
      cannot signal completion if it must acquire the lock held by the waiting thread.
- [ ] Are query cancellation, dependency `sub()`/`set_ready()`/`set_always_ready()`, RPC callbacks,
      and destruction of callback/controller owners invoked outside manager and producer locks when
      they may wake tasks, run cleanup, or re-enter runtime-filter code?
- [ ] When avoiding a race by extending a lock across merge/publish/reset, does the protected region
      avoid calling code that can synchronously re-enter the same context or acquire locks in the
      opposite order? Prefer taking an immutable stage snapshot or per-RPC owner when practical.
- [ ] Do all early returns and exceptions release locks before waiting for cleanup that needs those
      locks, and are `Defer` actions reviewed for what they execute while local lock guards exist?

A classic inversion to reject during review is:

```text
Thread A: Producer::_rmtx  -> RuntimeFilterMgr lock
Thread B: RuntimeFilterMgr lock -> producer->set_synced_size() -> Producer::_rmtx
```

Another common callback deadlock is:

```text
Thread A: holds GlobalMergeContext::mtx -> waits for RPC/dependency completion
Thread B: RPC completion callback       -> needs GlobalMergeContext::mtx to signal completion
```

### Producer and Dependency Coordination

- [ ] Every `CountedFinishDependency::add()` has exactly one matching `sub()` or a query-cancel path
      that unblocks all affected dependencies? Check all parallel producers, not only the final
      producer that sends the size RPC.
- [ ] Error and `END_OF_FILE` handling cannot release only the sender's dependency while sibling
      producer dependencies remain blocked?
- [ ] Runtime-filter RPC timeouts use the intended timeout domain (runtime-filter wait versus query
      execution timeout), with the choice documented?

### Merge and Recursive CTE Stages

- [ ] Recursive-stage validation and the operation it protects are atomic with respect to reset?
      Do not check `stage` under `GlobalMergeContext::mtx`, unlock, and then read or mutate the same
      stage's merger, `done`, targets, or callback storage.
- [ ] `publish_callbacks` and `sync_size_callbacks` resize/write/clear operations are synchronized
      with recursive reset, and an old stage cannot repopulate a new stage's callback owners?
