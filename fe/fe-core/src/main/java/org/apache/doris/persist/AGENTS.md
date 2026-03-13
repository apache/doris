# EditLog / Persistence — Review Guide

## Write and Replay

- [ ] Every metadata state change has both a write-side EditLog call and matching replay in `EditLog.loadJournal()`?
- [ ] Replay logic equivalent to master-side transition, not approximate reconstruction?
- [ ] New persisted objects have complete Gson annotations and image/checkpoint coverage?
- [ ] Master failover at any point avoids duplicate state, missing state, and leaked txn metadata?

## Transaction EditLog Modes

`DatabaseTransactionMgr` supports inside-lock and outside-lock persistence.

- [ ] Both modes kept correct for transaction changes?
- [ ] PREPARE transactions from non-`FRONTEND` sources intentionally not journaled?
- [ ] Outside-lock mode: all in-memory mutations complete before `submitEdit()`, since other threads observe new state immediately?
- [ ] `awaitTransactionState()` kept outside write lock? (Unbounded wait, not timeout-based)

## Fatal Errors

- [ ] EditLog flush failures not swallowed? FE exits on durable-log failure to prevent split brain
