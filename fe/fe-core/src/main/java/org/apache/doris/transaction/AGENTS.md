# Transaction Management — Review Guide

## Lifecycle

State machine: `PREPARE → COMMITTED → VISIBLE` or `ABORTED`.

- [ ] `PublishVersionDaemon` builds and sends publish work to every relevant backend?
- [ ] Publish-finish semantics preserved (more complex than simple quorum check)?
- [ ] Timeout/abort paths release state cleanly, no partially visible metadata left?
- [ ] `TransactionState.tableIdList` (unsynchronized `ArrayList`) mutated only under external serialization?
- [ ] Multi-table local transaction paths use ID-sorted locking?

## Cloud Mode

- [ ] Cloud paths use `CloudGlobalTransactionMgr` through interface methods, not concrete-type assumptions?
- [ ] No unsafe downcasts from `GlobalTransactionMgrIface`?
