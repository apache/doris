# FE Core Module — Review Guide

## Locking

- [ ] Metadata-locking paths avoid RPC, external IO, and journal waits while holding catalog/database/table locks?
- [ ] Multiple tables locked in ID-sorted order?
- [ ] Existing `tryLock(timeout)` patterns preserved, not replaced with unbounded blocking?
- [ ] No database/catalog locks taken inside broad `synchronized` blocks or async callbacks?
- [ ] Shared `Map`/`List` traversals protected by locking, snapshots, or concurrent containers against `ConcurrentModificationException`?

## Exceptions

- [ ] FE-common `AnalysisException` (checked) vs Nereids' `AnalysisException` (unchecked) distinguished correctly?
- [ ] User-visible errors use `ErrorReport`/`ErrorCode` with custom codes starting at 5000?
- [ ] RPC boundaries convert to `TStatusCode`/`PStatus`, not leaking Java exceptions?

## Visible Version

- [ ] `OlapTable.getVisibleVersion()` respects cloud/non-cloud split: local in shared-nothing, RPC+TTL cache in cloud?
- [ ] Cloud `VERSION_NOT_FOUND` normalized to `PARTITION_INIT_VERSION` — "missing" and version 1 intentionally indistinguishable?
