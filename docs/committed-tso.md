# Committed TSO for bounded incremental reads

In cloud mode, a strongly consistent `@incr` query with an explicit `endTimestamp` on every incremental relation uses the master FE's durable committed TSO. Transactions with commit TSO at or below this prefix are truly visible or aborted. The name does not refer to the intermediate `COMMITTED` transaction state.

```sql
SELECT COMMITTED_TSO, COMMITTED_TSO_PHYSICAL_TIME
FROM information_schema.tso_status;
```

Both columns are nullable BIGINTs. `COMMITTED_TSO_PHYSICAL_TIME` is Unix epoch milliseconds and gives the maximum allowed end timestamp; the full encoded `COMMITTED_TSO` must not be passed as an `endTimestamp` string. Convert milliseconds to the timestamp format and time zone accepted by `@incr`; clients producing whole-second windows must round down. The interval remains `[start, end)`. Existing binlog retention and table requirements still apply.

The system table reads the master's persisted prefix without allocating a TSO. While a new master recovers, it keeps exposing the previous persisted prefix. A first startup, an old image without a prefix, or classic mode returns NULL for the new columns. Existing disabled/uninitialized TSO errors remain unchanged.

If the requested end exceeds the prefix, the query immediately returns MySQL error 5100, `ERR_INCR_WINDOW_NOT_READY`. Its message includes `requestedEndTimestampMs`, `committedTSO`, `committedTSOPhysicalTimeMs`, and `retryAfterMs`. Master-to-follower RPC preserves this classification. Arrow Flight SQL returns UNAVAILABLE with metadata `doris-error-code=5100` and `doris-error-name=ERR_INCR_WINDOW_NOT_READY`; the description preserves the window and retry details. Clients should retry the same split/window/offset after a cancellable delay, and advance offsets only after that window completes. Shortening a refused window or treating it as an empty success can lose data. This Doris change does not implement a Connector's retry loop.

For accepted windows, planning skips the transaction watermark and conflict polling. Cloud partition visible versions are still refreshed from MetaService. Classic reads, eventual consistency, unbounded reads, and queries mixing bounded and unbounded incremental relations retain the existing behavior.

## Allocation and persistence

The allocator registers transaction identity and its first commit TSO under the same lock that advances its clock. Retries retain the earliest registration until a real terminal result is known. Bitmap preparation, callbacks, and commit metadata validation precede allocation; one request reuses the same TSO for RPC retries. A lazy commit response marked incomplete cannot release a registration even if its returned transaction status says VISIBLE. A separate worker reconciles at most 64 old registrations per cycle; missing/error responses do not release them.

After recovery, the next candidate prefix is the current allocated TSO when no registrations remain, otherwise the smaller of that TSO and the oldest pending TSO minus one. The candidate is published only after its journal write succeeds. The reservation window and committed prefix share one journal record and one immutable persisted snapshot. `tso_service_window_duration_ms` defaults to 1000 ms. A monotonic timer also persists the prefix when the reservation window does not move. This is approximately one combined journal write per second, compared with the previous five-second window renewal; it does not reduce total journal frequency relative to the old implementation.

## Recovery and upgrades

A new master calibrates beyond the previous reserved window, registers new allocations, and waits `tso_service_window_duration_ms + 1000` milliseconds before taking a fixed exclusive transaction-ID bound from MetaService. An instance-wide strict check must then find no running transaction below that bound. The check covers every database/table and never skips expired running transactions. An expired lazy transaction can still await real publication. Normal pending registrations continue to constrain the prefix after the recovery check succeeds.

Upgrade MetaService before using the new FE's committed prefix: recovery requires an explicit acknowledgement of the strict-check option. Old journal/image records remain readable and imply an unknown prefix. Old BE requests without column names retain the original four-column system-table response; new BE requests explicitly name all six columns. A new BE cannot obtain the new columns from an old FE.

The fixed recovery wait is a temporary operational assumption, not a fencing protocol. It cannot prevent an old master that continues allocating after the wait from assigning an old TSO outside the captured transaction bound. Clock skew, an old longer reservation window, or long process pauses can violate that assumption. This change deliberately retains that accepted limitation. Recovery can also wait for long-running old transactions, and any oldest pending transaction can delay the global prefix across unrelated tables.

## Diagnosis

FE metrics expose the committed prefix, reserved window, pending count, oldest pending TSO/transaction/age, recovery readiness and transaction bound. TSO persistence and reconciliation have counters and latency histograms. Reconciliation failures preserve the watermark and produce a rate-limited warning. Unknown transaction status must be investigated; registrations are not discarded by TTL.
