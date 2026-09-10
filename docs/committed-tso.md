# Committed TSO for bounded incremental reads

In cloud mode, a strongly consistent `@incr` query with an explicit `endTimestamp` on every incremental relation uses the master FE's durable committed TSO as a boundary for reads that require no transaction wait. Transactions with commit TSO at or below this prefix are truly visible or aborted. The name does not refer to the intermediate `COMMITTED` transaction state.

```sql
SELECT CURRENT_TSO_PHYSICAL_TIME, COMMITTED_TSO, COMMITTED_TSO_PHYSICAL_TIME
FROM information_schema.tso_status;
```

The committed columns are nullable BIGINTs. `COMMITTED_TSO_PHYSICAL_TIME` is Unix epoch milliseconds and gives an end timestamp guaranteed to require no transaction wait across all tables. Larger ends can also be readable after checking only the relevant tables, up to `CURRENT_TSO_PHYSICAL_TIME`; the full encoded `COMMITTED_TSO` must not be passed as an `endTimestamp` string. Convert milliseconds to the timestamp format and time zone accepted by `@incr`; clients producing whole-second windows must round down. The interval remains `[start, end)`. Existing binlog retention and table requirements still apply.

The system table reads the master's persisted prefix without allocating a TSO. While a new master recovers, it keeps exposing the previous persisted prefix. A first startup, an old image without a prefix, or classic mode returns NULL for the new columns. Existing disabled/uninitialized TSO errors remain unchanged.

The master handles a bounded window in three ways:

| Requested end | Behavior |
| --- | --- |
| After the current TSO's physical time | Immediately return MySQL error 5100, `ERR_INCR_WINDOW_NOT_READY`, with reason `END_AFTER_CURRENT_TSO`. Later allocations could still fall in this window. |
| At or before the durable committed TSO's physical time | Proceed without waiting for transactions. |
| Between those boundaries | After recovery, wait only for registered transactions involving the queried tables whose earliest possible commit TSO falls before the physical end. An empty matching set proceeds immediately. Exceeding `change_visible_timeout_ms` returns error 5101, `ERR_INCR_VISIBLE_WAIT_TIMEOUT`. |

The error message includes `reason`, `requestedEndTimestampMs`, `currentTSO`, `currentTSOPhysicalTimeMs`, `committedTSO`, `committedTSOPhysicalTimeMs`, `retryAfterMs`, and `timeoutMs`. Master-to-follower RPC preserves both classifications and allows one additional second for the typed wait-timeout response to arrive. Arrow Flight SQL returns UNAVAILABLE for both errors, with their respective `doris-error-code` and `doris-error-name` metadata. Other exceptions keep their existing wrapping. Clients should retry the same split/window/offset after a cancellable delay, and advance offsets only after that window completes. Shortening a refused window or treating it as an empty success can lose data. This Doris change does not implement a Connector's retry loop.

Bounded reads skip the transaction-ID watermark and table-wide conflict polling. The allocator lock protects the clock and the fixed set of matching registrations; waiting releases this lock and terminal notifications wake the readers. A successful table-specific wait does not advance the global durable prefix or require its next journal write. Queries with different ends on multiple incremental relations use their maximum end for the matching table set, which can conservatively wait longer. Cloud partition visible versions are still refreshed from MetaService. Classic reads, eventual consistency, unbounded reads, and queries mixing bounded and unbounded incremental relations retain the existing behavior.

## Allocation and persistence

The allocator registers transaction identity, the involved table IDs, and its first commit TSO under the same lock that advances its clock. Retries retain the earliest registration and the union of involved table IDs until a real terminal result is known. Bitmap preparation, callbacks, and commit metadata validation precede allocation; one request reuses the same TSO for RPC retries. A lazy commit response marked incomplete cannot release a registration even if its returned transaction status says VISIBLE. A separate worker reconciles at most 64 old registrations per cycle; missing/error responses do not release them.

After recovery, the next candidate prefix is the current allocated TSO when no registrations remain, otherwise the smaller of that TSO and the oldest pending TSO minus one. The candidate is published only after its journal write succeeds. The reservation window and committed prefix share one journal record and one immutable persisted snapshot. `tso_service_window_duration_ms` defaults to 1000 ms. A monotonic timer also persists the prefix when the reservation window does not move. This is approximately one combined journal write per second, compared with the previous five-second window renewal; it does not reduce total journal frequency relative to the old implementation.

## Recovery and upgrades

A new master calibrates beyond the previous reserved window, registers new allocations, and waits `tso_service_window_duration_ms + 1000` milliseconds before taking a fixed exclusive transaction-ID bound from MetaService. An instance-wide strict check must then find no running transaction below that bound. The check covers every database/table and never skips expired running transactions. An expired lazy transaction can still await real publication. Normal pending registrations continue to constrain the prefix after the recovery check succeeds. Before recovery completes, reads above the durable prefix return 5100 with reason `TSO_RECOVERING`; an empty new-master registration set does not prove old transactions are visible. Reinitialization invalidates any existing read wait.

Upgrade MetaService before using the new FE's committed prefix: recovery requires an explicit acknowledgement of the strict-check option. Old journal/image records remain readable and imply an unknown prefix. The new window-error fields in the FE RPC are optional: new followers treat an absent business code as 5100; older followers classify a new master's 5101 as the original retryable 5100 until upgraded. Old BE requests without column names retain the original four-column system-table response; new BE requests explicitly name all six columns. A new BE cannot obtain the new columns from an old FE.

The fixed recovery wait is a temporary operational assumption, not a fencing protocol. It cannot prevent an old master that continues allocating after the wait from assigning an old TSO outside the captured transaction bound. Clock skew, an old longer reservation window, or long process pauses can violate that assumption. This change deliberately retains that accepted limitation. Recovery can also wait for long-running old transactions, and the oldest pending transaction can delay the global prefix across unrelated tables. After recovery, that delay no longer blocks reads of unrelated tables whose requested end is at or before the current TSO physical time.

## Diagnosis

FE metrics expose the committed prefix, reserved window, pending count, oldest pending TSO/transaction/age, recovery readiness and transaction bound. TSO persistence and reconciliation have counters and latency histograms. Reconciliation failures preserve the watermark and produce a rate-limited warning. Unknown transaction status must be investigated; registrations are not discarded by TTL.
