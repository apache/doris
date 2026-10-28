# Delete-sign retention for Unique Key tables with a sequence column

## Purpose and prerequisites

On shared-nothing Unique Key Merge-on-Write tables with a row-level sequence column, a row with `__DORIS_DELETE_SIGN__ = 1` remains invisible to queries but rejects late writes with a smaller sequence. Previously, base compaction could immediately remove this row, so the same late write could be accepted on a replica that had compacted and rejected on another replica.

The BE configuration `sequence_delete_sign_retention_version_gap` delays physical removal to cover bounded ingestion disorder. It requires:

- Sequence values for the same key increase in business event order, including deletes and reinserts. Different keys need not share a global counter. Equal sequences retain their existing semantics.
- Out-of-order arrival, queuing and retries finish within the configured version window.
- Replicas have broadly similar ingestion, publish and compaction progress. The window must cover their normal lag.
- Every BE uses the same configuration value, including newly added BEs.

This feature does not provide unconditional replica consistency. A long-unavailable replica, a replay beyond the window, or a deliberate historical reload with a smaller sequence can still produce different results. Use a greater sequence for a new recovery event, or rebuild the affected data, rather than waiting for compaction to accept an old sequence.

## Configuration and cleanup rule

`sequence_delete_sign_retention_version_gap` is a mutable, nonnegative `int64` BE configuration. Its default is `0`, preserving immediate pruning. A positive value enables retention for shared-nothing MOW tables with a row-level sequence column; MOR tables, tables without such a sequence column, and cloud mode retain their current behavior.

Base, full and cold compaction keep a delete-sign row until:

```text
local maximum published tablet version - original row publish version >= configured gap
```

The local maximum and configuration are captured when the compaction reader initializes. Compaction preserves the existing hidden `__DORIS_VERSION_COL__`; merging files does not reset a row's age. Cumulative and segment compaction keep their existing delete-sign behavior. Ordinary queries and checksums continue to exclude delete signs.

Reaching the gap only makes the row eligible for removal. It remains until a compaction covering it succeeds, and continues rejecting lower-sequence writes while physically present. Higher-sequence updates may supersede it normally; retention does not preserve every historical delete. For legacy schemas without a version column, or stored rows whose version is an unknown placeholder (`<= 0`), positive retention conservatively keeps the delete sign.

The test-only switch `enable_prune_delete_sign_when_base_compaction = false` still disables delete-sign pruning for base/full compaction entirely.

## Sizing the gap

Use the target tablet's published-version growth, rather than row count or compaction count. For example, at 10 versions/minute, retaining for roughly 24 hours suggests:

```ini
sequence_delete_sign_retention_version_gap = 14400
```

If the peak is 20 versions/minute and the same duration is needed at that peak, start from `20 * 60 * 24 = 28800`, then allow for retries and replica lag. This is a version gap, not a wall-clock TTL: a faster tablet expires rows sooner, and an idle tablet never expires them merely because time passes. Actual removal also waits for compaction. Retention consumes disk and scan resources without a fixed space bound.

## Upgrade and configuration changes

No new storage or wire format is introduced. Upgrade all BEs before enabling positive retention: old BEs can still prune immediately. Set the same value on every BE and account for compactions already running with the previous value before relying on the new window. Increasing the value cannot recover delete signs that have already been removed. Disabling retention or downgrading restores the old pruning behavior and ends the protection.
