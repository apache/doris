# FIX-L17 — iceberg same-table multi-version version-blind schema binding (fail-loud guard)

## Problem (HEAD-verified)
`PluginDrivenMvccExternalTable.getSchemaCacheValue()` (`:485`) binds the table's schema version-BLIND via
`MvccUtil.getSnapshotFromContext(this)` (`StatementContext.getSnapshot(TableIf):1015` — returns the default
pin if present, else the lone pin, else EMPTY when 2+ same-table pins exist with no default). Binding is
per-reference (`BindRelation.getLogicalPlan:739` pins the reference's snapshot, then binds its columns via
`getFullSchema → getSchemaCacheValue`; `ExternalTable.getFullSchema:176` re-evaluates fresh each call — no
freeze). Meanwhile the DATA path is version-AWARE (`PluginDrivenScanNode.pinMvccSnapshot:869` uses the
3-arg `getSnapshotFromContext(table, tableSnapshot, scanParams)`).

For a self-join `t FOR VERSION AS OF v1 a JOIN t FOR VERSION AS OF v2 b` across a schema change, ref `b`
binds while 2 pins exist → EMPTY → LATEST schema, but its scan reads v2 → **skew**: the FE tuple carries
latest columns/field-ids while BE reads v2 files → field-id/name mismatch (crash / wrong NULLs). Narrow,
low severity, dominantly fail-loud. **Newly possible in this branch** (pre-P6 the table-only MVCC key
collapsed a self-join to one self-consistent snapshot; this branch made the data path per-reference
version-aware while schema binding stayed version-blind).

## Why NOT the analysis-time (getSchemaCacheValue) guard — red-team `wf_f7b69cf7-ec8`
An analysis-time guard (detect 2+ divergent pins in `getSchemaCacheValue`, throw) is **order-dependent and
holey** — all 3 lenses flagged it:
- **Masking (BLOCKER):** a plain/latest reference (`t@v1 JOIN t`) creates a default("") pin, so
  `getSnapshot(TableIf)` returns it (not EMPTY) and the guard is never reached → v1 ref silently skews.
  `t a JOIN t@v1 b JOIN t@v2 c` throws or silently-skews **depending on relation binding order** — the same
  query, non-deterministic.
- **MTMV refresh:** `MTMVTask.beforeMTMVRefresh` pre-seeds a default("") pin → the guard is never reached
  during MV refresh → silent skew.
- **@incr mix:** `t@v1 JOIN t@incr` (null pinnedSchema) → the design's "one null pin → latest, no throw"
  rule is itself a silent-skew hole.

## Fix — deterministic per-reference guard at the scan node (Option A, fail-loud)
Guard where each reference's OWN version-aware pinned schema is already resolved: `pinMvccSnapshot()`
(`PluginDrivenScanNode:863`). After resolving this scan's version-aware `snapshot`, if it carries a non-null
`getPinnedSchema()` (an explicit schema-at-snapshot pin — iceberg/paimon FOR VERSION/TIME/tag/branch; null
for latest/`@incr`/sys/hive → skipped), verify every **materialized, real tuple slot column resolves in that
pinned schema**:
- `uniqueId >= 0` (iceberg field-id present) → the field-id must be in the pinned schema's field-ids (BE
  matches iceberg by field-id → rename with a stable id is fine, renumber/added-column is caught).
- `uniqueId < 0` (paimon has no top-level field-id) → the column NAME must be in the pinned schema (BE
  matches by name → rename is caught).
If any bound slot is unresolved → the tuple was bound at a different version than this reference scans →
throw `UserException` (checked; `pinMvccSnapshot` already `throws UserException`) with a clear message.

This is **deterministic** (runs at scan time with all pins loaded, checks THIS reference against its OWN
tuple — order-independent), **complete** (catches self-join, latest-masked, `@incr`-mixed, AND MTMV-refresh —
every path builds scan nodes), and has **no false positives**: for `t@old JOIN t@latestSnapId` where the
explicit version IS latest, each reference's tuple matches its own version-aware schema → no throw (the
analysis-time guard would have wrongly thrown this working query). Common queries are untouched — a plain /
single-version / latest reference has a null pinnedSchema (or its tuple matches) → the guard is a no-op.

### Structural machinery (connector-agnostic)
- Static package-private `assertBoundColumnsResolveInPinnedSchema(List<Column> boundColumns,
  SchemaCacheValue pinnedSchema, String tableName)` — directly unit-testable (takes plain `Column`s + a
  `SchemaCacheValue`, no scan-node construction). Field-id-when-present-else-name matching (no source-name
  branch; the field-id-vs-name choice keys on whether `uniqueId` is populated, a generic column property).
- `pinMvccSnapshot` extracts the projected bound columns (`desc.getSlots()` where `getColumn() != null`,
  mirroring the existing `:1749` loop) and calls the helper with this scan's `getPinnedSchema()`.

## Iron rule
Clean. `PluginDrivenScanNode` is the generic connector-agnostic node; the guard operates on generic
`SchemaCacheValue`/`Column`/`SlotDescriptor` types. No `instanceof HMSExternal*`, no source-name branch, no
property parsing. Applies uniformly to every `PluginDrivenMvccExternalTable` connector.

## Residual / accepted (registered)
- **Nested field-id-only renumber** (a nested struct field's id changes with unchanged nested name+type) is
  invisible to the top-level field-id/name check (fe-core `StructField` carries no field id). Effectively
  never occurs in iceberg (ids are stable); not detected — accept, note under the TODO.
- **Same-version-different-selector** collapses to one map entry (`versionKeyOf`) → not multi-version → no
  guard. Correct.
- The guard THROWS (does not repair) the `t@v1 JOIN t@v2` same-schema-S-but-S≠latest case (silently broken
  today) — throwing > silent skew; the repair is the TODO.

## TODO (design debt — registered as D-MVCC-VERSION-SCHEMA in the task-list)
Make schema binding per-reference version-aware end-to-end (Trino model: a version-scoped
`ConnectorTableHandle` / per-reference column-handle resolution) so two references of one `TableIf` carry two
schemas and the self-join across a schema change WORKS instead of throwing. Removes the guard's restriction.
Nereids-wide (schema is bound off the single shared `TableIf` via the no-arg `getSchemaCacheValue()`).

## Tests (fe-core, Mockito)
Directly on the static helper (RED-able, no scan-node construction):
- RED: bound columns include one whose field-id is absent from the pinned schema (renumber/added column) →
  throws. And a name-only (uniqueId=-1) column absent from the pinned schema (paimon rename) → throws.
- GREEN: all bound columns resolve by field-id (rename keeps id → resolves; subset projection ok) → no throw.
- GREEN: all bound columns resolve by name when uniqueId=-1 → no throw.
- GREEN: null pinnedSchema (latest/@incr) → no throw.
Plus the existing `PluginDrivenScanNodeMvccPinTest` suite stays green (the guard is off the pinned-snapshot
path only when schemas match).

## Blast radius
`pinMvccSnapshot` runs at 4 scan-side sites (idempotent read-only check; throws consistently at the first).
Only fires when `getPinnedSchema() != null` (explicit schema-at-snapshot pin) AND a bound column is
unresolved — i.e. genuine version/schema skew. Plain / latest / single-version / hive / sys-table / count
scans are no-ops. e2e live-gated (iceberg self-join FOR VERSION AS OF v1 vs v2 across an ALTER).
