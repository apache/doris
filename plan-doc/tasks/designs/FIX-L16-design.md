# FIX-L16 — iceberg snapshot/schema cache skew: VERIFIED BENIGN (no functional fix)

## Verdict: REFUTED (already-fixed + benign by construction)
Two claims were bundled under L16; both are non-issues at HEAD (`63f3c1965e7`):

### (1) "superset dict via requestedLowerNames" — already fixed / absent
`IcebergScanPlanProvider.getScanNodeProperties` hasSnapshotPin arm (`:1161-1163`) passes
`Collections.emptyList()` over `pinnedSchema(table, iceHandle)` — the FULL pinned schema, NOT
`requestedLowerNames`. `git log -L` shows it has been `emptyList` since the file's first commit. So the
"superset via requestedLowerNames" shape the task-list line describes does not exist.
(And had it existed, the effect would be a hard THROW, not a benign superset: `IcebergSchemaUtils`
`buildCurrentSchema` does `caseInsensitiveFindField(name)` per requested name and throws when absent;
`requestedLowerNames` is keyed off the LATEST-schema column handles, so a column renamed since the pinned
snapshot would be absent from the OLD pinned schema → throw at plan time. The `emptyList` branch instead
iterates `pinnedSchema.columns()` and emits every pinned top-level column — a guaranteed superset of the
BE scan slots, which are themselves built from the pinned schema.)

### (2) "snapshot cache vs schema cache skew" — benign by construction
- The pin is **atomic**: `beginQuerySnapshot` (`IcebergConnectorMetadata:1533-1540`) captures
  `(snapshotId, latest schemaId)` from ONE table load, so the two ids cannot skew relative to each other
  within the TTL.
- The `schemaId` is threaded from ONE `ConnectorMvccSnapshot` via `applySnapshot`→`withSnapshot` into BOTH
  `handle.getSchemaId()` (dict, `pinnedSchema`) and `snapshot.getSchemaId()` (slots, `getTableSchema`).
- iceberg `schemas()` is **append-only**, so a fixed `schemaId` present at pin time is present in every later
  generation — `pinnedSchema` is generation-stable. `get(schemaId)==null` only for a loaded generation
  PREDATING the schema's creation, and THEN `getTableSchema` (same seam, same query) also predates it and
  ALSO falls back to `table.schema()` — so dict names and slot names degrade to the SAME latest schema and
  still match. No reachable input makes the dict top-level names diverge from the BE scan-slot names; a
  self-join at two snapshots is per-handle isolated.

## The load-bearing invariant (why the guard comment)
`dict top-level names == BE StructNode scan-slot names` — a divergence makes BE's unconditional
`children.at(name)` `std::out_of_range`-SIGABRT the whole BE on a schema-evolved time-travel read. That
invariant holds ONLY because `IcebergScanPlanProvider.pinnedSchema` (dict) and
`IcebergConnectorMetadata.getTableSchema` (slots) use the **SAME schemaId selector + the SAME silent
fallback to `table.schema()`**. Hardening ONE of them (e.g. making `pinnedSchema` throw-loud on a missing
schemaId) would break the symmetry and CREATE the crash it looks like it prevents.

## Change: guard comment only (no behavior change)
Add a cross-referencing note on both `pinnedSchema` and `getTableSchema`'s fallback stating the two
selectors/fallbacks MUST stay identical, so a future "defensive" edit does not silently introduce the
asymmetry. No code/behavior change; no new test (nothing RED-able — a divergence test cannot be
constructed at HEAD, itself evidence the hazard is benign).

## Relation to L17
L16's residual and L17 share the root "schema binding not version-tracked", but on DIFFERENT axes: L16 =
snapshot-cache-vs-schema-cache atomicity across time (benign, above); L17 = per-reference version blindness
within one statement (the real, if narrow, fe-core gap). The Trino-style structural elimination (serialize
the pinned schema onto the handle) belongs to L17's track, not here.

## Iron rule
Clean — comments only, inside fe-connector-iceberg. No fe-core touch.
