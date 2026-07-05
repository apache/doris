# P6.2-T07 — iceberg MVCC / time-travel — design

> **Task**: port iceberg point-in-time reads (`FOR VERSION AS OF` / `FOR TIME AS OF` / `@branch` / `@tag`)
> and the query-begin MVCC pin into the connector, driven through the **generic** `PluginDrivenMvccExternalTable`
> + `PluginDrivenScanNode` seam (E5/D-042, already in fe-core). Self-contained port of legacy
> `IcebergScanNode.getSpecifiedSnapshot` / `IcebergUtils.getQuerySpecSnapshot` / `IcebergMvccSnapshot` /
> `getLatestIcebergSnapshot`; mirrors the proven paimon template (`PaimonConnectorMetadata.{beginQuerySnapshot,
> resolveTimeTravel,applySnapshot,getTableSchema(@snapshot)}`). **0 new SPI** (all seams exist; the iceberg
> `ConnectorMvccSnapshot` carries `snapshotId`/`schemaId` + a `ref` property). Also resolves the **T06 fail-loud
> race** with the user-signed Option A (§6). **Zero behavior change pre-cutover** — iceberg stays out of
> `SPI_READY_TYPES`; the new code is exercised only by offline UT until P6.6.

## 0. The generic seam (what fe-core already does — we only implement the connector side)

`PluginDrivenExternalDatabase.buildTableInternal:53` instantiates `PluginDrivenMvccExternalTable` **iff** the
connector declares `ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT`. That table:
- **Query-begin (B5a)**: `materializeLatest` → `metadata.beginQuerySnapshot(session, handle)` → pins a
  `ConnectorMvccSnapshot`, `pinnedSchema = null` (reads latest schema).
- **Explicit time-travel**: `loadSnapshot` → `metadata.resolveTimeTravel(session, handle, spec)` (empty ⇒
  fe-core throws the kind-specific `notFoundMessage`) → `metadata.applySnapshot(handle, snapshot)` **then**
  `metadata.getTableSchema(session, pinnedHandle, snapshot)` (schema AS OF the pinned `schemaId`) → the pinned
  schema becomes the Doris table's columns.
- **Scan**: `PluginDrivenScanNode` pins the context snapshot onto `currentHandle` via
  `applyMvccSnapshotPin → metadata.applySnapshot` before `planScan` / `getScanNodePropertiesResult`
  (3 call sites: `:678`, `:876`, `:1059`). `getCountFromSnapshot` already reads `scan.snapshot()`, so it
  follows the pin once `buildScan` applies it.

`ConnectorMetadata` defaults for all four MVCC methods are no-ops (`beginQuerySnapshot`/`resolveTimeTravel` →
empty, `applySnapshot` → handle unchanged). So today iceberg (no capability declared) is a plain non-MVCC
table. T07 = declare the capability + implement the four methods + thread the pin into the scan.

## 1. The 5 time-travel kinds — legacy → connector (verbatim resolution)

fe-core `PluginDrivenMvccExternalTable.toTimeTravelSpec` maps Doris syntax to a `ConnectorTimeTravelSpec.Kind`
(digital `FOR VERSION AS OF` → `SNAPSHOT_ID`, non-digital → `TAG`; `FOR TIME AS OF` → `TIMESTAMP`; `@tag`/
`@branch`/`@incr` → `TAG`/`BRANCH`/`INCREMENTAL`). The connector owns ALL provider parsing. Mapping legacy
`IcebergUtils.getQuerySpecSnapshot` (:1294) per kind, **empty-if-not-found** (fe-core translates empty into the
user-facing error; only a malformed value throws):

| Kind | iceberg SDK resolution | `snapshotId` | `schemaId` | pin property |
|---|---|---|---|---|
| `SNAPSHOT_ID` | `id=Long.parseLong(v)`; `Snapshot s=table.snapshot(id)`; `s==null ⇒ empty` | `id` | `s.schemaId()` | — (typed `snapshotId`) |
| `TIMESTAMP` | `millis` (digital⇒`parseLong`, else `IcebergTimeUtils.datetimeToMillis(v, sessionZone)`); `SnapshotUtil.snapshotIdAsOfTime(table, millis)` (throws `IllegalArgumentException` if none ⇒ catch → empty) | resolved id | `table.snapshot(id).schemaId()` | — |
| `TAG` | `SnapshotRef r=table.refs().get(name)`; `r==null \|\| !r.isTag() ⇒ empty` | `r.snapshotId()` | `SnapshotUtil.schemaFor(table,name).schemaId()` | `iceberg.scan.ref=name` |
| `BRANCH` | `SnapshotRef r=table.refs().get(name)`; `r==null \|\| !r.isBranch() ⇒ empty` | `r.snapshotId()` | `SnapshotUtil.schemaFor(table,name).schemaId()` | `iceberg.scan.ref=name` |
| `INCREMENTAL` | **unsupported for iceberg** (legacy `getQuerySpecSnapshot` never dispatches `@incr`; the legacy path silently read latest = a no-op). | — | — | — → **fail loud** |

- **Tag vs branch both use `useRef(name)`** at scan time (legacy `createTableScan:577-602`: `scan.useRef(ref)`
  when `info.getRef() != null`, else `scan.useSnapshot(snapshotId)`). So the connector carries the ref NAME in a
  property (`ConnectorMvccSnapshot` has no `ref` field — typed `snapshotId`/`schemaId` only) and `applySnapshot`
  routes it to `handle.withSnapshot(snapshotId, ref, schemaId)`. We still resolve `snapshotId` for MTMV /
  consistency, but the scan pins by REF (legacy parity: a later commit to the branch/tag is honored).
- **`SnapshotUtil.snapshotIdAsOfTime` throws** (not returns -1) when no snapshot ≤ the timestamp — catch
  `IllegalArgumentException` → `Optional.empty()` (fe-core renders "can't find snapshot earlier than or equal to
  time"). A malformed datetime string is a `DateTimeException` ("can't parse time: …", legacy parity) and
  propagates (fail loud, not empty — a parse error is a user mistake, not a not-found).
- **`INCREMENTAL`**: throw a clear connector exception ("incremental read (@incr) is not supported for Iceberg
  tables"). Intentional divergence from legacy's silent read-latest (a latent no-op); fail-loud is correct
  (Rule 12). UT-invisible pre-cutover; registered as a deviation.

## 2. `beginQuerySnapshot` (query-begin pin) — legacy `getLatestIcebergSnapshot`

`Table t = loadTable(...)` (auth-wrapped); `Snapshot s = t.currentSnapshot()`;
`snapshotId = s==null ? -1 : s.snapshotId()`; `schemaId = t.schema().schemaId()` (the LATEST schema id, even
when `currentSnapshot().schemaId()` is older — legacy `getLatestIcebergSnapshot:1412` comment: schema-only
changes without a new snapshot). Return `ConnectorMvccSnapshot.builder().snapshotId(snapshotId).schemaId(
schemaId).build()`. An empty table pins `snapshotId=-1` (not `Optional.empty()` — iceberg DOES support MVCC,
mirrors paimon's `INVALID_SNAPSHOT_ID`). **No cache in T07** — a live `loadTable` + `currentSnapshot()` per
call; the per-catalog `IcebergLatestSnapshotCache` is **T08** (the risk-register "live read" option is valid for
T07: the generic seam calls `beginQuerySnapshot` once per query and reuses the pin within the query).

## 3. `applySnapshot` + `getTableSchema(@snapshot)`

- **`applySnapshot(session, handle, snapshot)`**: `snapshot==null ⇒ handle`; else read `snapshotId =
  snapshot.getSnapshotId()`, `ref = snapshot.getProperties().get("iceberg.scan.ref")`, `schemaId =
  snapshot.getSchemaId()`. If `snapshotId < 0 && ref == null` (empty-table latest pin) ⇒ return handle UNCHANGED
  (read latest — a `useSnapshot(-1)` would be a non-existent snapshot, mirrors paimon's `-1` guard). Else return
  `iceHandle.withSnapshot(snapshotId, ref, schemaId)`.
- **`getTableSchema(session, handle, snapshot)`** (new overload): `snapshot==null || snapshot.getSchemaId()<0`
  ⇒ delegate to the latest `getTableSchema(session, handle)`. Else `Table t = loadTable(...)`;
  `Schema sc = t.schemas().get((int) schemaId)` (legacy `IcebergUtils.getSchema:1088` — `table.schemas().get`
  when `schemaId != NEWEST_SCHEMA_ID(-1)` and `currentSnapshot()!=null`, else `table.schema()`); `parseSchema(
  sc)`; build the `ConnectorTableSchema` (same property assembly as latest, but `iceberg.format-version` /
  `location` / `iceberg.partition-spec` from `t` — these are table-level, not schema-versioned). Mirrors
  `PaimonConnectorMetadata.getTableSchema(@snapshot)` factoring.

## 4. Scan-time pin (`IcebergScanPlanProvider`)

`buildScan` (today `table.newScan()` + filters, line 248) gains the pin (mirrors legacy `createTableScan`):
```
TableScan scan = table.newScan();
if (ref != null)         scan = scan.useRef(ref);
else if (snapshotId>=0)  scan = scan.useSnapshot(snapshotId);
// predicate conversion uses the CURRENT schema, byte-parity legacy createTableScan:589
// (convertToIcebergExpr(conjunct, icebergTable.schema())) — a predicate on a column renamed since the
// pinned snapshot resolves to no field and drops to BE residual, exactly like legacy; the common no-rename
// case is identical, and the unbound expression still binds against the pinned snapshot's schema at plan time.
new IcebergPredicateConverter(table.schema(), zone).convert(filter) → scan.filter(expr) ...
```
`buildScan` now takes the `IcebergTableHandle` (for the pin fields). `getCountFromSnapshot(scan, session)`
already reads `scan.snapshot()`, so the COUNT path follows the pin automatically (no change). `planScanInternal`
passes the handle into `buildScan`. (The pinned schema IS used — but only for the field-id DICT in
`getScanNodeProperties` §5, where the dict must reflect the pinned schema the BE slots come from.)

## 5. The field-id dict under a pin (Option A — user-signed §6)

`getScanNodeProperties` (T06) builds the dict from `table.schema()` (current) keyed off the requested column
names. Under a pin this must change so the dict reflects the PINNED schema AND covers every BE scan slot:
```
IcebergTableHandle h = (IcebergTableHandle) handle;
if (h.hasSnapshotPin()) {
    Schema dictSchema = h.getSchemaId() >= 0 && table.schemas().containsKey((int) h.getSchemaId())
            ? table.schemas().get((int) h.getSchemaId()) : table.schema();
    // Option A: full pinned-schema dict (a guaranteed SUPERSET of the BE slots — see §6).
    props.put(SCHEMA_EVOLUTION_PROP, encodeSchemaEvolutionProp(table, dictSchema, Collections.emptyList()));
} else {
    props.put(SCHEMA_EVOLUTION_PROP, encodeSchemaEvolutionProp(table, table.schema(), requestedLowerNames(columns)));
}
```
`IcebergSchemaUtils` gains a 3-arg `encodeSchemaEvolutionProp(Table table, Schema dictSchema, List<String>
requestedLowerNames)` (the existing 2-arg delegates with `table.schema()`); `buildCurrentSchema` is unchanged
(empty `requestedLowerNames` ⇒ all `dictSchema.columns()` — the already-tested T06 fallback). name-mapping stays
table-level (`extractNameMapping(table)`).

## 6. 🔴 The T06 fail-loud race — Option A (user-signed 2026-06-23)

**Corrected root cause** (code-grounded, refines T06 design §6/§7): under time-travel the Doris table's columns
= the PINNED schema (`PluginDrivenMvccExternalTable.getSchemaCacheValue` → `pinnedSchema`), so the query slots
carry PINNED names. But `PluginDrivenScanNode.buildColumnHandles` (`:1048`/`:1117`) calls `getColumnHandles(
currentHandle)` BEFORE `pinMvccSnapshot` (`:1059`), so `allHandles` is keyed by CURRENT/latest names. A column
**renamed** `a`→`b` (iceberg field-id 5, permanent) between the pinned snapshot and now: slot `a` →
`allHandles.get("a")` = null → **`a` is dropped from `columns`** → the T06 dict (keyed off `columns`) misses
field-id 5 → BE `iceberg_reader.cpp:181 children_column_exists("a")` **StructNode DCHECK → whole-BE crash**.
(The T06 §6 hypothesis was the FE `buildCurrentSchema` fail-loud; in fact the dropped column never reaches the
dict builder, so the failure is the BE DCHECK, not the FE throw. The design's two fixes — ① build the dict from
the handle's field-id, ② resolve `getColumnHandles` at the pinned handle — both fail: ① the handle is already
dropped upstream, ② `getColumnHandles` has no snapshot param ⇒ a shared fe-core/SPI change that ALSO does not
fix paimon's snapshot-id case.)

**Option A (chosen, connector-local, 0 SPI / 0 fe-core / 0 paimon impact)**: when the handle carries a snapshot
pin, build the field-id dict from the FULL pinned schema (all columns) instead of the pruned `columns`. Safe and
correct for iceberg because:
1. **BE matches by file field-id, not FE projection.** `by_parquet_field_id` reads each file column's embedded
   field-id and matches it to the table-side dict; the dict only needs to be a SUPERSET of the BE scan slots so
   the `StructNode` lookup never misses. A full pinned-schema dict is exactly that superset, and the renamed
   slot `a` (field-id 5) IS in it → no DCHECK, correct field-id match.
2. **iceberg projection is BE-tuple-driven, not FE-`columns`-driven.** `planScan`/`buildScan` never call
   `scan.select(columns)` (verified — `columns` feeds ONLY the dict, line 482). So the upstream-dropped column
   does not drop the data read; only the dict needs to be made robust.
3. **Reuses tested code.** Routes time-travel to T06's existing empty-`requestedLowerNames` → all-columns
   branch; the dict is small FE→BE metadata, so the lost pruning under time-travel is negligible. Extra dict
   entries beyond the slots are harmless (BE only looks up the slots it needs).

The deeper cross-connector gap — `getColumnHandles` has no snapshot-aware overload, so paimon's snapshot-id
time-travel + rename is the SAME latent BE crash — is **registered as a P6.6 holistic concern** (shared fe-core,
needs paimon impact analysis), like the GLOBAL_ROWID blocker (T06 §6). Option A closes the iceberg-side crash
now without touching the shared seam.

## 7. Capability + TZ util

- **`IcebergConnector.getCapabilities()`** (currently the `Connector` default = empty set) → override returning
  `EnumSet.of(SUPPORTS_MVCC_SNAPSHOT, SUPPORTS_TIME_TRAVEL)`. `SUPPORTS_MVCC_SNAPSHOT` is the gate for
  `PluginDrivenMvccExternalTable`; `SUPPORTS_TIME_TRAVEL` mirrors paimon (verify consumers at impl). **Inert
  pre-cutover** (the capability is consumed only on the plugin-driven path, which iceberg does not use until it
  enters `SPI_READY_TYPES`).
- **`IcebergTimeUtils`** (new, self-contained — extract the TZ alias map currently inlined in
  `IcebergScanPlanProvider`): `resolveSessionZone(ConnectorSession)` (alias map = `ZoneId.SHORT_IDS` +
  CST/PRC→Asia/Shanghai + UTC/GMT→UTC, byte-identical to T02) and `datetimeToMillis(String value, ZoneId zone)`
  = `LocalDateTime.parse(value, ofPattern("yyyy-MM-dd HH:mm:ss")).atZone(zone).toInstant().toEpochMilli()`
  (byte-parity legacy `TimeUtils.timeStringToLong(value, sessionTZ)`; `DateTimeParseException` →
  `DateTimeException("can't parse time: " + value)`). `IcebergScanPlanProvider.resolveSessionZone` delegates to
  it (removes the duplicate alias map). `IcebergConnectorMetadata.resolveTimeTravel`'s `TIMESTAMP` case uses it.

## 8. Component layout (files touched)

- **`IcebergTableHandle`** (+pin): `snapshotId` (long, -1=none), `ref` (String, null=none), `schemaId` (long,
  -1=latest) + `withSnapshot(snapshotId, ref, schemaId)` (returns a NEW handle, immutable) + `hasSnapshotPin()`
  (`snapshotId>=0 || ref!=null`) + getters. `toString`/`equals`/`hashCode` include the pin (handle identity).
- **`IcebergTimeUtils`** (new): TZ alias map + `resolveSessionZone` + `datetimeToMillis`.
- **`IcebergConnectorMetadata`**: `beginQuerySnapshot`, `resolveTimeTravel`, `applySnapshot`, `getTableSchema(
  …, ConnectorMvccSnapshot)` overload (+ refactor the latest `getTableSchema` table-property assembly into a
  shared `buildTableSchema(Table, Schema)` so latest and at-snapshot cannot drift). `REF_PROPERTY =
  "iceberg.scan.ref"`.
- **`IcebergSchemaUtils`**: 3-arg `encodeSchemaEvolutionProp` overload.
- **`IcebergScanPlanProvider`**: `buildScan` takes the handle + applies `useSnapshot`/`useRef` + pinned-schema
  predicate; `getScanNodeProperties` Option-A dict; `resolveSessionZone` delegates to `IcebergTimeUtils`.
- **`IcebergConnector`**: `getCapabilities` override.

## 9. Deviations (UT-invisible; P6.6 docker validates)

- **`INCREMENTAL` (@incr) fail-loud** vs legacy silent read-latest (legacy `getQuerySpecSnapshot` never
  dispatches `@incr`). Intentional (Rule 12).
- **`TIMESTAMP` honors a digital value as epoch-millis** (paimon-style, generic-seam `digital` flag); legacy
  iceberg always parsed as a datetime string (a digital value would have failed). Strict superset — every
  legacy-accepted datetime string is parsed identically (`yyyy-MM-dd HH:mm:ss` + session zone); only the
  previously-erroring digital form now succeeds. Benign.
- **`beginQuerySnapshot` reads live** (no cache); T08 adds `IcebergLatestSnapshotCache`. Within a query the pin
  is stable (single-pin seam); across queries it may drift until T08 — a performance/consistency-window
  divergence, not correctness.
- **Option A: full pinned-schema dict under time-travel** vs T06 pruned. Superset, race-safe (§6); the deeper
  `getColumnHandles`-no-snapshot gap (paimon-shared) is a P6.6 holistic concern.
- **Tag/branch pin by REF name** (`useRef`), not by resolved snapshot id (legacy parity — honors later commits
  to the ref). `snapshotId`/`schemaId` are still resolved for MTMV + the schema-at-snapshot dict.

## 10. Tests (TDD; real `InMemoryCatalog`, no Mockito; assert resolved snapshot/schema vs legacy expectation)

- **`IcebergTableHandle`**: pin getters/`withSnapshot`/`hasSnapshotPin`; equality includes the pin.
- **`IcebergTimeUtils`**: `datetimeToMillis` parity for a known string×zone (incl. `CST`→Asia/Shanghai alias);
  `DateTimeException` on a malformed string.
- **`IcebergConnectorMetadata`** MVCC (InMemoryCatalog table with ≥2 snapshots, a tag, a branch, schema
  evolution): `beginQuerySnapshot` (current id + latest schema id; empty table → -1); `resolveTimeTravel` each
  kind (snapshot-id present/absent→empty; timestamp at/before; tag/branch present/wrong-kind→empty; INCREMENTAL→
  throws); `applySnapshot` (pin fields threaded; empty/-1 → unchanged); `getTableSchema(@snapshot)` (schema AS OF
  an older schema-id differs from latest; null/-1 snapshot → latest).
- **`IcebergScanPlanProvider`**: `buildScan` pins (`useSnapshot`/`useRef` — assert the planned file set comes
  from the pinned snapshot, not latest); COUNT follows the pin; **Option-A dict** — under a pin with a renamed
  column the dict contains the renamed (pinned-name) field-id entry (decode + assert), and a non-pinned scan
  keeps the T06 pruned dict.
- **`IcebergConnector`**: `getCapabilities` contains `SUPPORTS_MVCC_SNAPSHOT` + `SUPPORTS_TIME_TRAVEL`.

**Acceptance**: fe-connector-iceberg UT green (no Mockito) + checkstyle 0 + `check-connector-imports.sh` clean +
iceberg still **not** in `SPI_READY_TYPES` (zero behavior change) + no SPI/fe-core/pom changes.
