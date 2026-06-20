# P5-fix-SCHEMA-EVOLUTION — native reader loses paimon schema-evolution (B-1a, +M-10 deferred)

> Task #3 of `task-list-P5-rereview2-fixes.md`. Finding: rereview2 §3 **B-1a** (BLOCKER) + **M-10** (MAJOR).
> Design chosen by user: **Design C — connector builds the thrift `TSchema` list directly** (zero new SPI surface). M-10 deferred (see §Deferred).

---

## Problem

On the **native** (ORC/Parquet) read path the connector emits only a per-file `schema_id`
(`TPaimonFileDesc.schema_id`) but never sets the scan-level `TFileScanRangeParams.current_schema_id`
or `history_schema_info`. BE (`be/src/format/table/table_schema_change_helper.h:219-237`) then takes
the `!__isset.history_schema_info` branch → **name-based** file↔table column matching. On a
schema-evolved table (column rename / reorder) the older-schema data files have different column
names, so renamed columns read **NULL / garbage silently**. JNI path is unaffected (the serialized
paimon `Table` carries its own schema); native is the default for ORC/Parquet, so the common case is broken.

E2E repro: `regression-test/.../test_paimon_full_schema_change.groovy` (struct field `a`→`new_a` over
ORC/Parquet) — `SELECT new_a` returns NULL for pre-rename rows.

## Root Cause

The cutover connector reproduced the per-file `schema_id` tag but **not** the two scan-level fields the
BE field-id matcher requires. Legacy `paimon/source/PaimonScanNode` set them via
`ExternalUtil.initSchemaInfo(params, -1L, currentColumns)` (current entry) +
`putHistorySchemaInfo(file.schemaId())` (per native split) → `PaimonUtil.getHistorySchemaInfo` (one
`TSchema` per referenced schema id, built from the historical paimon `TableSchema`). The generic
`PluginDrivenScanNode` bridge never calls any `ExternalUtil.initSchemaInfo*` (grep-confirmed: only
Iceberg/Hudi/legacy-Paimon scan nodes do), and the connector has no seam emitting these fields.

## Frozen BE contract (re-confirmed against current code)

`be/src/format/table/table_schema_change_helper.{h,cpp}` + `gensrc/thrift/{PlanNodes,ExternalTableSchema}.thrift`:

- `TFileScanRangeParams.current_schema_id` (i64, field 25) + `history_schema_info`
  (`list<TSchema>`, field 26). `TSchema{schema_id, root_field:TStructField}`;
  `TField{is_optional, i32 id, name, TColumnType type, TNestedField nestedField, name_mapping}`.
- Native matcher `gen_table_info_node_by_field_id(params, split_schema_id)`:
  - if `current_schema_id == split_schema_id` → `ConstNode` (identity, name-based) **fast path**.
  - else linear-scan `history_schema_info` for the entries whose `.schema_id ==` current and split;
    **`InternalError("miss table/file schema info")` if either is absent** (fail-loud, not silent).
  - else `by_table_field_id(history[current].root_field, history[split].root_field)` — matches table
    field → file field **by `TField.id`**, names may differ (rename-safe); a table id absent from the
    file → `add_not_exist_children` (read NULL).
- **What BE actually reads from each `TField`** (`table_schema_change_helper.cpp:312-430`): `id`,
  `name`, and `type.type` used **only as a nesting tag** (`== MAP / ARRAY / STRUCT`, else scalar).
  It never reads precision/scale and **never reads the tuple/slot descriptor** in the field-id path.

Two consequences that drive the design:
1. A correct `TSchema` needs only paimon field `id` + `name` + a primitive **tag** — **no Doris `Type`
   / `toColumnTypeThrift` required**. So the connector can build it (and `org.apache.doris.thrift.*`
   — incl. `…thrift.schema.external.*` — is **import-legal** in connectors; the gate only bans
   `catalog|common|datasource|qe|analysis|nereids|planner`).
2. `Column.uniqueId` (M-10) is **not** read by BE; it only mattered in legacy as the *source* the
   FE history-builder read. Building the history map straight from paimon `DataField.id()` makes
   B-1a independent of M-10 → M-10 deferred.

## Design (Design C — connector-side, no new SPI)

Build the scan-level schema dictionary entirely inside the connector, from the live (snapshot-pinned)
paimon `Table`'s `SchemaManager`, and set it on `params` via the **existing** `populateScanLevelParams`
SPI hook. The per-split `schema_id` tag is **already** emitted (`PaimonScanRange` →
`fileDesc.setSchemaId`) — no change there.

### What to emit (parity with legacy semantics)

- `params.current_schema_id = -1L` — keep the legacy `-1` sentinel ("latest"), for exact parity
  (legacy always routed through `by_table_field_id`, never the ConstNode fast path).
- `history_schema_info` =
  - one entry keyed **`-1`** built from the table's **latest** `TableSchema`
    (`schemaManager().latest()`), i.e. the "current/target" schema, **plus**
  - one entry per id in `schemaManager().listAllIds()`, each built from `schemaManager().schema(id)`.

  Using `listAllIds()` (all committed schemas) instead of only the split-referenced ids (which the
  connector cannot know at this seam without planning) **guarantees** every native file's
  `schema_id` is covered → never the BE `InternalError`. Extra entries are harmless (BE only looks up
  `current_schema_id` and each split's id). Perf delta vs legacy logged as a deviation (§Risk).

### How to build each `TSchema` (direct port of `PaimonUtil.getSchemaInfo`)

New package-private static helpers in the connector (mirroring `PaimonUtil.getSchemaInfo(349-430)`
but emitting a primitive **tag** instead of `toColumnTypeThrift`), so they are unit-testable from
plain paimon `DataField`s without a live `DataTable`:

```
TSchema buildSchemaInfo(TableSchema s):
    TSchema{ schemaId = s.id(); rootField = buildStructField(s.fields()) }

TStructField buildStructField(List<DataField> fields):           // sets id+name (the match keys)
    for f in fields:
        TField c = buildField(f.type()); c.setName(f.name()); c.setId(f.id());  // id only on top-level + struct fields
        add c

TField buildField(DataType t):                                   // nesting structure + tag
    ARRAY → nested.array_field.item_field = buildField(elem);  type.type = ARRAY
    MAP   → nested.map_field.key/value     = buildField(k/v);   type.type = MAP
    ROW   → nested.struct_field            = buildStructField(t.getFields()); type.type = STRUCT
    else  → type.type = tag(t)                                   // scalar: BE ignores the exact tag
    set is_optional = t.isNullable()
```

`tag(DataType)`: a compact `paimon DataTypeRoot → TPrimitiveType` switch (mirrors
`PaimonTypeMapping.toConnectorType`'s choices; **only ARRAY/MAP/STRUCT are load-bearing**, scalars are
cosmetic — default `STRING`). Field-id is set **only** on top-level columns and STRUCT fields
(array element / map key+value / leaf scalars are matched structurally by BE, never by id —
exactly as legacy `getSchemaInfo` does).

### Where it runs + transport (provider is per-call → state via props)

`getScanPlanProvider()` returns a **new** `PaimonScanPlanProvider` per call, so
`getScanNodeProperties` and `populateScanLevelParams` run on **different instances**; the only shared
channel is the props map (bridge caches `getScanNodeProperties` output and feeds it to
`populateScanLevelParams`). Therefore:

1. **`getScanNodeProperties`** (already resolves the live scan `Table`): if the table is a native-
   eligible normal data table (`table instanceof org.apache.paimon.table.DataTable`), build the
   `current_schema_id` + `List<TSchema>` from its `schemaManager()`, stage them onto a throwaway
   `TFileScanRangeParams`, `TSerializer`-serialize → base64 → `props.put("paimon.schema_evolution", …)`.
   Guard: non-`DataTable` (sys-tables `audit_log`/`binlog`, which go JNI and never consult
   `history_schema_info`) or any `SchemaManager` failure → skip the prop (no regression; native
   non-evolved tables still read correctly via BE name-matching).
2. **`populateScanLevelParams`**: if `props["paimon.schema_evolution"]` present, `TDeserializer` it and
   copy `currentSchemaId` + `historySchemaInfo` onto the real `params`.

`TSerializer`/`TDeserializer` (`org.apache.thrift.*`, already a transitive runtime dep of the thrift
classes the connector uses) keep the transport classloader-safe; the schema is built from the **live**
table (no paimon `Table` round-trip).

## Implementation Plan

Connector only (`fe-connector-paimon`); **no fe-core / SPI / BE / thrift-IDL changes**.

1. `PaimonScanPlanProvider.java`
   - Imports: `org.apache.doris.thrift.schema.external.{TSchema,TField,TStructField,TArrayField,TMapField,TNestedField,TFieldPtr}`, `org.apache.doris.thrift.{TColumnType,TPrimitiveType}`, `org.apache.thrift.{TSerializer,TDeserializer}`, paimon `schema.{SchemaManager,TableSchema}` + `table.DataTable` + `types.*`.
   - New static helpers `buildSchemaInfo` / `buildStructField` / `buildField` / `tag` (above).
   - New helper `buildSchemaEvolutionParams(Table) → Optional<String>` (base64) guarded on `DataTable`.
   - `getScanNodeProperties`: after resolving `table`, `buildSchemaEvolutionParams(table)` → put `paimon.schema_evolution` prop (skip when empty).
   - `populateScanLevelParams`: read `paimon.schema_evolution` → deserialize → set `current_schema_id` + `history_schema_info` on `params`.
2. No change to `buildNativeRange` / `PaimonScanRange` (per-file `schema_id` already emitted).

## Risk Analysis

- **Fail-loud on coverage gap**: history covers `-1` (latest) + all `listAllIds()` ⊇ every committed
  file `schema_id`, so the BE `InternalError("miss table/file schema info")` cannot trigger from a
  missing entry. (A file referencing a *deleted* schema is already unreadable — pre-existing, fail-loud.)
- **Perf (accepted deviation, logged)**: legacy fetched only split-referenced schemas via a cached
  loader; Design C reads `listAllIds()` + each `schema(id)` from the live table per scan (no fe-core
  cache reachable from the connector). Bounded (K small JSON reads, K = #schema versions; once per
  scan, props cached). Correctness-first; future optimization = referenced-only (needs a split-aware
  seam) or a connector-side cache. → `deviations-log.md`.
- **Scalar `type.type` tag is best-effort**: safe because BE consumes only the ARRAY/MAP/STRUCT-vs-
  scalar distinction in the field-id path (verified). Nested tags are exact.
- **Snapshot/time-travel**: matches legacy (current = latest schema; D-043 schema-at-snapshot is a
  separate, untouched path). E2E is rename, not time-travel.
- **JNI / sys-tables**: `history_schema_info` set on a JNI-only scan is never read by BE → harmless;
  `DataTable` guard additionally avoids building it for sys-tables.

## Test Plan

### Unit (runnable FE, `PaimonScanPlanProviderTest`)
- `buildSchemaInfo`/`buildField` over constructed paimon `DataField`s (no `DataTable` needed):
  - flat schema → `TSchema.root_field.fields[i].{id,name}` == paimon field id/name; `type.type` tag correct.
  - nested ARRAY<INT>, MAP<STRING,INT>, ROW<f1,f2> → correct `TNestedField` shape; STRUCT children carry their paimon ids; array element / map kv carry no id (match legacy).
  - rename case: two `TableSchema`s (id 0 `a:int`, id 1 `new_a:int` same field id) → both entries present; ids stable across rename.
- `populateScanLevelParams` round-trip: a staged `paimon.schema_evolution` prop → asserts
  `params.isSetCurrentSchemaId()` (== -1) and `params.getHistorySchemaInfo()` matches the built list.
- Guard: non-`DataTable` (e.g. `FakePaimonTable`) → no `paimon.schema_evolution` prop, existing
  prop-map assertions unchanged (update any test that snapshots the exact prop set).

### E2E (CI-gated — note as gated, not run locally)
- `test_paimon_full_schema_change.groovy` (rename over ORC/Parquet): pre-rename rows read the correct
  values under `SELECT new_a` (was NULL).

## Deferred — M-10 (`Column.uniqueId == -1`)

The connector still builds `ConnectorColumn` without a field-id channel, so Doris `Column.uniqueId`
stays `-1`. Per the user's Design-C choice this is **deferred**: rereview2 §4 refuted the standalone
M-10 repro (no demonstrated user-visible consumer; BE does not read the tuple descriptor in the
field-id path, and the only legacy `Column.uniqueId` consumer — `ExternalUtil.initSchemaInfo` via the
legacy scan node — is dead post-cutover). B-1a is fully fixed independently. Logged in
`deviations-log.md` (re-confirm inconsequential if a future field-id consumer appears, e.g. an
SPI-on iceberg/hudi reusing `ExternalUtil` from Doris columns).

## Review Outcome (clean-room, 3-lens + verify)

A 3-lens adversarial review (legacy-parity / BE-contract / edge-regression, each finding
adversarially verified) **confirmed the non-time-travel core mechanism correct** but found **2 real
BLOCKERs in the `-1`/current entry**, both fixed (re-verified `fix_complete && !new_defect`):

1. **Column-name casing.** I built the `-1` entry with paimon's case-preserving `field.name()`. BE
   keys the table-side `StructNode` by that name **verbatim** (`table_schema_change_helper.cpp:404,414`),
   while the native reader looks it up by the **lowercase Doris slot name**
   (`vorc_reader.cpp:500-501`); and because `current_schema_id=-1` never equals a real file
   `schema_id`, the `ConstNode` fast-path is **never** taken — so `by_table_field_id` runs on **every**
   native read. A mixed/upper-case column → `children.at("mycol")` miss → `std::out_of_range`/crash,
   **regressing even never-evolved tables**. **Fix:** lowercase **only top-level** names of the `-1`
   entry (default-locale `toLowerCase()`, byte-matching the slot-name producer
   `PaimonConnectorMetadata` + legacy `parseSchema:507`); nested struct names stay paimon-cased
   (legacy is asymmetric — `PaimonUtil:302` keeps nested case), historical entries fully paimon-cased.

2. **Time travel.** I built the `-1` entry from `schemaManager().latest()` (absolute latest), but a
   time-travel read's tuple slots use the **snapshot-pinned** schema → BE keys by latest names, the
   reader queries pinned names → crash/wrong-rows on a column renamed between the pinned snapshot and
   latest. **Fix:** build the `-1` entry from `((FileStoreTable) table).schema()` — the resolved
   (snapshot-pinned) schema, the same one the tuple uses (verified: `copyInternal`/`tryTimeTravel`
   sets `tableSchema` to `schema(snapshot.schemaId())`). For non-time-travel reads `schema() == latest`
   → no change. Guard narrowed `DataTable`→`FileStoreTable` (gives both `schema()` and
   `schemaManager()`; every native-eligible table is a `FileStoreTable`).

The MINOR (eager `listAllIds()` reads all committed schemas, uncached → a transient IO error on an
*unreferenced* schema aborts a scan legacy would complete) is the design's accepted fail-loud
deviation — logged in `deviations-log.md` (DV-027), not a commit blocker.
