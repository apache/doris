# P6.2-T06 — iceberg field-id schema dictionary (`history_schema_info`) — design

> **Task**: port the schema-evolution field-id dictionary so BE matches file↔table columns **by field-id**
> (rename/reorder-safe) on iceberg native (parquet/orc) reads, instead of falling back to NAME matching
> (which silently reads NULL/garbage for renamed columns) or DCHECK-aborting the whole BE on a missing
> column (CI #969249 class). **Highest-risk P6.2 task** — UT-invisible (only P6.6 docker e2e truly validates).
> **0 new SPI** (mirrors paimon `FIX-SCHEMA-EVOLUTION` — string property over `getScanNodeProperties` →
> `populateScanLevelParams`; `ConnectorColumn` unchanged). Mirrors the proven paimon template
> (`PaimonScanPlanProvider` schema-evolution machinery), with the iceberg-specific deltas in §3.

## 0. 🔴 Key correction to the HANDOFF/recon plan (legacy-parity, code-grounded)

The HANDOFF task line + recon §4 said the iceberg dict should **"enumerate all committed schema-ids"**
(`table.schemas()`) and emit one historical entry per id. **This is paimon-shaped and WRONG for iceberg.**
Code-grounded recon (workflow `wf_9da2a77c-df8`, 4 readers + main-line cross-read) proved:

- **Legacy iceberg emits exactly ONE schema entry** (`schema_id = -1`, `current_schema_id = -1`).
  `IcebergScanNode.createScanRangeLocations:452-471` calls `initSchemaInfoFor{All,Pruned}Column(params, -1L, …)`
  **once** — no loop over schema-ids. (`ExternalUtil.java:91/110/136` each `addToHistorySchemaInfo` once.)
- **BE reads FILE field-ids directly from the parquet/orc file metadata** (NOT from `history_schema_info`):
  `iceberg_reader.cpp:149 history_schema_info.front()` (single entry); `by_parquet_field_id`
  (`table_schema_change_helper.cpp:430-436` reads `parquet_fields_schema[idx].field_id` from the file) /
  `by_orc_field_id` (reads `ICEBERG_ORC_ATTRIBUTE` from the orc file). It matches the **table-side** field-ids
  (from the single `-1` entry) to the **file-side** field-ids (from the file) by equality. Because iceberg
  field-ids are **permanent invariants** (a column's id never changes; only its name can), a single
  table-side entry suffices — there is no per-file `schema_id` to look up (`TIcebergFileDesc` has **no**
  `schema_id` field; `setIcebergParams:285-395` sets none).
  - Contrast paimon/hudi `by_table_field_id`: matches table field-id ↔ **FE-supplied file** field-id, so it
    **needs** per-`schema_id` historical entries. Iceberg does not. This is the load-bearing divergence.

⇒ The iceberg dict = **one `TSchema` (schema_id = -1), `current_schema_id = -1`**, built from the requested
columns, with per-field name-mapping. **Faithful to legacy, simpler than the recon plan, and Rule-2/Rule-11
correct.** (Documented as an intentional deviation from the HANDOFF plan.)

## 1. BE invariants the dict must satisfy (from `table_schema_change_helper.{h,cpp}` + `iceberg_reader.cpp`)

1. **Field-id path gate**: `params.__isset.history_schema_info` ⇒ BE takes the field-id path
   (`table_schema_change_helper.h:219`). So emitting the dict switches BE off the legacy name path onto
   field-id matching. Must therefore be **correct**, not partial.
2. **Single entry, `current_schema_id = -1`**: iceberg's reader uses `history_schema_info.front()` and never
   looks a per-file `schema_id` up, so one entry is enough. (`current_schema_id == split_schema_id` ConstNode
   fast-path in the *base* helper is the paimon/hudi path; iceberg uses its own `by_parquet_field_id`.)
3. **StructNode DCHECK — names must cover the query slots**: `StructNode::children_column_exists` /
   `get_children_node` `DCHECK(children.contains(table_column_name))` (`table_schema_change_helper.h:141-167`);
   `iceberg_reader.cpp:181 children_column_exists(desc.name)`. The `-1` entry's top-level field names **must be
   a superset of the query slot names** or BE DCHECK-aborts the whole BE (CI #969249). ⇒ build the `-1` entry
   from the **requested columns** (= the authoritative Doris slots), NOT an independent schema read.
4. **`TField` fields BE reads in the field-id path**: `id` (the join key), `name` (StructNode key + name-mapping),
   `type.type` (**nested-vs-scalar discriminator ONLY** — BE never inspects the scalar tag), `nested_field`
   (struct/array/map structure), `name_mapping` (fallback for old files lacking embedded field-ids). ⇒ emit a
   **`TPrimitiveType.STRING` placeholder for ALL scalars** (no full type conversion needed; identical to paimon
   `buildField`, verified against the BE source).
5. **name-mapping fallback** (`by_parquet_field_id_with_name_mapping`,
   `table_schema_change_helper.cpp:559-586`): when ANY file column lacks an embedded field-id, BE falls back to
   `TField.name_mapping` (then plain name). Needed for old iceberg files written before field-ids were embedded.
   ⇒ set `TField.nameMapping` on each field whose field-id is in `schema.name-mapping.default`.

## 2. Transport mechanism (0 new SPI — mirror paimon `FIX-SCHEMA-EVOLUTION`)

`getScanPlanProvider()` returns a fresh provider per call, so the two SPI methods share no instance state.
Carry the dict through the **node-properties map** (`PluginDrivenScanNode` round-trips the SAME map from
`getScanNodeProperties` → `populateScanLevelParams`, verified `PluginDrivenScanNode.java:1044-1084`/`963-981`):

- `getScanNodeProperties` builds the dict (a throwaway `TFileScanRangeParams` holding `current_schema_id` +
  one `TSchema`), TBinaryProtocol-serializes + base64-encodes it under prop key `iceberg.schema_evolution`.
- `populateScanLevelParams` (new override) decodes it and copies `currentSchemaId`/`historySchemaInfo` onto the
  real `TFileScanRangeParams`. **Fail loud** on a decode error (this prop is produced by us — a failure is a
  real bug, and silently dropping it would re-introduce the silent wrong-rows BLOCKER on evolved reads).

## 3. iceberg deltas over the paimon template

| Aspect | paimon | iceberg (this task) |
|---|---|---|
| #entries | `-1` entry + one per `listAllIds()` (file field-ids from FE) | **`-1` entry ONLY** (file field-ids from the file) |
| field-id source | paimon `DataField.id()` | iceberg `Types.NestedField.fieldId()` (permanent) |
| `-1` entry keyed off | requested `columns`, matched by name to resolved+latest paimon schema | requested `columns`, matched by name (`caseInsensitiveFindField`) to `table.schema()` |
| name-mapping | — (paimon has none) | **per-field `TField.nameMapping`** from `schema.name-mapping.default` (legacy `extractNameMapping`) |
| scalar type | `STRING` placeholder | `STRING` placeholder (same) |
| top-level casing | `toLowerCase()` (default locale) | **`toLowerCase(Locale.ROOT)`** — byte-match `parseSchema` (which uses `Locale.ROOT`) |
| nested casing | paimon-cased | iceberg name **as-is** — byte-match `IcebergTypeMapping` (nested `f.name()` unchanged) |

## 4. Connector wiring (the recon-flagged gap)

`IcebergConnectorMetadata` **lacks `getColumnHandles()`** ⇒ today `PluginDrivenScanNode.buildColumnHandles`
returns empty and the provider never receives the requested columns. Add it so the `-1` entry can be keyed off
the **pruned requested columns** (the CI #969249 fix; without it the dict falls back to all-fields = the exact
anti-pattern). `buildColumnHandles` passes the **pruned query slots** (GLOBAL_ROWID filtered out — not a table
column handle; `PluginDrivenScanNode.java:1114-1131`). The legacy "GLOBAL_ROWID present → all-columns" branch
(`IcebergScanNode:465-466`) is approximated by **pruned-or-all-when-empty** (same as paimon; safe because the
pruned `-1` entry == the exact read slots ⊇ what BE looks up). Adding `getColumnHandles` is runtime-inert
pre-cutover and benign at cutover: iceberg `planScan` ignores `columns`, `applyProjection` is the default no-op.

## 5. Component layout

- **`IcebergColumnHandle`** (new, `implements ConnectorColumnHandle`): `name` (lowercased) + `fieldId`. equals/
  hashCode on name. Mirror `PaimonColumnHandle`.
- **`IcebergConnectorMetadata.getColumnHandles`** (new override): auth-wrapped `loadTable` (mirror
  `getTableSchema`) → `schema.columns()` → `LinkedHashMap` lowercase(`Locale.ROOT`) name → handle.
- **`IcebergSchemaUtils`** (new, self-contained — mirrors `IcebergPartitionUtils`/`IcebergPredicateConverter`
  style; zero fe-core import):
  - `CURRENT_SCHEMA_ID = -1L`, `SCHEMA_EVOLUTION_PROP = "iceberg.schema_evolution"`.
  - `extractNameMapping(Table)` → `Map<Integer,List<String>>` (port legacy `extractNameMapping` +
    `extractMappingsFromNameMapping`; fail-soft warn on parse error, like legacy).
  - `encodeSchemaEvolutionProp(Table, List<String> requestedLowerNames)` → base64 string (orchestrator:
    extractNameMapping + buildCurrentSchema + TSerializer/Base64). Fail loud on serialize error (+ `LinkageError`,
    mirror paimon — a thrift CL split surfaces as a clean per-query failure, not a session-killing `Error`).
  - `applySchemaEvolution(TFileScanRangeParams, String encoded)`: decode → `setCurrentSchemaId` +
    `setHistorySchemaInfo`. Fail loud on decode error.
  - `buildCurrentSchema(Schema, List<String> requestedLowerNames, Map nameMapping)` → `TSchema` (schema_id=-1):
    for each requested name `caseInsensitiveFindField` → `NestedField` → `buildField` with top-level name =
    the requested lower name; **fail loud** if a requested column is absent (defensive — within one query the
    requested columns came from the same `table.schema()` so this should never fire). Empty requested list →
    all `schema.columns()` (count-only / no-getColumnHandles fallback).
  - `buildField(NestedField, String nameOverride, Map nameMapping)` (recursive): `TField` = id=`fieldId()`,
    name=`nameOverride` (top) / `field.name()` (nested, as-is), isOptional=`isOptional()`, `TColumnType` =
    STRING (scalar) / ARRAY|MAP|STRUCT (nested), nested `TNestedField` for list(elementField)/map(key+value
    Field)/struct(fields), and `nameMapping` when `nameMapping.containsKey(fieldId)`.
- **`IcebergScanPlanProvider`**:
  - `getScanNodeProperties`: after `path_partition_keys`, `props.put(SCHEMA_EVOLUTION_PROP,
    IcebergSchemaUtils.encodeSchemaEvolutionProp(table, requestedLowerNames(columns)))`. **Unconditional**
    (legacy `createScanRangeLocations` always sets the dict; not gated on `force_jni_scanner` — legacy isn't).
    System tables (JNI) are P6.5, out of this provider's scope.
  - `populateScanLevelParams` (new override): `IcebergSchemaUtils.applySchemaEvolution(params, props.get(...))`
    when present.

## 6. 🔴 CONFIRMED CUTOVER BLOCKER (adversarial review `wf_7109cc62-b6e`; user-signed defer to P6.6)

**GLOBAL_ROWID is mis-classified on the SPI path → BE DCHECK-abort once iceberg cuts over.** Legacy
`IcebergScanNode.classifyColumn:908-918` returns `SYNTHESIZED` for GLOBAL_ROWID (top-N lazy materialization),
so BE `iceberg_reader.cpp:162-178` skips the field-id dict for it and fills the topn row id. The generic SPI
`PluginDrivenScanNode` does **not** override `classifyColumn` (`FileQueryScanNode:224` returns
`REGULAR`/`PARTITION_KEY` only), so GLOBAL_ROWID is `REGULAR` → `iceberg_reader.cpp:189-190` pushes it into
`column_names` → the field-id matcher looks it up in the dict's `StructNode` → `children_column_exists` DCHECK
on a name absent from the dict → whole-BE crash. The connector **cannot** fix this: GLOBAL_ROWID is filtered
out of `columns` by `buildColumnHandles` before the connector sees it, and it is not a real iceberg column (no
field id) so it does not belong in the dict. **This gap pre-exists T06** (without the dict, BE took the
name-tolerant `by_parquet_name` path and merely mis-filled GLOBAL_ROWID as NULL — wrong but no crash); T06's
dict emission flips BE onto the field-id path that turns the silent wrong-fill into a crash.

**Fix belongs in the shared fe-core node** (`PluginDrivenScanNode.classifyColumn` → GLOBAL_ROWID =
`SYNTHESIZED`), and it is **cross-cutting / paimon-risky**: `paimon_reader.cpp` has NO SYNTHESIZED-GLOBAL_ROWID
handler (only `REGULAR`/`GENERATED` → `column_names`), so blindly making it `SYNTHESIZED` in the shared node
could break paimon top-N. ⇒ **Out of T06's connector-only scope; resolve holistically before iceberg enters
`SPI_READY_TYPES` (P6.6 cutover), with paimon-impact analysis (and likely BE coordination).** UT-invisible
(only P6.6 docker top-N-lazy-mat over an iceberg table triggers it). **User裁定 2026-06-22: 登记为 P6.6 翻闸阻塞项,
本轮不改 fe-core。**

## 7. Deviations (UT-invisible, registered for P6.6 docker validation)

- **Single `-1` entry vs HANDOFF "enumerate all schema-ids"** — legacy-faithful (§0); the recon plan was
  paimon-shaped. BE proves a single entry suffices for iceberg (file field-ids from the file).
- **`is_optional = true` unconditional (legacy parity, not iceberg's required/optional flag)** — legacy
  `ExternalUtil` sets `is_optional` from the Doris column's `isAllowNull()`, which `parseSchema` forces to
  `true` for every iceberg column. BE does NOT read `is_optional` on the iceberg field-id path
  (`table_schema_change_helper`/`iceberg_reader` never reference it), so it is inert there, but we keep legacy
  parity rather than leak iceberg's required/optional flag into the dictionary.
- **No paimon-style `latest()` fallback** when a requested column is absent in the resolved schema — fail loud
  instead. **Inert for T06** (no MVCC pinning yet: `applyMvccSnapshotPin` is a no-op without a
  `PluginDrivenMvccSnapshot`, so `getColumnHandles` and `getScanNodeProperties` read the SAME current handle →
  the requested columns are always present → fail-loud never fires). The adversarial review (`wf_7109cc62-b6e`)
  flagged a **T07 race**: `buildColumnHandles` runs at `PluginDrivenScanNode:1048` BEFORE
  `pinMvccSnapshot:1059`, while `getScanNodeProperties:1063` runs after the pin — so a time-travel query that
  pins an older snapshot could request a column dropped at that snapshot and trip the fail-loud. **Revisit at
  T07** (options: build the dict from the `IcebergColumnHandle`'s carried field id — the stable Doris-slot id,
  legacy `Column.uniqueId` parity — instead of re-looking-up by name; or resolve `getColumnHandles` at the
  pinned handle). Not a live T06 defect.
- **GLOBAL_ROWID all-columns vs pruned** — approximated by pruned-or-all-when-empty (paimon parity). The
  pruned `-1` entry is a valid superset of the BE field-slot lookups; the GLOBAL_ROWID **crash** is the
  separate, fe-core, cutover-blocker concern in §6 (NOT a dict-keying problem).
- **`STRING` scalar placeholder** — BE uses `type.type` only as a nested-vs-scalar discriminator (verified
  against the BE source); identical to paimon.
- **`Locale.ROOT` top-level lowercasing** (vs paimon default-locale) — byte-matches iceberg `parseSchema`.

## 8. Tests (TDD; real `InMemoryCatalog`, no Mockito; assert decoded dict vs legacy expectation, not class names)

- `IcebergColumnHandle`: getName/getFieldId, equals/hashCode on name.
- `IcebergConnectorMetadata.getColumnHandles`: lowercased name → handle with iceberg field-id; ordered.
- `IcebergSchemaUtils` (unit, decode the base64 → assert `TFileScanRangeParams`):
  - current_schema_id == -1, exactly ONE history entry, schema_id == -1.
  - top-level field ids == iceberg field-ids; names == lowercased requested names; **superset of slots**.
  - **rename** (`updateSchema().renameColumn`): the entry carries the NEW name but the **same field-id** (the
    rename-safe invariant — proves a single entry handles evolution).
  - pruned projection: only requested columns in the entry (CI #969249 — entry == slots).
  - empty requested list → all columns.
  - **name-mapping**: table with `schema.name-mapping.default` → `TField.nameMapping` set on the mapped fields
    (incl. nested), absent on unmapped.
  - **nested** struct/array/map: nested `TField`s carry their own field-ids; scalar leaves are `STRING`; nested
    names as-is.
  - fail loud: requested column absent from schema → exception (not a silent drop).
- `IcebergScanPlanProvider`:
  - `getScanNodeProperties` emits `iceberg.schema_evolution` (unconditional), round-trips through
    `populateScanLevelParams` to current_schema_id=-1 + non-empty history.
  - end-to-end with the existing partitioned/native ranges (the dict coexists with `path_partition_keys`).

**Acceptance**: connector UT green (no Mockito) + checkstyle 0 + `check-connector-imports.sh` clean + iceberg
still **not** in `SPI_READY_TYPES` (zero behavior change) + no SPI/fe-core/pom changes.
