# Hudi-on-HMS schema-evolution parity — authoritative step design (HD-C4/HD-C5)

> Recon: `wf_85bd47a0-0aa` (6 HEAD-grounded readers + completeness + decomposition critics; iron-rule reader failed, gap closed by the critics + hand verification). All file:line HEAD-verified. Signed-off scope = **full parity** (memory `hudi-schema-evolution-full-parity-signoff`, 2026-07-09). This is the hardest hudi sub-area. Every sub-step is a **dormant** connector-internal commit (hms not in `SPI_READY_TYPES`; hudi handles not yet diverted) with a same-loader unit test where possible; correctness is only observable on BE at flip-time e2e.

---

## 0. Problem

Post-flip a hudi-on-HMS table is a generic `PluginDrivenExternalTable`. Today the hudi connector emits **nothing** for schema-history → BE `can_map_by_history_schema` returns false (`be/src/format_v2/table/schema_history_util.cpp:124`) → `BY_NAME` fallback (case-insensitive `to_lower` matching). That safe baseline is correct for plain/add/drop/reorder reads but a **renamed** column reads NULL on old files (its old physical name no longer matches the new table name). To reach legacy parity the connector must drive BE's `BY_FIELD_ID` path by emitting field-ids + a per-version schema history + a per-split schema id, and must resolve schema **at the pinned instant** for time travel.

---

## 1. BE contract (what FE must populate — NO BE code changes)

BE's native format_v2 hudi machinery is **already in place and identical to paimon** (`be/src/format_v2/table/hudi_reader.cpp:30-52` mirrors `paimon_reader.cpp:36-55`). Field-id mapping engages **only** when ALL of the following are populated together:

1. **`TFileScanRangeParams.current_schema_id`** (PlanNodes.thrift field 25) — **keep `-1`** (the sentinel). BE `find_external_root_field` (`be/src/format_v2/table_reader.cpp:254`) selects the history `TSchema` whose `schema_id == current_schema_id` as the *target overlay* and stamps each projected table column `identifier = TYPE_INT(field.id)` from it (`:514`). A real id equal to any per-split id would drive the **v1** engine's `current==split → ConstNode` case-sensitive verbatim-name path (`be/src/format/table/table_schema_change_helper.h:243`) = **the banned half-fix**. `-1` is `!= ` every split id (splits are `>=0`) ⇒ always the field-id path.
2. **`TFileScanRangeParams.history_schema_info`** (field 26, `list<TSchema>`) covering: (a) one `TSchema{schema_id=-1}` — the *target/current* overlay, built from the **requested** columns (names == BE scan slots by construction, the CI-969249 `StructNode` DCHECK invariant); (b) one `TSchema` per distinct per-split `schema_id` in the scan. Each `TField` MUST carry `id` (InternalSchema field id, **stable across renames**), `name` (**physical file column name in THAT version**), `type` (`TColumnType`), and `nestedField` for struct/array/map — `id`+`name` at **every** nesting level.
3. **`THudiFileDesc.schema_id`** (field 12, **native reader only**) = the InternalSchema version of the commit that wrote that base file; `>=0` and present in `history_schema_info`. Unset/`-1` ⇒ `BY_NAME` (safe, no evolution).

**Mapping mechanics** (`schema_history_util.cpp:132`, `column_mapper.cpp:102`): file columns are stamped with the split-schema's field-ids by `to_lower(name)` match against the physical file columns; table columns are stamped with the `-1`-entry ids; `FieldIdMatcher` maps table→file **purely by integer id** — a rename works because the same id appears under a different name in the `-1` vs split `TSchema`. A missing id ⇒ `ranges::find_if` end ⇒ nullptr ⇒ column becomes missing/const **silently** (no throw). ⇒ **populate `id` at EVERY level of EVERY `TSchema`** or you get silent corruption strictly worse than `BY_NAME`.

**JNI / MOR-realtime path** (`be/src/format_v2/jni/hudi_jni_reader.cpp`) consumes **no** schema_id / history / field-id. It forwards `hudi_params.column_names` + `column_types` (fields 8/9) + `instant_time` verbatim to `HadoopHudiJniScanner` and projects by `table_column.name`. So MOR evolution correctness is **entirely FE-supplied** = HD-C5.

**Lowercase rule (load-bearing).** format_v2 hudi is fully case-insensitive (`to_lower` everywhere) and cannot SIGABRT. But the **same** `history_schema_info` thrift is consumed by the **v1** `format/table` hudi reader (`table_schema_change_helper.cpp:393` builds `StructNode` children keyed by the **raw** history `TField.name`; `helper.h:143/162/167` then `children.at(lowercased_table_name)` → `std::out_of_range` → uncaught → whole-process SIGABRT). ⇒ the FE emitter MUST lowercase **every** `TField.name` at **every** nesting level (mirror the iceberg `DROP_AND_ADD` fix; memory `catalog-spi-history-schema-info-lowercase-nested-names`).

---

## 2. Template = PAIMON (not iceberg), lowercase = ICEBERG

| Concern | iceberg | paimon | **hudi = ** |
|---|---|---|---|
| BE path | `by_file_field_id` (ids in parquet/orc metadata) | `by_table_field_id` (FE-supplied history) | **paimon** — needs per-version history + per-split id |
| history entries | ONE (`-1` only) | `-1` + one per `SchemaManager.listAllIds()` | **paimon-style multi-entry** |
| per-split `schema_id` | none (`TIcebergFileDesc` has no such field) | `RawFile.schemaId()`, native branch only | **per-file, native branch only** |
| `current_schema_id` | `-1` | `-1` | **`-1`** |
| `-1` entry source | requested columns | requested columns (name-match pinned schema, latest fallback) | **requested column handles** (CI-969249) |
| lowercase | **every level** (`IcebergSchemaUtils.buildField:245`, `Locale.ROOT`) | top-level only (`PaimonScanPlanProvider:1521`) | **iceberg — every level** |
| scalar `TColumnType` | STRING placeholder (nested-vs-scalar discriminator) | STRING placeholder | **STRING placeholder** (BE ignores scalar tag on field-id path; do NOT port legacy full Doris-type map) |

Reference emitters: `PaimonScanPlanProvider.buildSchemaEvolutionParam:1363-1395` / `buildField:1532-1578` / `PaimonScanRange.populateRangeParams:238-241` (per-split, native-only) / `encode+apply:1462/1477`. Iceberg lowercase call site: `IcebergSchemaUtils.buildField:245`.

---

## 3. What to port from legacy (fe-core → connector)

Legacy flow (`HudiScanNode` + `HudiUtils` + `HudiSchemaCacheKey/Value` + `HMSExternalTable.initHudiSchema` + `HiveMetaStoreClientHelper.getHudiTableSchema`):

- **Field-id source = MODE-AWARE InternalSchema** (`HiveMetaStoreClientHelper.getHudiTableSchema:829-857`): evolution on ⇒ `getTableInternalSchemaFromCommitMetadata(instant)` (stable ids); evolution off ⇒ `AvroInternalSchemaConverter.convert(getTableAvroSchema(true))` (positional ids, versionId 0). **DO NOT** source the `-1`-entry/handle ids from a naive `convert(latest avro)` when evolution is on — the completeness critic's crux: naive-convert ids won't match the per-file `getCommitInstantInternalSchema` ids ⇒ `BY_FIELD_ID` mismatch. Same logical column = same id across versions **only** through the mode-aware path.
- **Per-file `schema_id`** (`HudiScanNode.setHudiParams:304-323`): native branch only; `FSUtils.getCommitTime(<dataFileName>)` → `InternalSchemaCache.searchSchemaAndCache` (evolution) = real versionId, else `convert()` = **0** (never `-1`; JAR-confirmed). JNI branch never sets it.
- **`TField` builder** (`HudiUtils.getSchemaInfo:350-421`): port as a self-contained `Types.Field → TField` converter — but **add every-level lowercasing** (legacy lowercases nothing, `:363`) and emit STRING-placeholder scalar type (legacy emits full Doris type `:407-410` — drop it).
- **Schema-at-instant** (`HudiScanNode.doInitialize:206-224`, `getSchemaCacheValue` keyed by `(nameMapping, queryInstant)`): resolve columns/colTypes/ids AT the pinned instant. **Legacy limitation to mirror:** `getTableInternalSchemaFromCommitMetadata` honors the instant **only** when schema.on.read is enabled; else falls back to LATEST avro. For a non-evolution table this is byte-equivalent (schema never changed).
- **metaClient/storage coupling** (HD-C1/C2): `hmsTable.getHudiClient()` → connector metaClient; every timeline/InternalSchema/`searchSchemaAndCache` touch on the metadata thread must be wrapped in `HudiMetaClientExecutor.execute` (legacy left `getHudiTableSchema`/`getCommitInstantInternalSchema` UNWRAPPED — the port must be STRICTER, iron rule). `getScanNodeProperties`/`planScan` already run on the TCCL-pinned scan thread.

---

## 4. Architecture placement & iron rules

- **100% connector-internal to `fe-connector-hudi`. NO new connector-api SPI method** — every seam pre-exists: `ConnectorTableOps.getTableSchema(session,handle,snapshot)` 3-arg default (+ hive gateway delegation ALREADY shipped `HiveConnectorMetadata.java:1070-1078`); `ConnectorScanPlanProvider.populateScanLevelParams` default no-op; `getScanNodeProperties`; `ConnectorColumn.withUniqueId`; `THudiFileDesc` field 12; `TFileScanRangeParams` 25/26.
- **fe-core stays source-agnostic** — it only plumbs the base64 `hudi.schema_evolution` scan-node prop and the per-split `hudi.schema_id` range prop; zero schema logic, zero `if(format)`.
- **Sibling delegation already reaches the hudi path** (recon-confirmed): `HiveConnector.getScanPlanProvider(handle):165-169` routes a foreign hudi handle to `resolveSiblingOwner(handle).getScanPlanProvider(handle)` (so `getScanNodeProperties`/`populateScanLevelParams` run); `getColumnHandles` delegated at `HiveConnectorMetadata:382`. No new hive-gateway change.
- **`getColumnHandles` runs BEFORE the MVCC pin** (`PluginDrivenScanNode.java:973` vs `pinMvccSnapshot:979`) ⇒ handle field-ids are LATEST-keyed. Fine for steady-state (InternalSchema ids stable). Under time travel a renamed column's pinned name is absent from the latest-built handle map ⇒ dropped from `columns`. ⇒ **C5b MUST re-resolve the `-1` entry against the pinned schema** (iceberg full-pinned / empty-requested pattern), not rely on the handle.

---

## 5. Dormant-commit decomposition (ascending same-loader testability; C4* before C5*)

- **HD-C4a — `HudiSchemaUtils` converter (new connector class).** Self-contained `InternalSchema`/`Types.Field → TSchema/TStructField/TField` builder + `encode`/`apply` round-trip on a throwaway `TFileScanRangeParams` (fields 25/26). Lowercase EVERY level (mirror `IcebergSchemaUtils.buildField:245`, NOT legacy verbatim, NOT paimon top-only); scalar `TColumnType` = STRING placeholder; `id`+`name`+`is_optional` at every level. Pure library, wired into nothing yet. Zero fe-core import. *Test (fully same-loader):* hand-build an InternalSchema with a mixed-case nested struct child; assert every emitted `TField.name` lowercased at every level, current/history round-trips base64, scalar `type.type==STRING`, nested ARRAY/MAP/STRUCT tags. Mirror `HudiSchemaParityTest`. **deps: none.**
- **HD-C4b — field-ids onto `HudiColumnHandle`.** Add `int fieldId` (default `ConnectorColumn.UNSET_UNIQUE_ID=-1`; ctor+getter; **KEEP equals/hashCode by name** like `IcebergColumnHandle:55-69` — do NOT add id to identity). Source ids from the **mode-aware** InternalSchema (mirror legacy `updateHudiColumnUniqueId` + `initHudiSchema`), via `ConnectorColumn.withUniqueId`; `getColumnHandles` threads `col.getUniqueId()` onto the handle. *Test (fully same-loader):* build an Avro/InternalSchema; assert each `ConnectorColumn.getUniqueId()==` the InternalSchema field id (non-evolution convert path); evolution-mode commit-metadata id source is e2e. **deps: none.** (Paimon-style provider-side name-resolution was considered and **rejected** in favor of legacy fidelity + reuse for future nested-prune field-ids; see §6.)
- **HD-C4c — per-split `THudiFileDesc.schema_id` (native branch ONLY).** `HudiScanRange.Builder` gains `schemaId`; `populateRangeParams` sets `fileDesc.setSchemaId` ONLY in the native else-branch (`HudiScanRange.java:219-221`), NEVER the JNI branch (`:192-218`) — legacy/paimon parity. Provider computes per-file id in `collectCowSplits` and the native no-log MOR read-optimized slice that **downgrades to native** (`HudiScanRange:179-183` — NOT only COW): `FSUtils.getCommitTime(fileName) → InternalSchemaCache.searchSchemaAndCache` (evolution) else fallback InternalSchema id `0`. Runs on the TCCL-pinned scan thread. Factor a **shared static per-file→schemaId resolver** into `HudiSchemaUtils` that C4d reuses. *Test:* stamping is same-loader (`HudiScanRangeTest`: Builder→populateRangeParams asserts field 12 set on native, UNSET on JNI); the id value is e2e. **deps: shares resolver with C4d.**
- **HD-C4d — scan-level dict emission + apply (steady-state / no-pin).** `getScanNodeProperties` builds a base64 `hudi.schema_evolution` prop = `current_schema_id(-1)` + the `-1` entry from the **requested** columns arg + one entry per **referenced-split** InternalSchema version (via the C4c shared resolver + C4a converter). OVERRIDE `populateScanLevelParams` to copy fields 25/26. Gate OFF for `force_jni`/MOR-realtime-only handles (paimon gate parity). *Test:* encode/decode round-trip same-loader; history-set completeness + BE engage are e2e. **deps: C4a; shares C4c resolver. HARD-ORDER after C4c** (a dormant prefix flipping with the dict present while per-split `schema_id` is unset(`-1`) is the forbidden `current==file==-1` v1-ConstNode degrade — land C4c+C4d back-to-back).
- **HD-C5a — schema-at-instant column list (`getTableSchema` 3-arg override).** Override `ConnectorTableOps.getTableSchema(session,handle,snapshot)` on `HudiConnectorMetadata` (ABSENT today → SPI default returns latest = HD-C2 residual #1). Key off `HudiTableHandle.getQueryInstant()` (NOT `snapshot.getSchemaId()` which stays `-1` for hudi); null instant → delegate to the 2-arg latest path (shared build-path so latest/at-instant can't drift). Resolve InternalSchema AT the instant WRAPPED in `HudiMetaClientExecutor.execute` (also closes the existing unwrapped `getSchemaFromMetaClient` gap). Hive delegation already shipped ⇒ no hive change. *Test:* null-instant→latest delegation branch is same-loader; the at-instant read is e2e. **deps: C4b.**
- **HD-C5b — scan-side at-instant: MOR/JNI column list + `-1` entry at pinned instant.** (i) `planScan` JNI `column_names`/`column_types` re-derived from the queryInstant-scoped schema (replacing latest `getTableAvroSchema(true)`) — legacy `HudiScanNode:222-224`; keep BOTH schema loci in lockstep. (ii) The dict `-1` entry re-resolved against the **pinned** InternalSchema (iceberg full-pinned / empty-requested) because handle ids are latest-keyed — else a renamed column drops a BE slot. `current_schema_id` stays `-1`. All off-scan-thread timeline touches under `HudiMetaClientExecutor.execute`. *Test:* almost entirely e2e; FE-unit asserts JNI col list + `-1` entry are derived from the instant-resolved schema, not latest. **deps: C4c, C4d, C5a.**

---

## 6. Resolved design decisions (mirror legacy byte-faithfully — consistent with the full-parity sign-off)

- **D1 — history-set enumeration = union of REFERENCED-split ids** (not all-versions, not lazy-per-split). Hudi has no `SchemaManager.listAllIds()`. The C4c per-file resolver already computes each selected split's `schema_id`; the C4d dict emits `-1` + exactly those referenced ids. Self-consistent by construction (every emitted split id is in history ⇒ no BE fail-loud "miss schema info"), cheaper than enumerating history, and byte-faithful to legacy's per-referenced-file `putHistorySchemaInfo` (just up-front at plan level, not lazy). ⇒ C4c and C4d share the resolver and land back-to-back.
- **D2 — `current_schema_id` stays `-1`, `-1` entry made pinned-correct under time travel** (C5b), rejecting the time-travel reader's "emit a real current_schema_id" (would risk the v1 `ConstNode` case-sensitive degrade; paimon + legacy both emit `-1`).
- **D3 — non-evolution FOR TIME AS OF mirrors legacy's latest-fallback.** For a non-evolution table the schema never changed via InternalSchema, so "latest" == "at-instant" — byte-equivalent. Evolution (schema.on.read) tables get true schema-at-instant. No extension beyond legacy.
- **D4 — keep field-id on the handle (C4b).** Mirrors legacy `updateHudiColumnUniqueId` + the plan mandate; the steady-state `-1` entry reads ids from the requested handles directly; same field-id also unblocks future nested-column-prune. Paimon-style provider-side name-resolution was the simpler alternative but diverges from legacy and would still need C5b re-resolve under pin anyway.
- **D5 — scalar `TColumnType` = STRING placeholder** (BE by_table_field_id uses `type.type` only as a nested-vs-scalar discriminator; don't port the legacy full avro→Doris type map).

---

## 7. Correctness gates / landmines

1. **Half-fix ban**: never engage `BY_FIELD_ID` (split id `>=0` + history set) with any `TField.id` unset → silent column drop. Populate `id` at every level of every `TSchema`.
2. **`current==split==-1` ban**: keep `current_schema_id=-1` with splits `>=0`.
3. **Split-entry names = physical file column names at that commit** (BE stamps file cols by name to get ids).
4. **Lowercase every nested level** (v1 SIGABRT guard).
5. **TCCL/UGI**: every new timeline/InternalSchema/`searchSchemaAndCache` touch under `HudiMetaClientExecutor.execute`.
6. **JNI omits schema_id**; only native base-file / native-downgraded MOR-RO slices carry it.
7. **`@incr` lists LATEST schema** (both sides; PluginDrivenMvccExternalTable INCREMENTAL branch + legacy leaves snapshot null) — intended, not a missing pin.

---

## 8. Flip-time e2e (per memory `hms-iceberg-delegation-needs-e2e`)

Heterogeneous-HMS catalog, hudi tables vs a standalone hudi catalog, same rows:
- COW + MOR schema-evolved (rename + reorder + add + drop) read (native BY_FIELD_ID engages, renamed column reads correctly on old files).
- Mixed-case nested-struct-field table (no SIGABRT on any reachable path).
- `FOR TIME AS OF` on a schema-evolved COW table (schema-at-instant column list) and a schema-evolved MOR table (JNI column list @instant).
- Non-evolution `FOR TIME AS OF` (latest-fallback byte-equivalence).
- `@incr` over an evolved table (latest schema).

---

## 9. Status — DESIGN SIGNED OFF (2026-07-09), split into two parts

User signed off the full design + all §6 decisions, with an explicit **two-part split** (2026-07-09):
- **Part 1 = HD-C4 steady-state read evolution (C4a→C4b→C4c→C4d)** as independent dormant commits, each with its §5 same-loader test, THEN a consolidated **adversarial review** of the whole steady-state part before Part 2. C4c+C4d land back-to-back (D1 shared resolver + prefix-flip safety).
- **Part 2 = HD-C5 time-travel-over-evolved (C5a→C5b)** as independent dormant commits + review, next work line after Part 1.

User will **start Part 1 in a fresh session**. Owed at flip: the §8 e2e.

**First actionable step (Part 1, C4a):** new connector-internal `HudiSchemaUtils` — pure `InternalSchema`/`Types.Field → TSchema/TStructField/TField` converter + base64 encode/apply round-trip on a throwaway `TFileScanRangeParams` (fields 25/26); lowercase every level (mirror `IcebergSchemaUtils.buildField`), STRING-placeholder scalar tag, `id`+`name`+`is_optional` at every level; wired into nothing yet; fully same-loader unit-testable. Zero fe-core import.

Related: [[hudi-on-hms-delegation-plan-2026-07-08.md]] §HD-C4/C5, `hudi-time-travel-step-design-2026-07-09.md` (pin spine), recon `wf_85bd47a0-0aa`, memories `hudi-schema-evolution-full-parity-signoff` + `catalog-spi-history-schema-info-lowercase-nested-names`.
