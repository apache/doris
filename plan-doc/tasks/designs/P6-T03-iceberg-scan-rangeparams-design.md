# P6.2-T03 — iceberg scan: typed range params → `populateRangeParams` → `TIcebergFileDesc`, native-vs-JNI fileFormat, `path_partition_keys` (design)

> Branch `catalog-spi-10-iceberg`. Builds on T01 skeleton + T02 (predicate pushdown / split enumeration). **Zero behavior change**: iceberg stays out of `SPI_READY_TYPES`; verified by offline UT only (the BE-visible parity is exercised only at the P6.6 docker cutover). Recon = `plan-doc/research/p6.2-iceberg-scan-recon.md` §2 + 4 parity-grade reader subagents (legacy `IcebergScanNode.setIcebergParams`/`createIcebergSplit`, paimon `PaimonScanRange`/`PaimonScanPlanProvider`, `ConnectorScanRange`/`PluginDrivenScanNode` SPI, `gensrc/thrift/PlanNodes.thrift`).

## Scope (migration P6.2-T03, `P6-iceberg-migration.md:206`)
Take the minimal `IcebergScanRange` (path/start/length/fileSize) T02 produced and make it **BE-ready**:
1. **Typed iceberg carriers** on `IcebergScanRange` + Builder: `formatVersion`, `partitionSpecId`, `partitionDataJson`, `firstRowId`/`lastUpdatedSequenceNumber` (v3), `partitionValues` (identity), `fileFormat` (already present).
2. **`populateRangeParams(TTableFormatFileDesc, TFileRangeDesc)`** override → build `TIcebergFileDesc` + attach via `setIcebergParams`, mirror legacy `IcebergScanNode.setIcebergParams` (`:285-395`).
3. **native-vs-JNI fileFormat**: per-range `rangeDesc.setFormatType(FORMAT_ORC/FORMAT_PARQUET)` from the data file's real format (JNI = system tables, **P6.5**, out of scope).
4. **identity partition columns → columns-from-path** keys/values/isNull on `rangeDesc` (+ `partition_data_json` from **all** partition fields).
5. **`path_partition_keys`** (the #968880 double-fill guard) emitted via a **new `getScanNodeProperties` override** (lowercased, comma-joined identity columns).
6. Provider `planScan` populates all carriers per `FileScanTask`.

**Deferred (later tasks):** merge-on-read delete files → `TIcebergDeleteFileDesc` (T04); COUNT pushdown `table_level_row_count` + batch mode (T05); field-id history dict `current_schema_id`/`history_schema_info` via `populateScanLevelParams` (T06); MVCC snapshot pin (T07); manifest-cache split path (T08); vended creds `location.*` (T09); EXPLAIN `appendExplainInfo`/`getDeleteFiles` read-back (with T04).

## Non-goals
- `populateRangeParams`/`getScanNodeProperties`/`getPartitionValues` are existing `ConnectorScanRange`/`ConnectorScanPlanProvider` hooks. **One non-breaking SPI default method is added** — `ConnectorScanRange.isPartitionBearing()` (default `false`) — to fix the adversarial-review bug below (user-approved deviation from the recon "0 new SPI" expectation, since a real query-crash justifies a minimal non-breaking addition; paimon and all other connectors keep the default → byte-unchanged).
- No `SPI_READY_TYPES` change; no fe-core import (port partition helpers self-contained, like T02's `IcebergPredicateConverter`).
- Deletes / COUNT / field-id / MVCC / vended (their tasks).

## Key code-grounded facts (verified)
- **Thrift shapes** (`gensrc/thrift/PlanNodes.thrift`): `TIcebergFileDesc` fields = `format_version`(1,i32), `content`(2,i32, deprecated/v1-only), `delete_files`(3), `original_file_path`(6), `partition_spec_id`(8,i32), `partition_data_json`(9,string), `first_row_id`(10,i64,v3), `last_updated_sequence_number`(11,i64,v3), `serialized_split`(12). Container `TTableFormatFileDesc`: `table_format_type`(1,string), `iceberg_params`(2), `table_level_row_count`(9,i64,default -1). `TFileRangeDesc`: `columns_from_path`(6), `columns_from_path_keys`(7), `columns_from_path_is_null`(15), `table_format_params`(8), `format_type`(13,`TFileFormatType`). **No per-desc `schema_id` on iceberg** (field-id rides `TFileScanRangeParams.current_schema_id`/`history_schema_info` — T06). **No thrift `path_partition_keys`** anywhere.
- **Generic node wiring** (`PluginDrivenScanNode`/`FileQueryScanNode`): parent `splitToScanRange` first calls `createFileRangeDesc` (writes path/start/size + columns-from-path from the node-level `path_partition_keys` prop), `setFormatType(getFileFormatType())` (node-level `file_format_type` prop → default `FORMAT_JNI`), THEN `setScanParams` → `PluginDrivenScanNode.setScanParams` (`:932`): `new TTableFormatFileDesc()`, `setTableFormatType(scanRange.getTableFormatType())` (= `"iceberg"`), `scanRange.populateRangeParams(formatDesc, rangeDesc)`, `rangeDesc.setTableFormatParams(formatDesc)`. **`populateRangeParams` runs AFTER the parent**, so it can/must overwrite columns-from-path + format_type.
- **`path_partition_keys`** = node-level property consumed by `PluginDrivenScanNode.getPathPartitionKeys()` → parent classifies those slots as `PARTITION_KEY` and excludes them from the file-decode set (`num_of_columns_from_file`); without it BE double-fills partition columns (decode-from-file AND append-from-path) → OrcReader/parquet DCHECK (CI #968880). It is order-independent at the node level (set membership for slot classification); the actual per-range key/value pairs come from `populateRangeParams`.
- **columns-from-path overwrite (the legacy `unset`)** (`setIcebergParams:370-393`): legacy **unsets** `columns_from_path`/`_keys`/`_is_null` (clearing the parent's path-parsed values — iceberg does NOT Hive-path-encode partitions, so the parent's `parseColumnsFromPathWithNullInfo` produces garbage), then re-sets from `icebergSplit.getIcebergPartitionValues()` **iterated in `getOrderedPathPartitionKeys()` order, filtered to present keys**; value = `v != null ? v : ""`, isNull = `v == null`. **No `__HIVE_DEFAULT_PARTITION__` sentinel** (verified zero hits in `datasource/iceberg/`) — null conveyed purely by the parallel `is_null` list. This **diverges from paimon** (which renders the sentinel and does NOT unset).
- **fileFormat per file** (decision below): legacy uses one table-uniform `getFileFormatType()` (`source.getFileFormat()` = `IcebergUtils.getFileFormat(table)`); we use the **per-file** `dataFile.format()` (paimon-style per-range override) — strictly more correct, identical for the format-uniform tables that are ~100% of real cases.
- **partition data provenance** (`createIcebergSplit:849`): gate `isPartitionedTable (= table.spec().isPartitioned()) && partitionData != null`. `specId = file.specId()`; `spec = table.specs().get(specId)`; `partitionData = (PartitionData) file.partition()`. `firstRowId`/`lastUpdatedSequenceNumber` from the **DataFile** (`v3+`): `firstRowId = file.firstRowId() != null ? : -1`; `lastUpdatedSequenceNumber = (file.fileSequenceNumber() != null && file.firstRowId() != null) ? file.fileSequenceNumber() : -1` (asymmetric guard). iceberg 1.10.1 `ContentFile` confirmed to expose `firstRowId()`/`fileSequenceNumber()`/`format()`/`specId()`/`partition()`.
- **partition value serialization** (`IcebergUtils.serializePartitionValue:801-860`): per-`Type` matrix (BOOLEAN/INTEGER/LONG/STRING/UUID/DECIMAL→`toString`; FLOAT/DOUBLE→`Float/Double.toString`; DATE→`LocalDate.ofEpochDay` ISO; TIME→`LocalTime.ofNanoOfDay(micros*1000)` ISO; TIMESTAMP→`LocalDateTime.ofEpochSecond(.../UTC)`, if `shouldAdjustToUTC()` convert UTC→session zone, ISO; BINARY/FIXED/else→throw). `partition_data_json` = JSON array over **all** partition fields (`getPartitionValues`); columns-from-path = **identity, non-binary/fixed** only (`getIdentityPartitionInfoMap`), keys lowercased `Locale.ROOT`.

## Architecture / decisions
1. **Typed carriers, not paimon's stringly-typed props.** Paimon stores `paimon.*` string props in its `properties` map and re-parses them in `populateRangeParams`; nobody else reads them. Iceberg's params are strongly numeric (`formatVersion`/`specId`/`firstRowId`), so typed `IcebergScanRange` fields + Builder are cleaner and parse-error-free (Rule 2). `getProperties()` stays `emptyMap`. *Deviation from paimon's pattern, documented.*
2. **Self-contained `IcebergPartitionUtils`** (connector-internal, mirrors T02's `IcebergPredicateConverter`): ports `getIdentityPartitionColumns`, `getIdentityPartitionInfoMap`, `getPartitionValues`, `getPartitionDataJson`, `serializePartitionValue`. Only iceberg-core + guava imports; no fe-core. `getPartitionDataJson` swaps fe-core `GsonUtils.GSON` → iceberg's bundled `org.apache.iceberg.util.JsonUtil.mapper().writeValueAsString(List<String>)` (functionally equivalent — BE re-parses the JSON array; byte form is irrelevant). The timezone-bearing methods take a resolved `ZoneId` (from the provider's existing `resolveSessionZone`, alias-map-aware) instead of legacy's raw `String`+`ZoneId.of(tz)` — fixes the latent legacy crash on a non-canonical session `time_zone` (e.g. `"CST"`), consistent with the T02 alias-map fix.
3. **fileFormat = per-file** (`dataFile.format().name().toLowerCase(ROOT)`), paimon-style per-range override in `populateRangeParams`: `"orc"`→`FORMAT_ORC`, `"parquet"`→`FORMAT_PARQUET`, else leave the node default (`FORMAT_JNI`). Node-level `getScanNodeProperties` emits `file_format_type=jni` (paimon parity + forward-compat for P6.5 system-table JNI). *Deviation: legacy is table-uniform; per-file is strictly more correct (legacy has a latent wrong-reader bug on mixed-format tables — rare).*
4. **columns-from-path: unset-then-set, ordered by identity columns.** Provider pre-builds each range's `partitionValues` as a `LinkedHashMap` in `getIdentityPartitionColumns(table)` order, filtered to keys present in the file's `getIdentityPartitionInfoMap` (reproduces legacy's `getOrderedPathPartitionKeys`-ordered, present-filtered columns-from-path). `populateRangeParams` **unsets** the three columns-from-path lists (clear the parent's iceberg-invalid path-parsed values), then, if non-empty, sets keys/values/isNull from the map's entrySet (value `""`/isNull on Java-null). Range overrides `getPartitionValues()` so the parent uses `normalizeColumnsFromPath`; `populateRangeParams` then overwrites it.
   - **Adversarial-review fix (Bug 2).** When a partitioned file's identity map is *empty* (partition-spec evolution from a transform to identity, or all-binary/fixed identity columns), `getPartitionValues()` is empty → the generic `PluginDrivenSplit.buildPartitionValues` collapsed empty→`null` → `FileQueryScanNode` fell back to Hive-style **path parsing**, which `throw`s `UserException` for iceberg's non-`key=value` file layout. Legacy never path-parses (it always supplies a non-null empty list). Fix: new `ConnectorScanRange.isPartitionBearing()` (default `false`; `IcebergScanRange` returns `true` when `partitionSpecId != null`) + `buildPartitionValues` returns a non-null empty list for a partition-bearing empty map (non-partition-bearing keep the legacy empty→null collapse → paimon byte-unchanged).
5. **Fail-loud on unsupported file format (Bug 1).** `buildRange` rejects a non-orc/parquet `dataFile.format()` with `"Unsupported format name: %s for iceberg table."`, restoring legacy `getFileFormatType()`'s `DdlException` guard instead of silently leaving the node default `FORMAT_JNI` (which BE would route to its system-table JNI reader).

## `IcebergScanRange.populateRangeParams` (new override) — mirrors `setIcebergParams`
```
public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc):
  TIcebergFileDesc fileDesc = new TIcebergFileDesc();
  fileDesc.setFormatVersion(formatVersion);
  fileDesc.setOriginalFilePath(path);                 // raw data-file path (== range path; un-normalized)
  if (partitionSpecId != null) fileDesc.setPartitionSpecId(partitionSpecId);
  if (partitionDataJson != null) fileDesc.setPartitionDataJson(partitionDataJson);
  if (formatVersion >= 3) {
      fileDesc.setFirstRowId(firstRowId != null ? firstRowId : -1);
      fileDesc.setLastUpdatedSequenceNumber(lastUpdatedSequenceNumber != null ? lastUpdatedSequenceNumber : -1);
  }
  if (formatVersion < 2) fileDesc.setContent(FileContent.DATA.id());   // v1 only (== 0)
  // delete_files: T04

  // native fileFormat (JNI = system tables, P6.5)
  if ("orc".equals(fileFormat))      rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
  else if ("parquet".equals(fileFormat)) rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);

  formatDesc.setTableLevelRowCount(-1);               // non-count path (COUNT = T05)
  formatDesc.setIcebergParams(fileDesc);              // table_format_type already "iceberg" (node)

  // columns-from-path: overwrite the parent's iceberg-invalid path-parsed values
  rangeDesc.unsetColumnsFromPath();
  rangeDesc.unsetColumnsFromPathKeys();
  rangeDesc.unsetColumnsFromPathIsNull();
  if (!partitionValues.isEmpty()) {
      keys=[], values=[], isNull=[]
      for (k,v) in partitionValues:                   // LinkedHashMap already in identity-column order
          keys.add(k); values.add(v != null ? v : ""); isNull.add(v == null)
      rangeDesc.setColumnsFromPathKeys(keys);
      rangeDesc.setColumnsFromPath(values);
      rangeDesc.setColumnsFromPathIsNull(isNull);
  }
```
`getPartitionValues()` overridden → returns `partitionValues`. Builder gains: `formatVersion(int)`, `partitionSpecId(Integer)`, `partitionDataJson(String)`, `firstRowId(Long)`, `lastUpdatedSequenceNumber(Long)`, `partitionValues(Map)`.

## `IcebergScanPlanProvider` changes
**`planScan`** per `FileScanTask`:
```
formatVersion = getFormatVersion(table)            // computed once (port T09 getFormatVersion)
orderedKeys   = IcebergPartitionUtils.getIdentityPartitionColumns(table)   // once
zone          = resolveSessionZone(session)        // existing (alias-map-aware)
partitioned   = table.spec().isPartitioned()
...per task...
  f = task.file()
  fmt = f.format().name().toLowerCase(ROOT)
  Integer specId=null; String json=null; LinkedHashMap partVals={}
  Long firstRowId=null, lastSeq=null
  if (partitioned && f.partition() instanceof PartitionData pd && pd has fields):
      specId = f.specId(); spec = table.specs().get(specId)
      json   = IcebergPartitionUtils.getPartitionDataJson(pd, spec, zone)
      idMap  = IcebergPartitionUtils.getIdentityPartitionInfoMap(pd, spec, table, zone)
      for k in orderedKeys: if idMap.containsKey(k): partVals.put(k, idMap.get(k))
  if (formatVersion >= 3):
      firstRowId = f.firstRowId() != null ? f.firstRowId() : -1
      lastSeq    = (f.fileSequenceNumber() != null && f.firstRowId() != null) ? f.fileSequenceNumber() : -1
  range = new IcebergScanRange.Builder()
            .path(f.path()).start(task.start()).length(task.length()).fileSize(f.fileSizeInBytes())
            .fileFormat(fmt).formatVersion(formatVersion)
            .partitionSpecId(specId).partitionDataJson(json)
            .firstRowId(firstRowId).lastUpdatedSequenceNumber(lastSeq)
            .partitionValues(partVals).build()
```
**`getScanNodeProperties`** (new override) — resolve table (auth-wrapped), emit:
- `file_format_type = "jni"` (parent default; per-range override to native)
- `path_partition_keys = String.join(",", getIdentityPartitionColumns(table))` — **only if non-empty** (lowercased already)

(`location.*`, `serialized_table`, `paimon.predicate`-analog, schema-evolution → later tasks.)

**`getFormatVersion(Table)`** — port the T09 body (BaseTable.operations().current().formatVersion() else `format-version` prop else 2).

## Tests (no Mockito, fail-loud; real iceberg SDK objects)
- **`IcebergPartitionUtilsTest`** (primary parity oracle): `serializePartitionValue` per-type matrix incl. timestamptz UTC→session-zone shift + non-canonical-zone (`"CST"`) crash-free; `getIdentityPartitionColumns` (multi-spec union, identity-only, lowercase, dedup); `getIdentityPartitionInfoMap` (skip binary/fixed + non-identity, lowercase keys, null value passthrough); `getPartitionDataJson` (all fields, JSON array, null→`null`).
- **`IcebergScanRangeTest`** (extend): `populateRangeParams` → assert every `TIcebergFileDesc` field by version (v1 content; v2 no content, no v3 fields; v3 first_row_id/seq incl `-1` fallback), `original_file_path`, `partition_spec_id`/`partition_data_json` present-vs-absent, `format_type` ORC/PARQUET per fileFormat, `table_level_row_count == -1`, `iceberg_params` attached; columns-from-path keys/values/isNull (ordered, null→`""`+isNull) and **unset** when `partitionValues` empty (pre-set lists cleared).
- **`IcebergScanPlanProviderTest`** (extend): partitioned `InMemoryCatalog` table with identity int partition + appended `DataFiles` (orc and parquet) → `getScanNodeProperties` has `path_partition_keys` (lowercased/ordered) + `file_format_type=jni`; unpartitioned table → no `path_partition_keys`; `planScan` ranges carry per-file `fileFormat` (orc vs parquet), `formatVersion`, `partitionSpecId`/`partitionDataJson`/`partitionValues`; v3 table → `firstRowId`/`lastUpdatedSequenceNumber`. End-to-end: run a range's `populateRangeParams` and assert the thrift.

## Deviations (UT-invisible parity notes, P6.6 docker)
- **Typed carriers vs paimon string-props** (cleaner; behavior identical).
- **Per-file fileFormat vs legacy table-uniform** (more correct on mixed-format tables; legacy latent wrong-reader bug; node `file_format_type=jni` + per-range override = paimon parity).
- **`JsonUtil.mapper()` (Jackson) vs `GsonUtils.GSON`** for `partition_data_json` (both valid JSON arrays; BE re-parses → identical values; escaping byte-form may differ for exotic string partition values, immaterial).
- **`ZoneId` (alias-map-resolved) vs legacy raw `ZoneId.of(String)`** in partition timestamp serialization — fixes legacy crash on non-canonical `time_zone`; identical for canonical zones (consistent with T02).
- **columns-from-path unset-then-set** preserved from legacy (NOT paimon, which omits unset); no `__HIVE_DEFAULT_PARTITION__` sentinel (iceberg uses the `is_null` list).
- **`delete_files` left unset for v2+** (legacy sets an empty `ArrayList` for v2+). Adversarial review (skeptic-verified) confirmed BE treats an unset optional thrift list identically to an empty one for a no-delete table, so this is safe; the list is populated in T04. (Decision: leave unset rather than scope-creep an empty list into T03.)
- **Bug 1 / Bug 2 fixes** (adversarial review, both verified against source): the unsupported-format fail-loud guard and the `isPartitionBearing()` empty-partition-map fix (see Architecture decisions 4–5). Bug 2 adds the one non-breaking SPI default method; Bug 1's exception type is `IllegalStateException` (connector cannot import fe-core `DdlException`), message byte-identical to legacy.
- Deletes/COUNT/field-id/MVCC/vended/EXPLAIN deferred to their tasks (their thrift fields left unset this task).

## TODO (ordered)
1. `IcebergPartitionUtils` (port 5 helpers, Gson→JsonUtil, String tz→ZoneId) + `IcebergPartitionUtilsTest` (TDD RED→GREEN).
2. `IcebergScanRange`: typed Builder/fields + `getPartitionValues()` + `populateRangeParams` + `IcebergScanRangeTest`.
3. `IcebergScanPlanProvider`: `getFormatVersion`, carrier population in `planScan`, `getScanNodeProperties` override + extend `IcebergScanPlanProviderTest`.
4. Gate: fe-connector-iceberg UT green, checkstyle 0, import-gate clean, iceberg still NOT in `SPI_READY_TYPES`, no pom change. Adversarial parity review vs legacy.
5. Update HANDOFF + memory; commit (path-whitelisted).
