# P6 — Paimon Connector Full-Path Clean-Room Review (2026-06-18)

**Scope.** Parity review of the *current* Paimon catalog connector (`fe/fe-connector/fe-connector-paimon`, forbidden from importing fe-core) **plus the generic engine bridge** (`org.apache.doris.datasource.PluginDriven*`, `org.apache.doris.connector.*`) against the **legacy fe-core baseline** (`org.apache.doris.datasource.paimon`, `org.apache.doris.datasource.property.{storage,metastore}`) **and the BE C++ consumers** (`be/src/format/table/paimon_*.{cpp,h}`, `partition_column_filler.h`). "Parity" = connector + generic bridge **together** reproduce the legacy fe-core paimon behavior; a difference is a regression only if the net end-to-end behavior changed.

**Method.** Clean-room multi-agent review with **zero historical priors**: 9 finder lines (grouped into 6 dimensions) traced the code independently → each finding passed through an **adversarial verifier** (verdict: confirmed / partial / refuted, with corrected severity/classification) → a **completeness critic** swept for uncovered surfaces. Findings graded BLOCKER / MAJOR / MINOR / NIT.

**Caveats.**
- The BE C++ side was reviewed **by reading only** (field-presence / wiring verification); no per-backend native read was exercised.
- **No live / docker e2e** was run; `enablePaimonTest=true` suites were not executed.
- All verdicts are **code-reasoning**, not runtime observation.
- `refuted` findings are excluded from headline counts and the main body; they appear in the appendix.

---

## Executive summary

### Counts by severity (confirmed + partial; wave 1 + wave 2 consolidated)

| Severity | Count |
|----------|-------|
| BLOCKER  | 2 |
| MAJOR    | 2 |
| MINOR    | 16 |
| NIT      | 10 |
| **Total** | **30** |

(The wave-1 dimension sections below carry 23 findings; wave 2 — the coverage-gap closure pass, see §Wave 2 — added 7 more. **No new BLOCKER/MAJOR** beyond wave 2's independent re-derivation of C1/C2.)

### Counts by verdict

| Verdict | Count |
|---------|-------|
| confirmed | 27 |
| partial   | 3 |
| refuted   | 3 |
| **Total reviewed** | **33** |

(Partial findings are kept in the body using the verifier's corrected severity/classification. The two BLOCKERs and the two MAJORs below are all confirmed/partial.)

### Every BLOCKER and MAJOR

- **[R1 / residual] BLOCKER** — LIVE cut-over path still depends on legacy `property/metastore/Paimon*MetaStoreProperties` + `PaimonExternalCatalog` constants → NOT deletable in B8.
- **[R2 / residual] BLOCKER** — `property/storage/{S3,OSS,COS,OBS,Minio}Properties` are shared cross-connector infra (≈26 named consumers) → NOT deletable in B8.
- **[C1 / config] MAJOR** (downgraded from BLOCKER by verifier) — MinIO storage backend is unbindable for pure `minio.*`-keyed catalogs → empty storage config → "no file io for scheme s3"; the default/tested `s3.*`-keyed MinIO path is unaffected.
- **[C2 / config] MAJOR** — HDFS-backed paimon catalog drops legacy-derived config keys (`hadoop.config.resources` XML, ipc fallback default, `hdfs.security.authentication`, `hdfs.authentication.*` kerberos aliases, `juicefs.*`) on the catalog-create Configuration path.

### Go / No-Go for B8 legacy deletion

**NO-GO as a blanket deletion.** Two BLOCKERs (R1, R2) identify legacy trees that are **still live or shared**: the `property/metastore/Paimon*` classes wire Kerberos auth for cut-over paimon at runtime, and `property/storage/*` is shared by ~26 cross-connector consumers (iceberg/hive/glue/dlf/storage-vault/load/cloud/policy). B8 may proceed **only for the proven-dead trees** (see §B8 deletion readiness) and **only after** severing the 5 metastore-props imports of `PaimonExternalCatalog` constants and scrubbing dangling javadoc `{@link PaimonSysTable}` references. The two config MAJORs (C1 MinIO, C2 HDFS) do not block B8 deletion mechanically but are open correctness regressions on live read paths that should be fixed before/with the cutover ships. **Wave 2 (coverage-gap closure) independently re-derived C1 and C2** — corroborating both; its skeptic rated MinIO a BLOCKER (severity reconciliation in §Wave 2) — and **narrowed C2**: the `hadoop.config.resources` XML-resource gap is a real MAJOR, but the kerberos-by-alias sub-claim was **refuted** (the per-FS Configuration auth marker is not load-bearing). The other closed surfaces (SHOW PARTITIONS, partitions-TVF, statistics/ANALYZE, `@branch`, MTMV, auth/UGI) are at parity. **The full-path review is now complete.**

---

## Read (scan / split planning → BE)

This dimension was covered by two finder lines: **read-scan** (FE scan/split planning) and **read-be** (FE→BE contract & native/JNI downflow).

### What was reviewed / verified-OK

The full Paimon scan/split path was traced clean-room: connector `PaimonScanPlanProvider` / `PaimonScanRange` / `PaimonIncrementalScanParams` / `PaimonPredicateConverter` / `PaimonTableHandle` / `PaimonTableResolver` plus the generic bridges `PluginDrivenScanNode` / `PluginDrivenSplit`, against legacy `PaimonScanNode` / `PaimonSplit` / `PaimonPredicateConverter` / `PaimonValueConverter` / `PaimonUtil` / `PaimonExternalTable`. Verified at parity (no row/route impact): split enumeration (`ReadBuilder.withFilter+withProjection`), JNI-vs-native routing (3-bool gate), COUNT(*) pushdown (first-arm, single representative range, `-1` DV sentinel), native sub-splitting (`computeFileSplitOffsets` byte-faithful incl. 1.1× tail guard, DV on every sub-range), deletion vectors, predicate pushdown (empty list always emitted to avoid BE NPE, AND-flatten / OR all-or-nothing / IN / IS NULL / LIKE-prefix, UTC TIMESTAMP literal, LTZ/FLOAT/CHAR not pushed), partition-value serialization (byte-faithful, `isNull` from Java-null only), `path_partition_keys` emission (prevents native double-fill DCHECK, CI-968880), incremental `@incr` validation + `FIX-INCR-SCAN-RESET`, time-travel via `Table.copy`, schema-evolution `-1` dict keyed off **requested** columns (CI-969249 invariant), LIMIT not consumed, cpp-reader serialization gated on `instanceof DataSplit`.

On the FE→BE contract: every emitted field (`file_format_type=jni`, `table_format_type=paimon`, `path_partition_keys`, `paimon.serialized_table`, `paimon.predicate` always-incl-empty, `paimon.options_json`, `location.*`, `paimon.schema_evolution`, per-split `paimon.split`/`schema_id`/real `file_format`/`deletion_file`/`row_count`/`columns_from_path*`) was verified consumed by the matching BE reader (`paimon_reader.cpp` / `paimon_cpp_reader.cpp` / `paimon_jni_reader.cpp` / `partition_column_filler.h` / `table_schema_change_helper.h`) against the thrift contract. The `file_format="jni"` legacy bug is fixed; the schema-evolution `-1` invariant and `$ro` schema-dict unwrap are present. **No BLOCKER/MAJOR** — all findings are EXPLAIN-output / split-weight / profile-counter parity deviations that do not change rows returned or scanner routing.

### Findings

| id | title | severity | legacy-class | verdict |
|----|-------|----------|--------------|---------|
| R1 (scan) | Plugin splits get uniform split weight; legacy computed proportional weight | MINOR | regression | confirmed |
| R2 (scan) | EXPLAIN drops legacy `predicatesFromPaimon:` line | MINOR | missing-port | confirmed |
| R3 (scan) | CAST-wrapped predicates not pushed to Paimon (legacy unwrapped CAST) | MINOR | intentional-deviation | confirmed |
| R1 (be) | JNI split `self_split_weight` omitted when weight is 0 | NIT | regression | confirmed |
| R2 (be) | `history_schema_info` emitted as scan-level dict, not per-split-accumulated | NIT | intentional-deviation | confirmed |

#### R1 (scan) — Plugin splits get uniform split weight

- **Severity:** MINOR · **Classification:** regression · **Verdict:** confirmed
- **Location:** `PluginDrivenSplit.java:39-48`; `PaimonScanRange.java:92-94,194-197` (real evidence is here, not the cited `buildNativeRange`); `FileSplit.java:104-117`.
- **Description:** Legacy `PaimonSplit` sets `selfSplitWeight` in both ctors (fileSize-sum for DataSplit/JNI, length for native sub-split, `+deletionFile.length()`) and `PaimonScanNode.getSplits:499` sets `targetSplitSize` on **all** splits, so `FederationBackendPolicy` distributes splits by proportional weight. On the SPI path `PluginDrivenSplit` forwards start/length/fileSize but never sets `selfSplitWeight`/`targetSplitSize`, so every plugin split hits the null branch → `SplitWeight.standard()` (uniform). FE-side load-balancing deviation only; same rows read.
- **Evidence:** `FileSplit.getSplitWeight()`: `if (selfSplitWeight != null && targetSplitSize != null) {...} else { return SplitWeight.standard(); }`. Legacy `PaimonSplit:60` `this.selfSplitWeight = dataFileMetas.stream().mapToLong(DataFileMeta::fileSize).sum();`.
- **Verifier:** Confirmed. BE-side thrift `self_split_weight` is only a profile-condition counter (`jni_reader.h:67`), NOT the scheduling input; both legacy and SPI set it identically only in the JNI branch → exact BE parity. The only delta is FE-side proportional-vs-uniform weighting via `FederationBackendPolicy.getSplitWeight()`. The connector already computes `selfSplitWeight` (`PaimonScanRange.getSelfSplitWeight:169`) but it never reaches `FileSplit`.
- **Remediation:** If parity matters for large-table scheduling, add `getSelfSplitWeight()`/`getTargetSplitSize()` to the SPI `ConnectorScanRange` and populate `FileSplit` fields in the `PluginDrivenSplit` ctor; otherwise document uniform-weight as accepted.

#### R2 (scan) — EXPLAIN drops legacy `predicatesFromPaimon:` line

- **Severity:** MINOR · **Classification:** missing-port · **Verdict:** confirmed
- **Location:** `PluginDrivenScanNode.java:270-275,315-324`; `PaimonScanPlanProvider.java:1116-1129` (appendExplainInfo). Legacy `PaimonScanNode.java:660-668`.
- **Description:** Legacy emitted a `predicatesFromPaimon:` block listing the converted Paimon `Predicate` objects actually pushed to the SDK (or ` NONE`). The SPI path emits a generic `PREDICATES: <doris-sql>` line (raw Doris conjuncts) + `paimonNativeReadSplits=` + VERBOSE `PaimonSplitStats`, but no `predicatesFromPaimon:`. Diagnostically a silently-dropped LTZ/FLOAT/CAST conjunct is no longer observable. No correctness impact; `grep predicatesFromPaimon regression-test` = 0.
- **Evidence:** Legacy `sb.append(prefix).append("predicatesFromPaimon:"); ... for (Predicate predicate : predicates) {...}`. Connector `appendExplainInfo` emits no predicate listing.
- **Verifier:** Confirmed. The generic node retains only counts, not the converted `Predicate` objects, so it cannot trivially reconstruct the line. The only paimon EXPLAIN test (`test_paimon_predict.groovy`) asserts only on `inputSplitNum=N`, so nothing breaks.
- **Remediation:** Re-emit by re-converting the filter (byte-parity with `Predicate.toString()` not reconstructible), or accept as an intentional EXPLAIN simplification recorded in the deviations log.

#### R3 (scan) — CAST-wrapped predicates not pushed to Paimon

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `PaimonConnectorMetadata.java:853-856` (`supportsCastPredicatePushdown=false`); legacy `PaimonPredicateConverter.java:178-200` (unwrap CastExpr). Bridge `PluginDrivenScanNode.buildRemainingFilter:1110-1143`.
- **Description:** Legacy unwrapped `CastExpr`, pushing e.g. `CAST(str_col AS INT)=5` to Paimon as `str_col='5'` exact-equality for source-side pruning — a latent correctness hazard (`'05'`/`' 5'` rows pruned at source, unrecoverable). The connector returns `supportsCastPredicatePushdown=false`, so CAST-bearing conjuncts are stripped before reaching the connector and remain BE-only (re-evaluated on returned rows). **Improves** correctness vs legacy; minor pushdown-selectivity cost.
- **Evidence:** `PaimonConnectorMetadata:854 return false;` with `'05'`/`' 5'` hazard javadoc. Sibling MaxCompute/Jdbc also return false; SPI default is true (`ConnectorPushdownOps:72-74`). Pinned by `PaimonConnectorMetadataTest:228`.
- **Verifier:** Confirmed. Strictly safer; perf-only deviation that improves correctness, not a regression.
- **Remediation:** No action for correctness; record as intentional deviation. Consider lossless-CAST-only pushdown later if selectivity regresses measurably; do not revert to blanket unwrap.

#### R1 (be) — JNI split `self_split_weight` omitted when weight is 0

- **Severity:** NIT · **Classification:** regression · **Verdict:** confirmed
- **Location:** `PaimonScanRange.java:92-94,194-197`. Legacy `PaimonScanNode.java:274`. BE `paimon_jni_reader.cpp:95`, `jni_reader.cpp:62,246-248`.
- **Description:** Legacy unconditionally calls `rangeDesc.setSelfSplitWeight(...)` on the JNI path. The connector stores the weight only when `selfSplitWeight > 0`, so a JNI split with computed weight 0 (non-DataSplit sys split with `rowCount()==0`, or DataSplit with total fileSize 0) leaves it unset → BE reads `-1` instead of 0.
- **Evidence:** `if (builder.selfSplitWeight > 0) { props.put("paimon.self_split_weight", ...); }`.
- **Verifier:** Confirmed; reachable. Impact is profile-only: `_self_split_weight` feeds only the `_max_time_split_weight_counter` diagnostic; never affects rows/counts/predicates/schema. (Finding's evidence has a minor BE line-citation mix-up; both underlying facts hold.)
- **Remediation:** No functional fix required; drop the `> 0` gate if exact profile parity is wanted.

#### R2 (be) — `history_schema_info` emitted as scan-level dict, not per-split-accumulated

- **Severity:** NIT · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `PaimonScanPlanProvider.java:1214-1232` (`buildSchemaEvolutionParam`, call site `:651`). Legacy `PaimonScanNode.java:236-251`. BE `table_schema_change_helper.h:240-266`.
- **Description:** Legacy added history entries lazily, one per distinct file `schema_id` referenced by a split, plus the `-1` entry. The connector emits, once at scan-node level, the `-1` entry **plus** an entry for every committed schema id (`SchemaManager.listAllIds()`). BE needs only the `-1` entry + an entry per split's `schema_id`; `listAllIds()` is a superset, so the "miss table/file schema info" error cannot fire spuriously.
- **Evidence:** `for (Long schemaId : schemaManager.listAllIds()) { history.add(buildSchemaInfo(schemaId, ...)); }`.
- **Verifier:** Confirmed. Paimon schema files are retained (not GC'd like snapshots), so the eager set is always a superset → equivalence holds; only metadata-read cost differs (reads all historical schemas even for single-schema queries). Both sides correctly skip non-data sys tables.
- **Remediation:** Accept as intentional; narrow to planned-split schema_ids only if cost matters on heavily-evolved tables.

---

## Write (INSERT / sink)

Covered by the **write** finder line.

### What was reviewed / verified-OK

Determined that **neither** legacy nor current paimon has a data-write (INSERT / sink / commit) path; the net end-to-end behavior is "INSERT rejected" in both — no data-write regression. Legacy `PaimonMetadataOps` has zero write/commit/`TableWrite` usage and `PaimonExternalCatalog extends ExternalCatalog` directly, so a legacy INSERT hit the catch-all `throw "Load data ... not supported"`. The current `PaimonConnector` declares no write capability (`getCapabilities` = MVCC_SNAPSHOT / TIME_TRAVEL / PARTITION_STATS only; comment "paimon write is not migrated") and does not override `getWritePlanProvider()` (default null); `PaimonConnectorMetadata` overrides zero `ConnectorWriteOps` methods, so `supportsInsert()`/`supportsInsertOverwrite()` default false and `beginInsert`/`getWriteConfig` throw. Every write entry point in the bridge fails loud: plain INSERT at `PhysicalPlanTranslator` (writePlanProvider==null + supportsInsert()==false → AnalysisException), INSERT OVERWRITE at `InsertOverwriteTableCommand.allowInsertOverwrite`, CTAS at the insert stage; redundant guard at `PluginDrivenInsertExecutor.beforeExec:116`. No half-wiring, no capability mismatch.

### Findings

| id | title | severity | legacy-class | verdict |
|----|-------|----------|--------------|---------|
| W1 | No paimon data-write path in current or legacy — INSERT correctly rejected | NIT | n/a | confirmed |
| W2 | INSERT rejection moved from sink-creator to translation-time AnalysisException | NIT | intentional-deviation | confirmed |

#### W1 — No paimon data-write path (no regression)

- **Severity:** NIT · **Classification:** n/a · **Verdict:** confirmed
- **Location:** `PaimonConnector.java:111-119`; `PaimonConnectorMetadata.java:73`.
- **Description:** Recorded only to make the "no write path" determination explicit. Both legacy and current reject INSERT/INSERT OVERWRITE/CTAS-as-write loudly.
- **Verifier:** Confirmed. Minor framing nit: on the SPI branch paimon is a `PluginDrivenExternalCatalog`, so the legacy `UnboundTableSinkCreator` throw is bypassed and rejection is downstream at the translator (which the finding also cites). Net conclusion holds.
- **Remediation:** None. Optionally add a regression test asserting INSERT INTO paimon raises a clear error to lock the contract.

#### W2 — INSERT rejection moved to translation-time AnalysisException

- **Severity:** NIT · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `PhysicalPlanTranslator.java:655-683`; `UnboundTableSinkCreator.java` (DML overloads `:102/:106`, overwrite `:140/:145`).
- **Description:** Because paimon is now a `PluginDrivenExternalCatalog`, an INSERT routes to `UnboundConnectorTableSink` instead of the legacy catch-all throw; `BindSink` binds a `LogicalConnectorTableSink` without checking capability; rejection surfaces at translation (`supportsInsert()==false` → `throw AnalysisException("Connector ... (type: paimon) does not support INSERT operations")`). User-visible outcome unchanged; only stage/wording differ. Backup guard at `PluginDrivenInsertExecutor:116`.
- **Verifier:** Confirmed. The finding's primary citation (`:65/:68`) is the no-DML overload; a real INSERT uses the DML overloads, which behave identically — strengthens the finding.
- **Remediation:** None; message is arguably clearer than legacy.

---

## DDL (catalog / db / table / CTAS) and config

Covered by three finder lines: **ddl-catalog** (CREATE/DROP CATALOG & DATABASE), **ddl-table** (CREATE/DROP TABLE, CTAS, type mapping), and **config** (storage credentials + metastore connection assembly).

### What was reviewed / verified-OK

**Catalog/DB dispatch.** `CatalogFactory.createCatalog` routes `paimon` through `SPI_READY_TYPES` → `PluginDrivenExternalCatalog`; metastore flavor resolved from `paimon.catalog.type` (default `filesystem`, lowercased) byte-equivalently to legacy. Unknown flavor rejected at CREATE CATALOG (`MetaStoreProviders.bind` → `IllegalArgumentException` → `DdlException`), functionally equivalent to legacy. **CREATE/DROP DATABASE are dispatched** (the legacy no-op bug class is closed): `PluginDrivenExternalCatalog` overrides `createDb`/`dropDb`/`createTable`/`dropTable` despite `metadataOps==null`, reaching the connector. HMS-only create-db-with-props gate, force-cascade drop, and exception wrapping mirror legacy (pinned by `PaimonConnectorMetadataDbDdlTest`, 10 tests).

**CREATE/DROP TABLE + type mapping.** Doris→Paimon scalar mapping (`PaimonTypeMapping` vs `DorisToPaimonTypeVisitor`) verified byte/behavior-equivalent (CHAR/VARCHAR/STRING→VarChar(MAX), DATETIME→plain TimestampType, DECIMAL families, VARBINARY/VARIANT, unsupported throws in both). Nested-type nullability non-divergent (every Doris nested type is nullable by default; map key forced `.copy(false)` identically). Paimon→Doris read-back, property normalization (strip PK+comment, location→`CoreOptions.PATH`), primary/partition keys, IF NOT EXISTS double-create probing (remote + local two-arm) all at parity. Two documented intentional deviations (COMMENT-clause fallback, blank-PK-token filtering) are safe.

**Config assembly.** Object-store backends **S3 / OSS / COS / OBS** reproduce legacy per-key (S3A impl, endpoint/region, credential providers when AK present, tuning defaults — S3 50/3000/1000, OSS/COS/OBS 100/10000/10000, Jindo OSS, COS `fs.cosn.*`, OBS native/S3A branch, 8 DLF `dlf.catalog.*` keys, endpoint-from-region). All **five metastore flavors** (HMS HiveConf assembly with storage-overlay-before-kerberos ordering, REST re-key + case-sensitive dlf-token rule, JDBC driver registration + create-time security gate, FS) reproduce legacy. Vended creds REST-only path threaded. Two real gaps found below (MinIO, HDFS) plus two minor deviations.

### Findings

| id | title | severity | legacy-class | verdict |
|----|-------|----------|--------------|---------|
| R1 (catalog) | CREATE DATABASE already-exists error code differs | NIT | intentional-deviation | partial |
| R2 (catalog) | Legacy paimon table-cache CacheSpec validations not ported | MINOR | missing-port | confirmed |
| R3 (catalog) | `listDatabaseNames` swallows remote failures → empty | MINOR | intentional-deviation | confirmed |
| R1 (table) | CREATE TABLE remote-only existing table loses MySQL errno 1050 | MINOR | regression | confirmed |
| R3 (table) | Auto/expression partition rejected where legacy silently stripped | MINOR | intentional-deviation | confirmed |
| C1 (config) | MinIO storage backend unbindable for pure `minio.*` catalogs | MAJOR | regression | partial |
| C2 (config) | HDFS catalog-create drops legacy-derived config keys | MAJOR | missing-port | confirmed |
| C4 (config) | HMS socket timeout hardcoded 10s, ignores `hive_metastore_client_timeout_second` | MINOR | missing-port | confirmed |

(`ddl-table R2` and `config C3` were refuted — see appendix.)

#### R1 (catalog) — CREATE DATABASE already-exists error code differs

- **Severity:** NIT · **Classification:** intentional-deviation · **Verdict:** partial
- **Location:** `PaimonConnectorMetadata.java:789-806`; `PluginDrivenExternalCatalog.java:354-380` (gate at `:368`).
- **Description (as filed):** Bridge consults remote `databaseExists` only when `ifNotExists` is true; a plain CREATE DATABASE on an existing-remote/cache-absent db falls through to `createDatabase(ignoreIfExists=false)`, paimon throws `DatabaseAlreadyExistException`, wrapped as generic `DdlException`. Filed as "ERR_DB_CREATE_EXISTS (1007) vs generic DdlException".
- **Verifier (partial — corrected):** Mechanism real and grade right, but the **error-code premise is inaccurate**: legacy `performCreateDb` does call `ERR_DB_CREATE_EXISTS`, but that throw happens inside `createDbImpl`'s own `catch(Exception)` which re-wraps via `DdlException(String, Throwable)` — which does **not** set the MySQL code. So legacy also loses code 1007 before the client sees it. The only real divergence is **message-string** wording, not an error code.
- **Remediation:** Acceptable as-is; surface a dedicated message only if client tooling depends on the exact text.

#### R2 (catalog) — Legacy paimon table-cache CacheSpec validations not ported

- **Severity:** MINOR · **Classification:** missing-port · **Verdict:** confirmed
- **Location:** legacy `PaimonExternalCatalog.java:161-170` vs current `PluginDrivenExternalCatalog.java:162-173`.
- **Description:** Legacy validated `meta.cache.paimon.table.{enable,ttl-second,capacity}` at CREATE CATALOG via `CacheSpec.check*` (throws `DdlException` for malformed values). The plugin path's `checkProperties` does not re-run these; a malformed value (`capacity=-5`, `ttl-second=abc`) that legacy rejected is now accepted.
- **Verifier:** Confirmed. Blast radius correctly bounded — these keys are **dead on the plugin path** (`PaimonExternalMetaCache`/`ExternalMetaCacheMgr.paimon` have zero non-legacy callers; plugin path uses the generic schema cache), so even well-formed values are no-ops. No query-correctness impact.
- **Remediation:** Prefer warn-and-strip at create (keys are confirmed dead); restoring full validation would re-impose checks on knobs that no longer do anything.

#### R3 (catalog) — `listDatabaseNames` swallows remote failures → empty

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `PaimonConnectorMetadata.java:95-104` vs legacy `PaimonMetadataOps.java:336-342`.
- **Description:** Legacy rethrew a metastore enumeration failure as `RuntimeException`; current catches `Exception`, `LOG.warn`s (no catalog name), returns `emptyList()`. A transient remote failure now presents as "zero databases" (empty SHOW DATABASES) rather than an error. Reaches the user via `PluginDrivenExternalCatalog.listDatabaseNames` → DB-init.
- **Verifier:** Confirmed. **Caveat:** the finding's mitigating claim of a "shared best-effort convention" is factually wrong — Hive/Hudi/JDBC/MaxCompute/Trino all propagate; Iceberg's emptyList is only for a structural unsupported-namespaces case. Paimon is the **sole** connector swallowing a generic remote failure. Low impact (diagnosability/UX only).
- **Remediation:** Keep best-effort if intended but include catalog name in the `LOG.warn` (legacy message did); or distinguish empty-catalog from remote-error and rethrow the latter.

#### R1 (table) — CREATE TABLE remote-only existing table loses MySQL errno 1050

- **Severity:** MINOR · **Classification:** regression · **Verdict:** confirmed
- **Location:** `PluginDrivenExternalCatalog.java:298-321`; `PaimonConnectorMetadata.java:725-739`. Legacy `PaimonMetadataOps.java:189-197`.
- **Description:** Legacy reported `ERR_TABLE_EXISTS_ERROR` (1050 / 42S01) when the table existed remotely and no IF NOT EXISTS. The bridge raises 1050 only for the **local-cache-conflict** arm (`:310-313`); a table existing **only remotely** falls through to `createTable(ignoreIfExists=false)` → `TableAlreadyExistException` → generic `DdlException`. CREATE still fails; only the error code/SQLSTATE/message changed.
- **Evidence:** `if (localExists) { ErrorReport.reportDdlException(ERR_TABLE_EXISTS_ERROR, ...); }` — local arm only; bridge comment self-documents the gap.
- **Verifier:** Confirmed. errno 1050 is a documented MySQL contract some ORMs branch on, so MINOR (not NIT). Reachability narrow (table exists remotely but absent from this FE cache — stale cache / other-FE / external create). The bridge already computes `remoteExists`.
- **Remediation:** Mirror the local arm: `if (remoteExists && !ifNotExists) reportDdlException(ERR_TABLE_EXISTS_ERROR, ...)` before falling through.

#### R3 (table) — Auto/expression partition rejected where legacy silently stripped

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `PaimonSchemaBuilder.java:122-139`; `CreateTableInfoToConnectorRequestConverter.java:125-191`. Legacy `PaimonMetadataOps.java:244` + `PartitionDesc.java:105-157`.
- **Description:** For an expression/auto partition (e.g. `PARTITION BY RANGE(date_trunc(col,...))`) legacy returned the bare underlying column names and silently dropped the transform, creating a table partitioned by the bare column. The converter emits a TRANSFORM field and the connector throws `DorisConnectorException("Paimon only supports identity partition columns, got transform: ...")`. Deliberate, safer deviation (mirrors MaxCompute `identityPartitionColumns`); changes behavior only for the auto-partition edge case (error vs silent partition-by-column).
- **Verifier:** Confirmed full chain (UnboundFunction → `transform=date_trunc` → guard → `DdlException`). Identity and LIST partitions unaffected.
- **Remediation:** Keep the explicit rejection; document as intentional deviation in HANDOFF.

#### C1 (config) — MinIO storage backend unbindable

- **Severity:** MAJOR (downgraded from BLOCKER) · **Classification:** regression · **Verdict:** partial
- **Location:** `S3FileSystemProvider.java:45-73` (supports/aliases); `FileSystemFactory.java:131-141` (no-match → empty list, no throw).
- **Description:** Legacy `MinioProperties` recognized `minio.endpoint/access_key/secret_key/region/session_token/connection.*/use_path_style` (region default `us-east-1`) and produced S3A config. The new path sources storage exclusively from fe-filesystem, which has **no MinIO provider** (registered: local/oss/azure/broker/s3/hdfs/cos/obs); `S3FileSystemProvider.supports()` and `S3FileSystemProperties` aliases include no `minio.*` key. A pure `minio.*` CREATE CATALOG matches no provider → `bindAllStorageProperties` returns empty (no throw) → empty hadoop map → no `fs.s3.impl` → paimon read fails "no file io for scheme s3".
- **Verifier (partial — corrected to MAJOR):** Mechanism and regression confirmed; no `minio.*→s3.*` normalization exists anywhere (grep ZERO). **But BLOCKER overstates impact:** every paimon MinIO regression suite (`test_paimon_table_properties.groovy` etc.) configures MinIO via canonical `s3.endpoint/s3.access_key/s3.secret_key`, which **do** match `S3FileSystemProvider` and work end-to-end. The default/tested MinIO path is unaffected; only the legacy `minio.*`-aliased namespace is broken, with a trivial `s3.*` workaround and zero test coverage demonstrating reliance.
- **Per-key loss (pure `minio.*` only):** endpoint, region(us-east-1), access_key, secret_key, session_token, connection.maximum(100), connection.request.timeout(10000), connection.timeout(10000), use_path_style(false), force_parsing_by_standard_uri. (Mixed configs using generic `s3.*`/`endpoint`/`region` aliases still bind.)
- **Remediation:** Add `minio.*` aliases to `S3FileSystemProperties` endpoint/region/access_key/secret_key/session_token/connection.*/use_path_style and to `S3FileSystemProvider.supports()` (region default `us-east-1`); or add a dedicated MinIO fe-filesystem provider.

#### C2 (config) — HDFS catalog-create drops legacy-derived config keys

- **Severity:** MAJOR · **Classification:** missing-port · **Verdict:** confirmed
- **Location:** `PaimonConnector.java:222-228` (`buildStorageHadoopConfig`) + `PaimonCatalogFactory.java:287-297` (raw passthrough, `:294`); `HdfsFileSystemProperties.java` (does not implement `HadoopStorageProperties`).
- **Description:** Legacy built the HDFS Configuration from `HdfsProperties.getHadoopStorageConfig()` emitting: (1) keys loaded from `hadoop.config.resources` XML; (2) `ipc.client.fallback-to-simple-auth-allowed` default `true`; (3) `hdfs.security.authentication=<authType>`; (4) kerberos `hadoop.*` derived from canonical `hdfs.authentication.{type,kerberos.principal,kerberos.keytab}` aliases; (5) `juicefs.*`. The new catalog-create path builds the Configuration only from `applyStorageConfig`'s raw passthrough of `fs.`/`dfs.`/`hadoop.` keys, so all five are dropped — HA HDFS via xml cannot resolve its nameservice; a catalog using `hdfs.authentication.*` aliases never gets `hadoop.security.authentication=kerberos`.
- **Verifier:** Confirmed. **Scope clarification (does not change severity):** the gap is strictly the **FE-side catalog-create** Configuration; the **BE/scan path is NOT affected** — `PaimonScanPlanProvider.java:620` consumes `sp.toBackendProperties().toMap()`, which for HDFS returns the full backend map (XML, ipc default, hdfs.security.authentication, kerberos-from-alias, juicefs). Kerberos UGI login is FE-injected via `executeAuthenticated`. Genuine connectivity break for HA-HDFS-via-xml + FILESYSTEM/JDBC flavors, gated and with raw-`hadoop.*` workarounds → MAJOR not BLOCKER. No test covers the gap (`PaimonCatalogFactoryTest:218-238` only asserts the reduced raw passthrough).
- **Remediation:** Have `buildStorageHadoopConfig`/`buildHadoopConfiguration` also fold in `sp.toBackendProperties().toMap()` for HDFS (reuses the already-ported map), or implement `HadoopStorageProperties` on `HdfsFileSystemProperties`. Verify a Kerberized HA HDFS paimon catalog with `hdfs.authentication.*` aliases + `hadoop.config.resources` can connect via the plugin path.

#### C4 (config) — HMS socket timeout hardcoded 10s

- **Severity:** MINOR · **Classification:** missing-port · **Verdict:** confirmed
- **Location:** `HmsMetaStorePropertiesImpl.java:179-181`. Legacy `HMSBaseProperties.java:204-208`.
- **Description:** Legacy set the metastore client socket timeout from `Config.hive_metastore_client_timeout_second` when the user had not overridden `hive.metastore.client.socket.timeout`; the connector hardcodes literal `"10"` (the current default). A user-set per-catalog property suppresses the default in both. The only divergence: an operator who raises `fe.conf hive_metastore_client_timeout_second` (e.g. 60) without the per-catalog property gets 60 in legacy but 10 here.
- **Verifier:** Confirmed (guard-key equivalence checked by disassembly). The FE Config value is genuinely unreachable from the connector — `ConnectorContext.getEnvironment()` exposes only doris_home and jdbc_drivers_dir, and metastore-spi has no fe-common dependency. Introduced by the SPI move. Opt-in-tuning-only; default preserved.
- **Remediation:** Thread the FE config value through `ConnectorContext.getEnvironment()` (or a dedicated accessor) instead of literal `"10"`. Low urgency.

---

## Metadata replay (persist / restart / GSON)

Covered by the **replay** finder line. **No findings** (zero defects in this dimension).

### What was reviewed / verified-OK

Persisted-state parity and replay-rebuild were traced clean-room: the bridge persisted objects (`PluginDrivenExternalCatalog/Database/Table`, `PluginDrivenMvccExternalTable`) and runtime carriers (`PluginDrivenMvccSnapshot`, `PluginDrivenSchemaCacheValue`), GSON `RuntimeTypeAdapterFactory` registrations, and base `ExternalCatalog` field/transient layout, against legacy paimon classes + legacy GSON registrations. Verified:

1. **Persisted shape identical** — both legacy paimon classes and current PluginDriven classes add zero `@SerializedName` fields; both persist only the base hierarchy + `catalogProperty` (which carries `type` and `paimon.catalog.type`). No field added/dropped.
2. **All three GSON subtype registrations present and consistent** — Catalog: PluginDriven default + `registerCompatibleSubtype` for all 5 legacy flavor class names. Database: `PaimonExternalDatabase` → `PluginDrivenExternalDatabase`. Table: `PaimonExternalTable` → `PluginDrivenMvccExternalTable` (correctly the MVCC variant). No missing registration → no replay ClassCastException.
3. **Flavor preserved on replay** — both derive flavor solely from `paimon.catalog.type`; collapsing 5 class names onto one PluginDriven catalog loses no flavor info (the legacy `*HMS/*DLF/*File/*Rest` subclasses were `@Deprecated` pass-throughs).
4. **Transient re-init complete** — post-deserialization `connector==null`; `initLocalObjects()` gated on non-persisted `objectCreated` rebuilds connector + transactionManager + executionAuthenticator from `catalogProperty`.
5. **type/logType migration** — `CatalogFactory` force-persists `type=paimon`; `gsonPostProcess` backfills type from logType and migrates `logType PAIMON→PLUGIN`.
6. **MVCC snapshot not persisted** — both snapshots are per-query runtime objects, never GSON-registered.

**Overall:** persistence + replay + GSON serde at parity; no regressions, missing ports, or standalone defects.

---

## Metadata cache (schema / partition / sys-table / mvcc)

Covered by the **cache** finder line.

### What was reviewed / verified-OK

Reviewed the connector (`PaimonConnectorMetadata`, `PaimonTableResolver`, `PaimonCatalogOps`) and the bridges (`PluginDrivenExternalTable`, `PluginDrivenMvccExternalTable`, `PluginDrivenMvccSnapshot`, `PluginDrivenSysExternalTable`, `ExternalMetaCacheInvalidator`) against the legacy paimon cache stack, tracing the actual fill/hit/invalidate/refresh wiring. The architecture deliberately replaces legacy's two engine-specific caches with (a) a single name-keyed latest-schema entry in the generic `DefaultExternalMetaCache`, and (b) **no second-level partition/snapshot cache** — partition view + snapshot pin + schema-at-snapshot are listed once per query and held on the per-statement `PluginDrivenMvccSnapshot` (the CACHE-P1 design).

Verified at parity: single-pin MVCC consistency within one query; no two-snapshot key collision (time-travel schemas never enter the shared cache); REFRESH TABLE reaches the cache (`forTableIdentity` invalidation → fresh `getTableHandle+getTableSchema`); REFRESH CATALOG nulls+rebuilds the connector; sys-table schema resolution via `getSchemaCacheValue` override (closes the legacy missing-override bug class); no hidden stale Table cache in the connector; partition staleness `Partition.lastFileCreationTime()` → `MTMVTimestampSnapshot`; `isPartitionInvalid` + `getNewestUpdateVersionOrTime` bypass-pin semantics. **No BLOCKER/MAJOR**; deviations are intentional (CACHE-P1) and TTL/REFRESH-bounded with the same effective staleness profile as legacy.

### Findings

| id | title | severity | legacy-class | verdict |
|----|-------|----------|--------------|---------|
| MC1 | Latest schema cached name-only (no schemaId in key) | MINOR | intentional-deviation | confirmed |
| MC2 | Time-travel schema re-resolved per query (no second-level cache) | NIT | intentional-deviation | confirmed |

#### MC1 — Latest schema cached name-only

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `ExternalTable.java:423-426`; `ExternalMetaCacheMgr.java:425-433`. Legacy `PaimonExternalMetaCache.java:73-75` + `PaimonSchemaCacheKey.java`.
- **Description:** Legacy keyed the schema cache by `(NameMapping, schemaId)` and derived the latest schemaId from the latest-snapshot projection. The new model keys by NameMapping only and reads `table.rowType()` of a freshly-resolved handle on each miss. Both TTL-bound and need REFRESH for immediate consistency; no concrete wrong-result scenario. Flagged only because the keying mechanism differs.
- **Verifier:** Confirmed. Time-travel schema IS pinned separately (`loadSnapshot` stores `pinnedSchema`), so two snapshots do not collide on the name-only key. Under async snapshot-cache refresh, legacy could surface an evolved latest schema marginally earlier; new model waits on the schema entry's own TTL/REFRESH — "different staleness shape, no substantiated regression."
- **Remediation:** Optional one-line doc note on `PluginDrivenExternalTable.initSchema()`; no code change.

#### MC2 — Time-travel schema re-resolved per query

- **Severity:** NIT · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `PluginDrivenMvccExternalTable.java:259-271`. Legacy `PaimonExternalTable.java:338-371` + schemaEntry.
- **Description:** Legacy served a FOR VERSION/TIME AS OF schema from the shared `(NameMapping, schemaId)` cache (repeated time-travel hit cache). The new model resolves schema-at-snapshot fresh inside `loadSnapshot` every query and pins it for that statement only — one extra `schemaAt` round-trip per time-travel query, within the CACHE-P1 design. Latest reads still cached via super; correctness preserved.
- **Verifier:** Confirmed; scoped to time-travel only. Pure perf trade.
- **Remediation:** None; a connector-side schemaId-keyed memo could be reintroduced later without touching the bridge if measured overhead warrants.

---

## Residual old logic / fallback (B8 scoping)

Covered by the **residual** finder line.

### What was reviewed / verified-OK

Swept the connector module (18 main classes) and the generic bridge for residual legacy logic, fallbacks, source-name branches, and dead-vs-live status. Verified clean: `tools/check-connector-imports.sh` exits 0 (connector does **not** illegally import fe-core; only doc-comment legacy-parity notes). All connector `instanceof` are legitimate Paimon-SDK/SPI dispatch. Connector `"paimon"` literals are its own identity registration. No force_jni/legacy fallback to fe-core. Generic bridge `getEngine()` switch is connector-agnostic TableType-identity preservation. Cut-over paimon never hits the legacy `instanceof Paimon*`/`Type.PAIMON` branches in Env SHOW-CREATE / `buildDbForInit` / ShowPartitions / UserAuthentication / RouteResolver — all take the PluginDriven branch; the legacy branches are DEAD-but-harmless.

### Findings

| id | title | severity | legacy-class | verdict |
|----|-------|----------|--------------|---------|
| R1 | LIVE path depends on legacy `property/metastore/Paimon*` + `PaimonExternalCatalog` constants | BLOCKER | n/a | confirmed |
| R2 | `property/storage/{S3,OSS,COS,OBS,Minio}Properties` shared cross-connector infra | BLOCKER | n/a | confirmed |
| R3 | Generic bridge source-name-branches VERBOSE per-backend EXPLAIN to paimon only; MaxCompute loses `backends:` block | MINOR | regression | partial |
| R4 | Dead legacy paimon handler/imports in `ShowPartitionsCommand` | MINOR | intentional-deviation | confirmed |
| R5 | Dead legacy paimon branches in Env / ExternalCatalog / UserAuthentication / RouteResolver | MINOR | intentional-deviation | confirmed |
| R6 | `systable/PaimonSysTable` + `metacache/paimon/*` + `ExternalMetaCacheMgr.paimon()` dead-for-cutover but compile-referenced | MINOR | intentional-deviation | confirmed |

#### R1 — LIVE path depends on legacy metastore-props (NOT deletable)

- **Severity:** BLOCKER · **Classification:** n/a · **Verdict:** confirmed
- **Location:** `PluginDrivenExternalCatalog.java:121/130/136-137`; `MetastoreProperties.java:90`; `PaimonPropertiesFactory.java:28-32`; `AbstractPaimonProperties.java:73-84`; `PaimonFileSystemMetaStoreProperties.java:21,82`.
- **Description:** The cut-over `initPreExecutionAuthenticator()` calls `catalogProperty.getMetastoreProperties()` → `MetastoreProperties.create()` → `PaimonPropertiesFactory` which instantiates the legacy `Paimon{FileSystem,HMS,AliyunDLF,Jdbc,Rest}MetaStoreProperties`. `AbstractPaimonProperties.initHdfsExecutionAuthenticator` wires the HDFS Kerberos `HadoopExecutionAuthenticator` — load-bearing for kerberized filesystem/jdbc paimon. These classes also import `datasource/paimon/PaimonExternalCatalog` constants (`PAIMON_FILESYSTEM`/`PAIMON_HMS`).
- **Verifier:** Confirmed. Live runtime + compile dependency both real; deleting `PaimonExternalCatalog` breaks compilation of 5 live metastore-props classes; deleting the metastore-props breaks Kerberos wiring for cutover paimon. Connector import check rc=0 (this is engine-bridge→fe-core-legacy, not a connector violation). HANDOFF.md:36-42 lists exactly these as B8 targets.
- **Remediation:** Before B8: KEEP `property/metastore/Paimon*MetaStoreProperties` + `PaimonPropertiesFactory` + `AbstractPaimonProperties`; migrate the `PAIMON_FILESYSTEM`/etc. constants out of `datasource/paimon/PaimonExternalCatalog` into the metastore-props module **before** deleting `PaimonExternalCatalog`. Do NOT delete `PaimonExternalCatalog` without first severing these 5 imports.

#### R2 — `property/storage/*` shared cross-connector infra (NOT deletable)

- **Severity:** BLOCKER · **Classification:** n/a · **Verdict:** confirmed
- **Location:** `property/storage/{S3,OSS,COS,OBS,Minio}Properties.java` (consumers across iceberg/hive/glue/dlf/storage-vault/load/cloud/policy).
- **Description:** The B8 scope text lists these as deletion candidates, but they are not paimon-specific; the paimon connector uses its own filesystem SPI while the rest of the engine still consumes `property/storage/*`. Deleting them is a whole-engine break.
- **Verifier:** Confirmed. The paimon connector AND even the legacy fe-core paimon tree reference none of these 5 classes. Concrete consumers verified: `DLFCatalog.java:22`, `StoragePolicy.java:20`, `S3StorageVault.java:24`, `BrokerLoadJob.java:29`, `S3ConnectivityTester.java:23`, `IcebergRestProperties.java:48`. Count nuance: finding said 83 (broad `property.storage.*` grep = 122 files repo-wide); the 5 named classes have ~26 named main-source consumers. Either metric → shared infra.
- **Remediation:** Explicitly EXCLUDE `property/storage/*` from the B8 paimon-deletion set; only the connector's own in-module storage was replaced.

#### R3 — Generic bridge source-name-branches VERBOSE EXPLAIN to paimon only

- **Severity:** MINOR (downgraded from MAJOR) · **Classification:** regression · **Verdict:** partial
- **Location:** `PluginDrivenScanNode.java:305-308`; baseline `FileScanNode.java:151-152,253-256`.
- **Description:** `FileScanNode` emits `appendBackendScanRangeDetail()` (the `backends:` block + per-file paths + dataFileNum/deleteFileNum/deleteSplitNum) **unconditionally** for `VERBOSE && !isBatchMode()`. `PluginDrivenScanNode` overrides without calling super and re-emits that block **only when `"paimon".equals(catalog.getType())`**. Legacy `MaxComputeScanNode` (extends `FileQueryScanNode`, no override) DID show the block; after cutover, cut-over MaxCompute VERBOSE EXPLAIN loses it. Direct violation of the project rule that `PluginDrivenScanNode` must not branch on source name for universal `FileScanNode` behavior; the inline comment claiming MaxCompute output stays byte-unchanged is wrong.
- **Verifier (partial — corrected to MINOR):** All facts verified (git baseline + routing). Real regression but **EXPLAIN-VERBOSE diagnostic-only** (no query/data impact) and **no regression test asserts `backends:` for maxcompute** → MAJOR overstates user impact; MINOR is right. **Note: this is in the GENERIC bridge and affects MaxCompute, not a paimon-only concern.**
- **Remediation:** Emit `appendBackendScanRangeDetail()` unconditionally under `VERBOSE && !isBatchMode()` (matching `FileScanNode`); connectors without delete files naturally render deleteFileNum=0 via `getDeleteFiles→empty`. Keep `paimonNativeReadSplits` behind the SPI `appendExplainInfo` delegation. Also fixes the false inline comment.

#### R4 — Dead legacy paimon handler/imports in `ShowPartitionsCommand`

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `ShowPartitionsCommand.java:51-53,211,369-376,480-481,505`.
- **Description:** Cut-over paimon is `PluginDrivenExternalCatalog`, so `:479` (`instanceof PluginDrivenExternalCatalog` → `handleShowPluginDrivenTablePartitions`) always fires first; the parallel `else if instanceof PaimonExternalCatalog` (`:480-481`), its method, the gate clause, the row-width clause, and the 3 legacy imports are DEAD for cutover. Harmless at runtime but blocks deleting the 3 legacy classes (compile dependency).
- **Verifier:** Confirmed. The only `new PaimonExternalCatalog` is in `PaimonExternalCatalogFactory:42` which has **zero callers**; GSON `registerCompatibleSubtype` (`:402-411`) remaps persisted legacy names to `PluginDrivenExternalCatalog` on deserialization — closing the replay path. `handleShowPluginDrivenTablePartitions` reproduces the same 5-column rich result (D-045). Not a mislabeled live dependency → not a B8 BLOCKER.
- **Remediation:** Delete the method + instanceof gate/dispatch/row-width clauses + 3 imports together with the legacy classes; behavior-neutral.

#### R5 — Dead legacy paimon branches in Env / ExternalCatalog / UserAuthentication / RouteResolver

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `Env.java:111-112,4917-4938`; `ExternalCatalog.java:53,956-957`; `UserAuthentication.java:32,58-59`; `ExternalMetaCacheRouteResolver.java:25,69-72`.
- **Description:** Each has a legacy paimon branch DEAD for cutover and a parallel PluginDriven branch that handles it (cut-over tables report `PLUGIN_EXTERNAL_TABLE`; catalog forces `logType=PLUGIN` and overrides `buildDbForInit`; RouteResolver falls to `ENGINE_DEFAULT`). All harmless; each is a compile dependency on a legacy class.
- **Verifier:** Confirmed at every cited file:line. `PaimonExternalCatalogFactory` has 0 consumers; no `case "paimon"` in the CatalogFactory built-in switch; GSON (`403/464/489`) remaps persisted legacy names to PluginDriven classes (replay-safe). Compile-only residuals, behavior-neutral. Classification nuance: better described as residual/dead than a true behavioral deviation (PluginDriven branches reproduce legacy exactly), but the schema lacks a "residual/dead" class so intentional-deviation is retained — labeling nuance only.
- **Remediation:** Remove these branches/imports atomically with the legacy classes in B8.

#### R6 — `systable/PaimonSysTable` + `metacache/paimon/*` + `ExternalMetaCacheMgr.paimon()` dead-for-cutover

- **Severity:** MINOR · **Classification:** intentional-deviation · **Verdict:** confirmed
- **Location:** `systable/PaimonSysTable.java:21-22`; `{PluginDrivenSysTable,NativeSysTable}.java` (javadoc `@link/@see`); `ExternalMetaCacheMgr.java:35,176-178,302`; `metacache/paimon/*`.
- **Description:** Cut-over surfaces sys tables via `PluginDrivenSysTable` (`PluginDrivenExternalTable.getSupportedSysTables:419`), not `PaimonSysTable`; the only refs to `PaimonSysTable` outside its tree are dangling javadoc + the legacy `PaimonExternalTable` consumer (itself a deletion target). `ExternalMetaCacheMgr.paimon()` + `metacache/paimon/*` loaders + `PaimonExternalMetaCache` registration have no consumer outside the legacy tree; cut-over paimon uses `ENGINE_DEFAULT` (PluginDriven classes don't override `getMetaCacheEngine()`). Dead-for-cutover but compile-referenced.
- **Verifier:** Confirmed. Dangling `{@link PaimonSysTable}`/`@see` (`PluginDrivenSysTable.java:27`, `NativeSysTable.java:36`) would break strict javadoc/checkstyle if the class is deleted without scrubbing. Zero runtime risk → safe B8 unit delete, not a BLOCKER.
- **Remediation:** Delete `systable/PaimonSysTable`, `metacache/paimon/*`, `PaimonExternalMetaCache`, `ExternalMetaCacheMgr.paimon()`/`ENGINE_PAIMON` as a unit, but FIRST scrub the dangling javadoc references. Confirm `ENGINE_DEFAULT` is intended steady-state (it is).

---

## Legacy-diff ledger

Consolidated view of every confirmed/partial finding by how it differs from the legacy baseline.

| id | dim | title | severity | classification | intended? |
|----|-----|-------|----------|----------------|-----------|
| R1 (scan) | read | Uniform split weight vs proportional | MINOR | regression | No — unintended FE load-balancing drift |
| R1 (be) | read | JNI `self_split_weight` unset when 0 | NIT | regression | No — but profile-only |
| R2 (table) — see appendix | ddl | (refuted) | — | — | — |
| R1 (table) | ddl | Remote-only CREATE TABLE loses errno 1050 | MINOR | regression | No — unannounced behavior change |
| C1 (config) | config | MinIO `minio.*` unbindable | MAJOR | regression | No — port gap |
| R3 (residual) | residual | MaxCompute loses VERBOSE `backends:` block | MINOR | regression | No — source-name branch bug |
| R2 (scan) | read | EXPLAIN drops `predicatesFromPaimon:` | MINOR | missing-port | Acceptable if logged |
| C2 (config) | config | HDFS catalog-create drops xml/ipc/kerberos-alias/juicefs keys | MAJOR | missing-port | No — must port |
| C4 (config) | config | HMS socket timeout ignores fe.conf override | MINOR | missing-port | No — should thread config |
| R2 (catalog) | ddl | Paimon table-cache CacheSpec validations not ported | MINOR | missing-port | Keys now dead — warn-and-strip |
| R3 (scan) | read | CAST predicates not pushed (safer) | MINOR | intentional-deviation | Yes — improves correctness |
| R2 (be) | read | history_schema_info eager superset | NIT | intentional-deviation | Yes |
| W2 | write | INSERT rejection moved to translation | NIT | intentional-deviation | Yes |
| R1 (catalog) | ddl | CREATE DB already-exists message wording | NIT | intentional-deviation | Yes (no errno loss) |
| R3 (catalog) | ddl | listDatabaseNames swallows remote failure | MINOR | intentional-deviation | Partly — paimon-local, not a shared norm |
| R3 (table) | ddl | Auto/expression partition rejected (safer) | MINOR | intentional-deviation | Yes — safer than silent strip |
| MC1 | cache | Latest schema name-keyed (no schemaId) | MINOR | intentional-deviation | Yes — CACHE-P1 |
| MC2 | cache | Time-travel schema re-resolved per query | NIT | intentional-deviation | Yes — CACHE-P1 |
| R4 (residual) | residual | Dead ShowPartitions legacy handler | MINOR | intentional-deviation | Yes — co-delete in B8 |
| R5 (residual) | residual | Dead Env/ExternalCatalog/Auth/Route branches | MINOR | intentional-deviation | Yes — co-delete in B8 |
| R6 (residual) | residual | Dead PaimonSysTable/metacache-paimon | MINOR | intentional-deviation | Yes — co-delete in B8 |
| W1 | write | No write path (both reject) | NIT | n/a | Yes |
| R1 (residual) | residual | Legacy metastore-props still LIVE | BLOCKER | n/a | N/A — must NOT delete |
| R2 (residual) | residual | property/storage/* shared infra | BLOCKER | n/a | N/A — must NOT delete |

---

## B8 deletion readiness

From the residual dimension. Deciding evidence shown for each.

### DEAD — safe to delete in B8 (co-delete as units; paimon-only)

| Tree / class | Deciding evidence | Caveat |
|--------------|-------------------|--------|
| `datasource/paimon/PaimonExternalCatalog{,Factory}`, `PaimonExternalDatabase`, `PaimonExternalTable`, `PaimonHMSExternalCatalog`, `PaimonDLFExternalCatalog` | `PaimonExternalCatalogFactory` has **0 callers**; no `case "paimon"` in CatalogFactory built-in switch; GSON `registerCompatibleSubtype` (`403/464/489`) remaps persisted legacy names → PluginDriven on replay | `PaimonExternalCatalog` is **import-referenced** by 5 live metastore-props classes (constants) and by `ShowPartitionsCommand`/Env/RouteResolver dead branches — see R1; sever those imports/branches first |
| `systable/PaimonSysTable` | Cut-over uses `PluginDrivenSysTable` (`getSupportedSysTables:419`); no `new PaimonSysTable(` outside legacy tree | Scrub dangling javadoc `{@link PaimonSysTable}` (`PluginDrivenSysTable:27`) and `@see` (`NativeSysTable:36`) before delete or strict javadoc breaks |
| `metacache/paimon/*` (`PaimonExternalMetaCache`, `PaimonTableLoader`, `PaimonPartitionInfoLoader`, `PaimonLatestSnapshotProjectionLoader`), `ExternalMetaCacheMgr.paimon()` + `ENGINE_PAIMON` registration | No consumer outside legacy tree; cut-over paimon uses `ENGINE_DEFAULT` (PluginDriven classes don't override `getMetaCacheEngine()`) | Delete as a unit; verify `ENGINE_DEFAULT` is intended steady-state (it is) |
| Dead legacy branches in `ShowPartitionsCommand`, `Env.getDdl`, `ExternalCatalog.buildDbForInit`, `UserAuthentication`, `ExternalMetaCacheRouteResolver` | Cut-over tables report `PLUGIN_EXTERNAL_TABLE`/`logType=PLUGIN`; PluginDriven branches always fire first; legacy branches unreachable | Remove branches + imports atomically with the legacy classes; behavior-neutral |

### STILL-CONSUMED — must NOT delete in B8

| Tree / class | Consumed by | Evidence |
|--------------|-------------|----------|
| `property/metastore/Paimon*MetaStoreProperties`, `PaimonPropertiesFactory`, `AbstractPaimonProperties` | **LIVE cut-over runtime** (paimon-specific, but on the generic bridge path) | `PluginDrivenExternalCatalog.initPreExecutionAuthenticator` → `MetastoreProperties.create` → `PaimonPropertiesFactory` → `HadoopExecutionAuthenticator` (Kerberos wiring) — R1 |
| `property/storage/{S3,OSS,COS,OBS,Minio}Properties` (+ abstract bases) | **Shared with iceberg/hive/glue/dlf** + storage-vault/load/cloud/policy/connectivity | ~26 named main-source consumers (DLFCatalog, StoragePolicy, S3StorageVault, BrokerLoadJob, S3ConnectivityTester, IcebergRestProperties, …) — R2; paimon connector references none |
| `datasource/paimon/PaimonExternalCatalog` **constants** (`PAIMON_FILESYSTEM`/`PAIMON_HMS`) | 5 live metastore-props classes (compile) | Migrate constants out into the metastore-props module **before** deleting `PaimonExternalCatalog` — R1 |

**Bottom line:** B8 is a phased deletion. The DEAD trees are safe **after** (a) severing the 5 metastore-props imports of `PaimonExternalCatalog` (migrate constants), (b) removing the dead legacy branches in the 5 fe-core call sites, and (c) scrubbing dangling javadoc. The metastore-props and storage trees stay.

---

## Coverage gaps & follow-ups

### Uncovered surfaces (from the completeness critic)

The six dimensions were drawn around the scan-read and DDL-create spines and left several distinct user-observable FE output paths / modalities unexercised. **All 7 were CLOSED by wave 2 (§Wave 2) — outcomes summarized there; the descriptions below are the original gap statements.** None turned out to be wrong; only cosmetic/intentional deviations were found, plus the independent re-derivation of C1/C2:

1. **SHOW PARTITIONS live rich path (cross-dim 3+5+6).** R4 only marked the DEAD legacy `handleShowPaimonTablePartitions` removable; the LIVE branch via `hasPartitionStatsCapability()` (5-column Partition/PartitionKey/RecordCount/FileSizeInBytes/lastModified) was never verified to reproduce legacy values/NULL/ordering — and the column width changed **VARCHAR(60) → VARCHAR(300)** (`ShowPartitionsCommand:509` vs `:516`), an unclassified delta.
2. **`partitions(...)` TVF / `information_schema.PARTITIONS`.** `PartitionsTableValuedFunction` + `MetadataGenerator.dealPluginDrivenCatalog`/`partitionsMetadataResult` for plugin-driven paimon never traced (column set, partitioned-vs-unpartitioned gating, per-partition values).
3. **Statistics / ANALYZE (entirely unscoped).** Column-level `ExternalTable.getColumnStatistic` is not overridden by PluginDriven; `SUPPORTS_PARTITION_STATS` is declared but its only consumer is `hasPartitionStatsCapability` (drives no actual stats collection/CBO); ANALYZE TABLE flow for a plugin-driven external table not traced.
4. **`@branch('name')` time-travel modality (dim 1).** Read finder traced `@incr` + snapshot/timestamp travel + sys-table, but not branch (3-arg branch Identifier, independent schema/snapshots, GSON round-trip of non-transient `branchName`).
5. **MTMV-on-paimon freshness contract (dim 5 partial).** Verified only the `lastFileCreationTime()→MTMVTimestampSnapshot` field map, not the end-to-end MV staleness/refresh-trigger behavior (`getTableSnapshot` version semantics, `getPartitionType` mapping).
6. **Runtime executeAuthenticated/UGI coverage as a contract (cross-dim 1+3+6).** Config keys verified, but not that **every** remote seam is wrapped in `executeAuthenticated` (e.g. `listTableNames`/`listTables`, `listDatabases`, snapshot enumeration, sys-table `getTable`, `getTableStatistics` `rowCount→plan()`). An unwrapped remote call on kerberized HMS would fail at runtime.
7. **BE native reader correctness per storage backend (dim 1 asserted, not exercised).** FE→BE verification was field-presence/wiring only; native-vs-JNI read correctness per backend (S3/OSS/COS/OBS/MinIO/HDFS) × format (ORC/Parquet) × DV+schema-evo not exercised end-to-end. Given C1 (MinIO) and C2 (HDFS), the BE native read on those backends is exactly where the FE-config gaps would surface as read failures, and that config→BE cross-seam was not traced.

### Prioritized fix-task list

1. **C1 (MAJOR / BLOCKER-if-`minio.*`-used)** — Add `minio.*` aliases to `S3FileSystemProperties` + `S3FileSystemProvider.supports()` (preserve MinIO defaults: region `us-east-1`, tuning 100/10000/10000). Confirmed end-to-end by two independent waves (FE catalog-create **and** BE read both broken for `minio.*` keys). Must-fix before cutover ships.
2. **C2 (MAJOR)** — Load `hadoop.config.resources` XML into the HDFS catalog-create Configuration for filesystem/jdbc flavors (have `HdfsFileSystemProperties` expose its already-XML-loaded backend map, or mirror the HMS `loadHiveConfResources`). **Scope is the XML-resource gap only** — wave 2 refuted the kerberos-by-alias sub-claim (per-FS auth marker is not load-bearing; JVM-global `UGI.setConfiguration` governs SASL).
3. **R3 (residual, MINOR)** — Drop the `"paimon".equals` gate on `appendBackendScanRangeDetail`; emit unconditionally under VERBOSE (fixes MaxCompute regression + project-rule violation + false comment).
4. **R1 (table, MINOR)** — Add the `remoteExists && !ifNotExists` arm reporting `ERR_TABLE_EXISTS_ERROR` in the bridge createTable.
5. **C4 (MINOR)** — Thread `hive_metastore_client_timeout_second` through `ConnectorContext.getEnvironment()`.
6. **R2 (catalog, MINOR)** — Warn-and-strip the now-dead `meta.cache.paimon.table.*` keys at create.
7. **R3 (catalog, MINOR)** — Include catalog name in the `listDatabaseNames` `LOG.warn`; decide whether to keep best-effort swallow (paimon-local, not a shared norm).
8. **Coverage follow-ups — CLOSED by wave 2 (§Wave 2).** SHOW PARTITIONS live path, partitions TVF, column-stats/ANALYZE, `@branch` reads, MTMV freshness, `executeAuthenticated` completeness, and the MinIO/HDFS config→BE cross-seam were all traced. All at parity except the C1/C2 re-derivation; the only new items are cosmetic/intentional deviations (document as accepted, no code change).
9. **R1/R2 (be/scan split weight), R2 (be history dict), MC1/MC2, R2 (scan EXPLAIN)** — Document as accepted deviations in the HANDOFF; no code change required.

---

## Wave 2 — coverage-gap closure & reconciliation

The wave-1 completeness critic flagged **7 user-observable surfaces/modalities** that the 6 scan-read/DDL-create dimensions left unexercised. Wave 2 ran a second clean-room adversarial pass (7 finder lines → per-finding adversarial verifier; same zero-priors discipline; 17 agents) to close them. **Outcome: 6 surfaces are at parity (only cosmetic/intentional deviations); the config→BE line independently re-derived C1 (MinIO) and C2 (HDFS).**

### Gap-closure scorecard

| Gap | Surface | Outcome | New findings |
|-----|---------|---------|--------------|
| G1 | SHOW PARTITIONS live rich path | **Parity.** Critic's `VARCHAR(60)→(300)` worry **debunked** — master already used `VARCHAR(300)` for the paimon branch; the `60` is the iceberg/HMS single-column branch. 5 columns = Partition / PartitionKey / RecordCount / FileSizeInBytes / **FileCount** (not "lastModified"); value source, type, width, ordering, null/0 handling all reproduce legacy. | 1 MINOR |
| G2 | `partitions(...)` TVF / `information_schema.PARTITIONS` | **Parity (net improvement).** On master a paimon catalog hit the TVF's "not support catalog" path and emitted *nothing*; the plugin path now emits name rows (HMS single-column contract). No column dropped, no value regression. | 1 MINOR + 1 NIT |
| G3 | Statistics / ANALYZE | **Full parity, 0 findings.** Row-count byte-identical (`PaimonCatalogOps.rowCount` == legacy `fetchRowCount`); column stats `Optional.empty()` in both (neither overrides `getColumnStatistic`); ANALYZE uses the generic `ExternalAnalysisTask` in both; `SUPPORTS_PARTITION_STATS` drives only SHOW PARTITIONS (no CBO/ANALYZE path existed in legacy or was dropped). | none |
| G4 | `@branch('name')` read modality | **Parity.** Branch resolves via the 3-arg Identifier with independent schema/snapshots; scan options reset; predicate/projection apply; sys-table-vs-scan-params rejected identically. | 1 NIT |
| G5 | MTMV-on-paimon freshness contract | **Parity.** `getTableSnapshot` / `getPartitionType` / `getNewestUpdateVersionOrTime` reproduce legacy snapshot/version semantics; the two deltas are paimon-inert (a cross-connector `v>=0` guard; a dropped-table empty-pin unreachable on a real freshness decision). | 1 MINOR + 1 NIT |
| G6 | `executeAuthenticated` / UGI completeness | **No regression.** The connector wraps *every* remote seam legacy wrapped (and a few more); the seams left unwrapped — split planning, `rowCount`, snapshot/time-travel resolution, schema-evolution dict, vended-token read — are **exactly** the seams legacy also left unwrapped. (Resolves the HANDOFF "split-plan RPC outside `executeAuthenticated`" open item: pre-existing, not a regression.) | 1 NIT |
| G7 | config→BE cross-seam (MinIO / HDFS) | **Re-derived C1 + C2** — see reconciliation below. | (C1/C2, already counted) + 1 refuted |

### New findings — all intentional-deviation, no new regressions

| id | gap | title | severity | verdict |
|----|-----|-------|----------|---------|
| W2-G1 | SHOW PARTITIONS | null partition renders `<col>=__HIVE_DEFAULT_PARTITION__` vs legacy `__DEFAULT_PARTITION__` (deliberate, to align prune/scan `IS NULL`) | MINOR | confirmed |
| W2-G2a | partitions-TVF | TVF emits partition NAME only, discarding stats the connector already collects (matches HMS contract; master emitted nothing for paimon) | MINOR | confirmed |
| W2-G2b | partitions-TVF | unpartitioned gating keys on partition COLUMNS (HMS-style) not INSTANCES (affects MaxCompute only, not paimon) | NIT | confirmed |
| W2-G4 | @branch | sys-table+scan-params error text reads "Plugin" not "Paimon" (bridge is connector-agnostic) | NIT | confirmed |
| W2-G5a | MTMV | `getNewestUpdateVersionOrTime` adds an inert `v>=0` cross-connector guard (paimon never emits the `-1` sentinel here) | MINOR | confirmed |
| W2-G5b | MTMV | dropped-table `materializeLatest` returns a `-1` empty pin instead of throwing (unreachable on real freshness path) | NIT | confirmed |
| W2-G6 | auth/UGI | scan/stats/snapshot/vended-token reads run outside `executeAuthenticated` — pre-existing, identical to legacy | NIT | confirmed |

### G7 reconciliation with wave-1 C1 / C2

**MinIO (C1).** Wave 2 independently re-derived the same end-to-end break: a `minio.*`-keyed catalog binds no fe-filesystem provider (`S3FileSystemProvider.supports()` has no `minio.*` alias; no MinIO provider is registered) → empty FE Hadoop config (catalog-create "No FileSystem for scheme s3") **and** empty BE creds (`PaimonScanPlanProvider:617-624` → no `location.AWS_*`). It also found the 2026-06-14 `applyCanonicalMinioConfig` work was **not** carried into this branch (`grep minio fe-connector-paimon` = 0). **Severity conflict (surfaced, not averaged):** wave-1's verifier rated **MAJOR** (every MinIO regression suite uses canonical `s3.*` keys, which bind and work; no test relies on `minio.*`); wave-2's verifier rated **BLOCKER** (a documented legacy config namespace is now fully broken end-to-end on both catalog-create and BE read). **Resolution:** confirmed regression on a *supported-but-untested* config → **must-fix before cutover ships**; treat as BLOCKER *iff* `minio.*` keying is supported in your deployment, else MAJOR with the trivial `s3.*` workaround. The fix is identical (add `minio.*` aliases to `S3FileSystemProvider`/`S3FileSystemProperties`, preserve MinIO defaults: region `us-east-1`, tuning 100/10000/10000).

**HDFS (C2) — scope narrowed.** Wave 2 split C2's two sub-claims:
- **XML resources (confirmed, MAJOR):** an HDFS HA/auth topology that lives only in a `hadoop.config.resources` XML file is **not** parsed into the FE catalog-create Configuration for filesystem/jdbc flavors (`HdfsFileSystemProperties` doesn't implement `HadoopStorageProperties`; the raw passthrough copies the `hadoop.config.resources` *key* verbatim but never loads the XML contents) → nameservice unresolvable at first metadata access. Inline `dfs.*` keys still work; BE scan path unaffected.
- **Kerberos-by-alias (REFUTED):** wave-1 C2 also claimed `hdfs.authentication.*` kerberos aliases are dropped from the FE Configuration and break a strict kerberized NameNode. The aliases *are* mechanically dropped from the per-FS Configuration — **but the impact does not occur**: the authenticator's first `doAs` calls `UserGroupInformation.setConfiguration(kerberosConf)` (JVM-global), so SASL/Kerberos negotiation is gated by the global UGI security state + the doAs UGI, **not** by the per-FileSystem Configuration's `hadoop.security.authentication`. Kerberized HDFS still opens; the missing per-FS marker is cosmetic/defensive. → **C2's required remediation is just the XML-resource fix** (have `HdfsFileSystemProperties` expose its already-XML-loaded backend map to the FE Hadoop-config path); the kerberos-alias UT proposed in wave 1 would assert a non-load-bearing property.

---

## Appendix: refuted findings

| id | dim | title | why refuted |
|----|-----|-------|-------------|
| R2 (table) | ddl | DROP TABLE of non-existent table loses MySQL errno 1109 | The legacy PRODUCTION drop path was base `ExternalCatalog.dropTable`, which short-circuits on local-cache miss and throws the **identical** generic `DdlException("Failed to get table: ...")` — byte-identical to the bridge. The `ERR_UNKNOWN_TABLE` arm in `performDropTable:291` was never reached for a cache-absent table; even in the only reachable case it is swallowed by `dropTableImpl`'s `catch(Exception)` re-wrap (drops the errno). No client-observable errno-1109 ever existed to lose. |
| C3 (config) | config | No-credentials S3 forces AWS-SDK-v2 provider list into `fs.s3a.aws.credentials.provider` | Code description accurate but the load-bearing impact premise is false for this build: hadoop-aws is **3.4.2** (`fe/pom.xml:370,1286`), where S3A is migrated to AWS SDK v2 and `CredentialProviderListFactory` accepts `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider` classes directly. Every emitted SDK-v2 class exposes `create()`, so they ARE consumable — no instantiation failure. The behavior is a deliberate, test-pinned unification shared with iceberg/hive. |
| HDFS-krb-alias (W2-G7) | config | Kerberos via `hdfs.authentication.*` aliases dropped from FE catalog-create Configuration | Mechanically true, but SASL negotiation is gated by JVM-global `UGI.setConfiguration(kerberosConf)` from the authenticator's first `doAs`, not the per-FS Configuration. Kerberized HDFS still opens; the missing marker is non-load-bearing. Narrows wave-1 C2 to the XML-resource gap only. |
| W (n/a) | write | (no refuted write findings) | — |
