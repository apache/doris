# Task List — P5 paimon **rereview2** fixes (2026-06-11)

> **Source**: `plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md` (2nd clean-room round; §9 = cross-check vs round 1).
> **Scope**: the confirmed BLOCKER/MAJOR set from round 2 (+ the critic-surfaced MAJOR). MINOR/NIT bundled at the end.
> **Baseline**: HEAD = `98a73bf7692`. Legacy `datasource/paimon/*` still in tree → use it for side-by-side parity on every fix.
> **Per-fix workflow** (project convention + `step-by-step-fix` skill):
> 1. Design doc → `plan-doc/tasks/designs/P5-fix-<ID>-design.md` (Problem / Root Cause / Design / Impl Plan / Risk / Test Plan).
> 2. **Re-confirm the finding against CURRENT code first** (report is review-only; line numbers may have drifted).
> 3. Implement (minimal, surgical, match style; connector must NOT import fe-core).
> 4. Build + UT (absolute `-f`, read surefire XML + `MVN_EXIT`); add fail-before/pass-after UTs.
> 5. **Independent commit per fix** (see Commit Policy below) → optional `plan-doc/reviews/P5-fix-<ID>-review-rounds.md`.
> 6. Log SPI changes in `01-spi-extensions-rfc.md`; user-signed decisions in `decisions-log.md`; accepted deviations in `deviations-log.md`.

## Commit Policy (read before the FIRST commit)
- HEAD is already committed this round (unlike round-1 which held). Independent per-fix commits are expected.
- **HARD precondition before any `git add`**: scrub `regression-test/conf/regression-conf.groovy` (plaintext Aliyun key) + remove scratch (`.audit-scratch/`, `conf.cmy/`, `META-INF/`, `*.bak`). **Path-whitelist `git add` — never `git add -A`.**
- Current branch `catalog-spi-07-paimon` (not `master`) → committing here is fine.
- Each commit message: `fix: <ID>` + root cause + solution + tests. End with the project Co-Authored-By trailer.

---

## Progress (priority-ordered)

| # | ID | sev | finding | area / file(s) | SPI? | design | impl | build+UT | commit |
|---|----|-----|---------|----------------|------|--------|------|----------|--------|
| 1 | FIX-URI-NORMALIZE | BLOCKER | B-7DV + B-7DF | native data-file + DV path scheme norm (oss/cos/obs/s3a→s3) | **yes** | ✅ | ✅ | ✅ | ✅ `20b19d19dd8` |
| 2 | FIX-STATIC-CREDS-BE | BLOCKER | B-9 | static s3/oss/cos/obs creds → BE as canonical `AWS_*` | **yes** | ✅ | ✅ | ✅ | ✅ `d23d5df9914` |
| 3 | FIX-SCHEMA-EVOLUTION | BLOCKER | B-1a (M-10 deferred) | connector builds `current_schema_id`/`history_schema_info` thrift dict (Design C) | no¹ | ✅ | ✅ | ✅ 222/0/0 | ✅ `667f779af04` |
| 4 | FIX-JDBC-DRIVER-URL | BLOCKER | B-8a + B-8b | resolve+alias `jdbc.driver_url` for BE; enforce security allow-list | no² | ✅ | ✅ | ✅ 232/0/0 | ✅ `2d15b1b7ed7` |
| 5 | FIX-MAPPING-FLAG-KEYS | MAJOR | M-crit | dotted-vs-underscore type-mapping flag keys (wrong type) | no | ✅ | ✅ | ✅ 234/0/0 | ✅ `9dcf6d1a9e5` |
| 6 | FIX-KERBEROS-DOAS | MAJOR | M-8 + M-11 | M-8: wire HDFS authenticator for fs/jdbc (fe-core); M-11: wrap ALL read RPCs in `executeAuthenticated` (connector, full legacy parity) | no³ | ✅ | ✅ | ✅ 248/0/0 + 21/0/0 | ✅ `2b1442fa57a` |
| 7 | FIX-FORCE-JNI-SCANNER | MAJOR | M-1 | honor `force_jni_scanner` session var on connector scan | no | ⬜ | ⬜ | ⬜ | ⬜ |
| 8 | FIX-COUNT-PUSHDOWN | MAJOR* | M-2 | FE-computed `mergedRowCount` / `paimon.row_count` (perf) | maybe | ⬜ | ⬜ | ⬜ | ⬜ |
| 9 | FIX-NATIVE-SUBSPLIT | MAJOR* | M-3 | native ORC/Parquet sub-file splitting (parallelism) | maybe | ⬜ | ⬜ | ⬜ | ⬜ |

`sev*` = round-2 rated MAJOR but round-1 rated **MINOR** (perf-only, correct results) — **user decides severity** (see §P2).
³ #6 SPI corrected `maybe`→**`no`** ([D-052](./decisions-log.md)/[D-053](./decisions-log.md)): M-11 is connector-only (wraps existing `ConnectorContext.executeAuthenticated`, full legacy parity per signed [D-052], superseding the D7=B read-path clause). M-8 adds an **internal fe-core hook** `MetastoreProperties.initExecutionAuthenticator(List<StorageProperties>)` (default no-op, wired in `PluginDrivenExternalCatalog`) — **not** connector SPI (`ConnectorContext`/`Connector` surface unchanged), so 01-spi-extensions-rfc.md is not touched. Scope = filesystem+jdbc only (DLF/REST/HMS excluded, "DLF" clause overstated). True end-to-end doAs is live-Kerberos-e2e only ([DV-031](./deviations-log.md)).
² #4 SPI corrected `maybe`→**`no`** ([D-050](./decisions-log.md)): the fix reuses the **existing** `Connector.preCreateValidation` + `ConnectorValidationContext.validateAndResolveDriverPath` hooks (B-8b) and the existing `paimon.options_json` transport (B-8a) — **zero new SPI surface**, connector-only. Scope = CREATE-time validation parity with the JDBC reference connector; the FE-restart/ALTER/scan-time re-validation gap (pre-existing fe-core, all plugin connectors) is accepted ([DV-028](./deviations-log.md)) + filed as a cross-connector follow-up. BE-side `paimon.jdbc.{user,password,uri}` alias-drop out of scope ([DV-029](./deviations-log.md), BE deserializes the table from `serialized_table`, doesn't rebuild a JdbcCatalog from these).
¹ #3 SPI corrected `yes`→**`no`**: user signed **Design C** ([D-049](./decisions-log.md)) — the connector builds the thrift `TSchema` dict directly from paimon (BE only needs field `id`/`name`/nesting-tag, no Doris `Type`), reusing the existing `populateScanLevelParams` hook → **zero new SPI surface**. M-10 deferred ([DV-026](./deviations-log.md)); eager all-schemas read accepted ([DV-027](./deviations-log.md)).
Legend: ⬜ todo / 🔄 in progress / ✅ done

> **Ordering rationale**: P0 (#1–4) all gate commit. #1+#2 first = broadest blast radius (they break *all* native reads on OSS/COS/OBS/private-S3 — basic cloud usage) and share the same BE-bound scan-property-normalization seam (reuse the `FIX-REST-VENDED` `ConnectorContext` pattern). #3 (B2) is the most *dangerous* failure mode (silent wrong rows) but has a narrower trigger (schema-evolved + native + rename) and a larger SPI surface; **if you weight silent-corruption highest, do #3 first — it is independent of #1/#2.** #4 (JDBC) is isolated to one flavor.

---

## P0 — BLOCKER (commit-gating)

### 1. FIX-URI-NORMALIZE — native data-file + DV paths sent to BE un-normalized
- **Findings**: B-7DF (data file), B-7DV (deletion vector). Failure: native ORC/Parquet + DV reads **fail outright** on `oss://`/`cos://`/`obs://`/`s3a://` warehouses (BE S3 factory only recognizes `s3://`). Pure `s3://`/`hdfs://` unaffected.
- **Connector**: `PaimonScanPlanProvider.java:269-276` (`.path(file.path())` raw), `:281-283` (`builder.deletionFile(df.path(),…)` raw); `PaimonScanRange.java:190-200`.
- **fe-core**: `PluginDrivenSplit.java:65-68` (single-arg, NON-normalizing `LocationPath.of`); `PluginDrivenScanNode`.
- **Legacy parity**: `source/PaimonScanNode.java:295-298` (DV) and `:443` (data file) — both use the **2-arg** `LocationPath.of(path, storagePropertiesMap).toStorageLocation()`.
- **Fix sketch**: connector can't import `LocationPath` → normalize in the fe-core bridge (`PluginDrivenSplit.buildPath` + the DV desc build in `PluginDrivenScanNode`/`PaimonScanRange`) using the storage-properties map, **or** add a `ConnectorContext` path-normalization SPI hook (mirror the `FIX-REST-VENDED` seam). Apply to **both** the data-file path and the DV path.
- **Test**: connector/bridge UT asserting an `oss://` input → `s3://` BE-bound path for both data + DV; live-e2e (OSS warehouse + DV) is CI-gated.

### 2. FIX-STATIC-CREDS-BE — static object-store creds reach BE as RAW keys
- **Finding**: B-9. Static `s3.*`/`oss.*`/`cos.*`/`obs.*` catalog creds are copied verbatim under `location.<rawkey>`; BE native reader wants `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/… → no usable creds → 403 on private buckets. (FIX-REST-VENDED fixed the *vended* seam; FIX-STORAGE-CREDS fixed the *catalog FileIO* seam — this is the **third, static→BE-scan seam**, see review §9.3.)
- **Connector**: `PaimonScanPlanProvider.java:347-356` (`getScanNodeProperties`, raw copy under `location.*`).
- **fe-core**: `PluginDrivenScanNode.java:307-320` (`getLocationProperties` only strips the `location.` prefix — no normalization).
- **Legacy parity**: `source/PaimonScanNode.java:176,650-652`; `AbstractS3CompatibleProperties.java:105-122` (canonical alias → `AWS_*`); BE `s3_util.cpp:146-150`.
- **Fix sketch**: normalize static aliases to BE `AWS_*` before they leave FE — reuse / extend the `ConnectorContext.vendStorageCredentials` normalization tail for static keys, or normalize in the bridge. Also covers the bare `AWS_*`/`access_key` (no `s3.` prefix) case currently dropped entirely.
- **Test**: UT mutating the connector test that currently codifies the raw key (`PaimonScanPlanProviderTest.java:535`) → assert BE-bound `AWS_ACCESS_KEY` present; live-e2e CI-gated (private S3/OSS).

### 3. FIX-SCHEMA-EVOLUTION — native reader loses paimon schema-evolution (+ field-id)
- **Findings**: B-1a (BLOCKER, silent wrong/NULL rows on column rename/reorder via native reader) + M-10 (MAJOR, `Column.uniqueId` left -1 — its root cause; standalone repro refuted but it feeds B-1a's BE contract).
- **Connector**: `PaimonScanRange.java:181-184` (only sets per-file `schema_id`); `PaimonScanPlanProvider.java:276`; `PaimonConnectorMetadata.java:1007-1012` (5-arg `ConnectorColumn`, no field-id); `ConnectorColumnConverter.java:65-70` (→ `uniqueId=-1`).
- **fe-core**: `PluginDrivenScanNode` never calls `ExternalUtil.initSchemaInfo` / sets `current_schema_id` / `history_schema_info`.
- **Legacy parity**: `source/PaimonScanNode.java:169` (`initSchemaInfo(-1L)`), `:285` (`putHistorySchemaInfo` per native split); `ExternalUtil.java:86-92`; `PaimonUtil.getHistorySchemaInfo`; `PaimonExternalTable.java:349-355` + `PaimonUtil.java:318-347` (recursive `updatePaimonColumnUniqueId`, incl. nested ARRAY/MAP/ROW).
- **BE contract (frozen)**: `be/src/format/table/table_schema_change_helper.h:219-236` falls back to `by_parquet_name`/`by_orc_name` when `history_schema_info` is unset; field-id path is `:241-267`.
- **Fix sketch**: (a) thread paimon `DataField.id()` through SPI `ConnectorColumn` (+ nested) → `Column.setUniqueId`; (b) emit `current_schema_id` + per-split `history_schema_info` on the native path via the bridge (`PluginDrivenScanNode` → `ExternalUtil.initSchemaInfo` + per-split schema). Largest SPI surface of the P0 set.
- **Test**: UT asserting the native split params carry `current_schema_id` + history schema; e2e = `test_paimon_full_schema_change.groovy` (rename over ORC/Parquet) CI-gated.

### 4. FIX-JDBC-DRIVER-URL — JDBC flavor driver_url unresolved + unvalidated
- **Findings**: B-8a (raw unresolved `jdbc.driver_url` + dropped `paimon.jdbc.*` alias → `MalformedURLException`) + B-8b (security allow-list / format / secure-path not enforced → arbitrary remote jar in FE JVM; stale "not in SPI_READY_TYPES" disclaimer).
- **Connector**: `PaimonScanPlanProvider.java:549-565` (forwards `jdbc.*` verbatim); `PaimonConnector.java:232-247,206-216,249-287` (`resolveFullDriverUrl` — no validation); `:230` (stale disclaimer).
- **Legacy parity**: `PaimonJdbcMetaStoreProperties.java:164-176` (emits `jdbc.driver_url=getFullDriverUrl(resolved)`), `:190`; `JdbcResource.java:300-329` (format + `checkCloudWhiteList` + `jdbc_driver_secure_path`).
- **Fix sketch**: resolve `driver_url` via `getFullDriverUrl` on the BE-options path + honor the `paimon.jdbc.*` alias (B-8a); enforce the FE security allow-list/format/secure-path — the wired hook is `ConnectorValidationContext.validateAndResolveDriverPath` (jdbc/trino override `preCreateValidation`); paimon must override it (B-8b). Remove the stale disclaimer.
- **Severity note (cross-check)**: round-1 rated the *security* facet PARTIAL ("default `jdbc_driver_secure_path="*"` → legacy also loads any jar"). B-8a (functional `MalformedURLException`) is unambiguous; for B-8b confirm whether to treat as BLOCKER or hardened-config-only — **fold both into one fix regardless.**

---

## P1 — MAJOR (fix or explicitly accept)

### 5. FIX-MAPPING-FLAG-KEYS — type-mapping flags silently dead (wrong column types)
- **Finding**: M-crit (critic-surfaced; **not 3-lens-gated → re-verify first**). Connector reads underscore keys `enable_mapping_binary_as_varbinary` / `enable_mapping_timestamp_tz`; FE/legacy set DOTTED keys `enable.mapping.varbinary` / `enable.mapping.timestamp_tz` → flags stuck false → BINARY→STRING and LTZ→DATETIMEV2 even when the user enabled the mapping.
- **Connector**: `PaimonConnectorProperties.java:39,42`; read `PaimonConnectorMetadata.java:1017-1027`; consumed `PaimonTypeMapping.java:130-165`. **Legacy**: `CatalogProperty.java:50,52`; `ExternalCatalog.setDefaultPropsIfMissing:302-306`; `PaimonUtil.paimonPrimitiveTypeToDorisType:253,257,283-286`.
- **Fix sketch**: read the dotted keys the FE actually sets (and reconcile the renamed `varbinary` key), or normalize dots→underscores in `PluginDrivenExternalCatalog.createConnectorFromProperties` before constructing the connector. Pure connector/FE-wiring; no BE.
- **Test**: UT constructing the connector with `{"enable.mapping.timestamp_tz":"true"}` → assert LTZ column maps to TIMESTAMPTZ (closes critic coverage-gap #2).

### 6. FIX-KERBEROS-DOAS — UGI doAs lost on fs/jdbc ops + partition listing
- **Findings**: M-8 (filesystem/jdbc over Kerberized HDFS lose `doAs` — `initializeCatalog` dead on cutover path; HMS unaffected) + M-11 (MTMV / SHOW PARTITIONS / partitions-TVF partition listing runs the `listPartitions` RPC without `doAs` on Kerberos HMS). Grouped: same authenticator mechanism.
- **Connector**: `PaimonConnector.java:124-196` (M-8); `PaimonCatalogOps.java:249-251`, `PaimonConnectorMetadata.java:892-894` (M-11). **fe-core**: `PluginDrivenExternalCatalog.java:122-137,150`; `PluginDrivenMvccExternalTable.java:157`; `PluginDrivenExternalTable.java:317-318`.
- **Legacy parity**: `PaimonFileSystemMetaStoreProperties.java:40-57`, `PaimonJdbcMetaStoreProperties.java:111-135` (M-8); `PaimonExternalCatalog.java:96-118` (`executionAuthenticator.execute` wrap), `metacache/paimon/PaimonPartitionInfoLoader.java:49` (M-11).
- **Fix sketch**: wire the fs/jdbc HDFS authenticator on the live (connector) create path; wrap the partition-listing read RPC in `executeAuthenticated` (note round-1 D7=B deliberately left read-vs-DDL asymmetric — confirm whether to wrap reads too). Scope = secured HMS/HDFS deployments. **Verify the M-8 "DLF" clause** (review says it's overstated; DLF inherits the no-op authenticator).

### 7. FIX-FORCE-JNI-SCANNER — `force_jni_scanner` session var ignored
- **Finding**: M-1. Connector reads only `paimonHandle.isForceJni()` (binlog/audit flag), never the session `force_jni_scanner`; native always chosen for ORC/Parquet. The JNI escape hatch (used to dodge native-reader bugs — incl. the B2 schema-evolution one) is gone.
- **Connector**: `PaimonScanPlanProvider.java:261,439-441` (`shouldUseNativeReader`). **Legacy**: `source/PaimonScanNode.java:361,430` (`sessionVariable.isForceJniScanner()` gate).
- **Fix sketch**: read `force_jni_scanner` from the session-properties map (the var is already in it — connector reads sibling `enable_paimon_cpp_reader` from there) and route all data splits to JNI when set. Pure connector.

---

## P2 — Severity-disputed MAJOR (perf-parity; round-1 = MINOR) — **user decides scope**

> Both are correct-results, perf/parallelism-only. Recommend **accept-or-defer** unless perf parity is required for cutover. If deferring, log in `deviations-log.md`.

### 8. FIX-COUNT-PUSHDOWN — `COUNT(*)` pushdown not implemented (M-2)
- Connector never computes `mergedRowCount` / emits `paimon.row_count` → BE materializes merged rows to count (esp. costly on PK tables). `PaimonScanPlanProvider.java:186-296` vs `source/PaimonScanNode.java:396,421-429,483-495`.

### 9. FIX-NATIVE-SUBSPLIT — native sub-file splitting lost (M-3)
- One split per RawFile; large ORC/Parquet files get a single scanner. `PaimonScanPlanProvider.java:263-286` vs `source/PaimonScanNode.java:434-465` (`determineTargetFileSplitSize` + `fileSplitter.splitFile`). See also critic coverage-gap on split-count accounting (P3).

---

## P3 — Coverage gaps to verify/close (completeness critic; NOT confirmed bugs)

> These are "go check", not fixes. Convert to a FIX-task only if a real divergence is found.

- **VERIFY FIX-HMS-CONFRES**: round-2 did **not** re-test `hive.config.resources` / hive-site.xml downflow into BE-facing scan props (the round-1 MAJOR's fix). Confirm it reaches `getScanNodeProperties` for HMS/DLF.
- **TRACE DDL write parity**: `PaimonConnectorMetadata.createTable/dropTable/createDatabase/dropDatabase` (`:683-797`) vs legacy `PaimonMetadataOps`; branch/tag DDL write (`ExternalCatalog.java:1427-1513`); IF-(NOT-)EXISTS short-circuit, editlog/cache-refresh ordering, error-code parity.
- **TRACE ANALYZE / column-stats**: `ExternalAnalysisTask` / `getColumnStatistic` parity (fetchRowCount itself already confirmed faithful).
- **CHECK split-count accounting** under lost splitting (`SqlBlockRuleMgr` limits, batch-mode) — ties to #9.

---

## P4 — MINOR / NIT (low-priority cleanup; full list in review §5)

Bundle as one optional cleanup pass after P0–P1. Most are display-only (DESC `Key`/`Extra`/`uniqueId`, VARCHAR(65533)→STRING, EXPLAIN delete-split accounting, error-message text), perf/architectural (cache granularity), or benign. **The one with a real (rare) data edge**, worth a deliberate decision:
- Partition null-sentinel coercion: a STRING partition whose literal value is `__HIVE_DEFAULT_PARTITION__` or `\N` is coerced to NULL (connector) vs read as the literal (legacy). `PaimonScanRange.java:212-225` / `ConnectorPartitionValues.java:32-54` vs `source/PaimonScanNode.java:323-326`.

---

## Notes / gates (reuse)
- maven: absolute `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`; verify via surefire XML + `MVN_EXIT`. `-pl :fe-connector-paimon -am` does **not** rebuild fe-core; fe-core changes need `-pl :fe-core -am`.
- Connector import gate: `bash tools/check-connector-imports.sh` (must stay clean — drives the "SPI?" column: B1/B3/B2 need fe-core-side or new `ConnectorContext` SPI seams because the connector can't import `LocationPath`/`StorageProperties`).
- cwd persists across Bash calls; `cd` breaks relative paths → always absolute.
- Tests: prefer runnable FE **unit tests** (connector harness: `FakePaimonTable` / `RecordingPaimonCatalogOps` / `RecordingConnectorContext` / `PaimonScanPlanProviderTest`). Live-e2e (S3/OSS/REST/JDBC/Kerberos) is CI-gated — note it as gated, don't claim it ran.
- Re-confirm each finding against current code before editing (review is read-only; lines may have drifted).
