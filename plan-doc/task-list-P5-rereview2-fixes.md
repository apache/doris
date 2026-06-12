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
| 7 | FIX-FORCE-JNI-SCANNER | MAJOR | M-1 | honor `force_jni_scanner` session var on connector scan | no | ✅ | ✅ | ✅ 250/0/0 | ✅ `05132a42668` |
| 8 | FIX-COUNT-PUSHDOWN | MAJOR* | M-2 | FE-computed `mergedRowCount` / `paimon.row_count` (perf); SPI count-pushdown overload + fe-core forward + connector collapse-to-one | **yes** | ✅ | ✅ | ✅ 252/0/0 + fe-core | ✅ `525be03371c` |
| 9 | FIX-NATIVE-SUBSPLIT | MAJOR* | M-3 | native ORC/Parquet sub-file splitting (parallelism); connector-side port of FileSplitter + determineTargetFileSplitSize | no | ✅ | ✅ | ✅ 258/0/0 | ✅ `2f5f467f53d` |

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
- **✅ DONE** `05132a42668` (design [`P5-fix-FORCE-JNI-SCANNER-design.md`](./tasks/designs/P5-fix-FORCE-JNI-SCANNER-design.md)). Re-verified vs current code (4-scout + synthesizer workflow); finding confirmed, current sites = `PaimonScanPlanProvider.java:295` (router) + `:436` (schema-evo emit gate). **Site A** (correctness): new `isForceJniScannerEnabled(session)` (mirror of `isCppReaderEnabled`, key `force_jni_scanner`) → `shouldUseNativeReader` gains an explicit `forceJniScanner` param (mirrors legacy's 3-boolean gate `PaimonScanNode.java:430` 1:1; handle name-force is OR-sibling, never replaced). **Site B** (correctness-neutral): suppress the native-only `paimon.schema_evolution` dict when force-JNI (BE consumes it only on native ORC/Parquet ranges — verified `paimon_reader.cpp`/`file_scanner.cpp:1045-1058`). Pure connector, **zero SPI**, no fe-core import, no BE param (legacy serializes none). UT 250/0/0 (+1 CI skip), fail-before two-test-red verified, import-gate + checkstyle clean. Real BE reader selection = CI-gated live-e2e only.

---

## P2 — Severity-disputed MAJOR (perf-parity; round-1 = MINOR) — **user decides scope**

> Both are correct-results, perf/parallelism-only. Recommend **accept-or-defer** unless perf parity is required for cutover. If deferring, log in `deviations-log.md`.

### 8. FIX-COUNT-PUSHDOWN — `COUNT(*)` pushdown not implemented (M-2)
- **✅ DONE** `525be03371c` (design [`P5-fix-COUNT-PUSHDOWN-design.md`](./tasks/designs/P5-fix-COUNT-PUSHDOWN-design.md), [D-054](./decisions-log.md), [DV-032](./deviations-log.md), [RFC §25 E15](./01-spi-extensions-rfc.md)). User signed off (2026-06-12): **proceed** + **connector collapse-to-one**. Adversarial review `wf_6ead7c2c-b58`: 1 MAJOR (degenerate single-split test) caught+fixed → strengthened to a 2-partition asymmetric-count (2+3=5) fixture pinning collapse N→1 + cross-split sum; 2 MINORs refuted (batch-path moot, EXPLAIN count-line cosmetic).
- Recon (`wf_1ce48c93-325`) re-verified vs current code: the emit seam (`PaimonScanRange.Builder.rowCount`→`paimon.row_count`→`setTableLevelRowCount`) AND the COUNT enum→BE path are **already built**; only the **signal+compute** was missing → **NOT pure-connector** (corrected the initial framing). `dataSplit.mergedRowCount()` is SDK-only (connector); the `getPushDownAggNoGroupingOp()==COUNT` signal lives only on the fe-core node and reached nobody.
- **Fix (3 files):** SPI `ConnectorScanPlanProvider` +1 default 7-arg `planScan(...,boolean countPushdown)` (delegates to 6-arg; other connectors no-op) [E15]; fe-core `PluginDrivenScanNode.getSplits` reads the agg-op and forwards (no post-loop math); connector `PaimonScanPlanProvider` extracts `planScanInternal(...,countPushdown)` + count short-circuit first-arm + static `isCountPushdownSplit` + `buildCountRange` (**collapse-to-one**: sum eligible `mergedRowCount`, emit ONE JNI count range bearing the total = legacy's ≤10000 case). Param=`boolean`, paimon-only (engineering calls). legacy `>10000` parallel-split trim intentionally dropped → [DV-032].
- **Gates:** connector 252/0/0 (1 CI-gated live skip), fe-core compile + checkstyle 0, import-gate clean, **fail-before exactly the 2 new tests red** (neuter `isCountPushdownSplit`→false), end-to-end real-local-PK-table test asserts collapse-to-one carrying the merged total (2). Real BE CountReader selection = CI-gated live-e2e (legacy paimon count regression covers the BE contract; no BE change).

### 9. FIX-NATIVE-SUBSPLIT — native sub-file splitting lost (M-3)
- **✅ DONE** `2f5f467f53d` (design [`P5-fix-NATIVE-SUBSPLIT-design.md`](./tasks/designs/P5-fix-NATIVE-SUBSPLIT-design.md), [D-055](./decisions-log.md), [DV-033](./deviations-log.md)). User signed off (2026-06-12): **implement now**.
- Recon (`wf_ad764bf6-1c9`): real gap (ORC/Parquet are PLAIN/splittable, legacy *does* sub-split); DV × sub-split is **SAFE** (DV rowids are global file positions; BE readers report global positions in a partial range; same DV on every sub-range, no offset re-basing, no guard); **pure-connector, zero SPI, zero fe-core** (the splitter math + 5 session vars re-stated with plain longs; only the specified-size `FileSplitter` branch is reachable).
- **Fix (1 file):** connector `PaimonScanPlanProvider` — 5 file-split session-var constants, 2 pure statics (`computeFileSplitOffsets` byte-exact port incl. the `>1.1D` tail guard; `determineTargetSplitSize` = `determineTargetFileSplitSize` + `applyMaxFileSplitNumLimit`, batch branch omitted), `sessionLong` + lazy `resolveTargetSplitSize`, native-arm sub-split loop, `buildNativeRange(+start,+length)`.
- **Gates:** connector 258/0/0 (1 CI-gated live skip), checkstyle 0, import-gate clean, **fail-before exactly the 3 splitting tests red** (neuter `computeFileSplitOffsets`→single range), end-to-end append-only fixture (small `file_split_size` → ≥2 contiguous sub-ranges tiling `[0,fileLength)`; default → 1 range). split-weight scheduling nicety not ported (pre-existing) → [DV-033]. Real BE multi-range + DV read = CI-gated live-e2e (legacy paimon regression covers the BE contract; no BE change).
- **Adversarial review `wf_4ac7479d-39d`: 2 confirmed (both fixed), 2 refuted.** (1) MINOR parity gap — under COUNT(*) pushdown a native-eligible split with no precomputed merged count (e.g. DV w/ null cardinality) was sub-split where legacy keeps it whole (`splittable=!applyCountPushdown`); my design/comments falsely claimed "no interaction". Fixed: native arm passes target=0 under `countPushdown` → single whole-file range (byte-exact legacy parity; correctness-neutral either way since BE sets per-scanner agg=NONE w/ DV). (2) MAJOR test gap (Rule 9) — no test pinned "same DV on every sub-range". Fixed: extracted `buildNativeRanges` + test asserts every sub-range carries the DV (mutation: DV only on first → red, verified). Refuted: split-weight (already DV-033), DV-correctness false alarms.

---

## P3 — Coverage gaps **VERIFIED** (2026-06-12; adversarial audit `wf_25450c36-b7a`: tracer → adversarial verifier → completeness critic)

> "Go check, not fix." Result: **3/4 PARITY_HOLDS; 1 real divergence → converted to FIX** (user-signed, [D-056](./decisions-log.md)).

- ✅ **VERIFY FIX-HMS-CONFRES — PARITY_HOLDS**: key spelling is exactly `hive.conf.resources` (NO `hive.config.resources` alias in fe-core/fe-common — the suspected MAPPING-FLAG-KEYS-class bug **refuted**; `HMSBaseProperties.java:58`, exact `props.get`); round-1 wiring present (`ConnectorContext.loadHiveConfResources` default-empty, `DefaultConnectorContext:140-153` reuses `CatalogConfigFileUtils.loadHiveConfFromHiveConfDir`, `buildHmsHiveConf` base-seed, `PaimonConnector` HMS branch); HMS-only (DLF parity: legacy `PaimonAliyunDLFMetaStoreProperties:73-84` builds a fresh HiveConf, no file load). **BE-downflow** (the part round-2 never tested): legacy HMS hive-site.xml keys do **NOT** reach BE scan props (legacy `getLocationProperties` = StorageProperties-derived only; `getBackendPaimonOptions` JDBC-only); plugin mirrors exactly (`PaimonScanPlanProvider:546-549/897`) + serializes the table from the same hive-conf-resources-built catalog. No divergence.
- 🔴 **TRACE DDL write parity — 1 REAL_DIVERGENCE (MAJOR) → FIXED** (see **P3-fix** below). Other 6 aspects parity/NIT: dropTable/createDatabase/dropDatabase IF-(NOT-)EXISTS + FORCE/cascade (enumerate-loop AND native cascade) match; branch/tag DDL rejected on BOTH sides (no production `PluginDrivenExternalCatalog` subclass overrides them → base throws `DdlException` "not supported"; legacy `PaimonMetadataOps:314-333` throws `UnsupportedOperationException` — cosmetic type/msg diff); editlog↔cache order reversed (NIT, one synchronized DDL, replay-equivalent); error-code collapse to generic `DdlException` (cosmetic, all ops) = [DV-034](./deviations-log.md).
- ✅ **TRACE ANALYZE / column-stats — PARITY_HOLDS**: `getColumnStatistic` returns `Optional.empty()` on BOTH sides (neither paimon side overrides it; only `ExternalView`/`ExternalTable`/`HMSExternalTable` do); `createAnalysisTask` byte-identical (`PaimonExternalTable:204-207` vs `PluginDrivenExternalTable:429-433`); `ExternalAnalysisTask` engine-agnostic. Empty fallback is **generic to the bridge, shared with legacy paimon → not a regression**; native lake column-stats would be a cross-connector enhancement, not parity.
- ✅ **CHECK split-count accounting — PARITY_HOLDS**: post-sub-split `selectedSplitNum` set in shared parent `FileQueryScanNode:419` (after `super.createScanRangeLocations`), read by `StmtExecutor:686-688` gated `instanceof FileScanNode` — identical both sides; legacy reaches the same via `PaimonScanNode:464 splits.addAll(...)`. No under/double-count. batch-mode unreachable for paimon both sides. 2 divergences both **pre-date #9** + non-correctness: EXPLAIN `inputSplitNum`/`scanRanges` line absent (`PluginDrivenScanNode:229` skips super, MINOR/cosmetic; SqlBlockRuleMgr reads the field not EXPLAIN); compress-suffix guard absent (NIT, native arm gated to `.orc`/`.parquet` → always PLAIN, can't fire).
- ⬜ **跨连接器 follow-up** ([DV-028]/[DV-030]/[DV-031]/[DV-032]/[DV-033]/**[DV-034]**) — hudi/iceberg full-adopter same seams; future batch close (NOT this round). The D-056 bridge fix already closes the createTable-local-conflict seam for ALL plugin connectors.

### P3-fix. FIX-CREATE-TABLE-LOCAL-CONFLICT — createTable drops legacy local-conflict rejection (MAJOR correctness)
- **✅ DONE** (2026-06-12; design [`P5-fix-CREATE-TABLE-LOCAL-CONFLICT-design.md`](./tasks/designs/P5-fix-CREATE-TABLE-LOCAL-CONFLICT-design.md), [D-056](./decisions-log.md), [DV-034](./deviations-log.md)). User signed off: **convert to FIX now**.
- **Root cause**: generic fe-core bridge `PluginDrivenExternalCatalog.createTable:293-309` collapses legacy `PaimonMetadataOps.performCreateTable:182-214`'s ordered remote-then-local probe into one `exists` OR consumed ONLY by the IF-NOT-EXISTS branch; the `!IF NOT EXISTS` path ignores it → a table present only in the local FE cache (case-fold under `lower_case_meta_names`, absent on a case-sensitive remote) is CREATED remotely instead of rejected with `ERR_TABLE_EXISTS_ERROR`. Silent metadata corruption; narrow/backend-dependent trigger.
- **Fix (1 fe-core file)**: split `exists` into `remoteExists`/`localExists`; `!IF NOT EXISTS` + `localExists` → `ErrorReport.reportDdlException(ERR_TABLE_EXISTS_ERROR, name)` (legacy local-arm). Remote-only conflict unchanged (case A). Option-2 surgical; zero SPI/connector/BE/RFC.
- **Gates**: fe-core `PluginDrivenExternalCatalogDdlRoutingTest` **fail-before exactly the 1 new test red** → **pass-after 26/0/0**, checkstyle 0. Real e2e = CI-gated (`lower_case_meta_names=1` + case-variant CREATE on case-sensitive paimon catalog; legacy paimon DDL regression covers the BE/contract).

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
