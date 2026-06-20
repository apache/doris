# P5-T29 (B8) — paimon legacy removal from fe-core (design)

> **Design-first, firsthand-verified.** Closure produced 2026-06-20 by two parallel re-grep +
> adversarial-verify workflows (`wf_a8bcfb20-405` Plan-A readiness, `wf_8a50af43-7a2` Plan-B
> feasibility) **plus a firsthand conflict-resolution pass** (the two workflows disagreed on whether
> `datasource/paimon/*` is dead; the import-level firsthand check settled it — see §0.1). This doc is
> the execution source for P5-T29.
> Mirrors **P4 #64300** (`73832991962`, "make fe-core odps-free"): delete files + clean reverse-refs +
> drop maven deps + `dependency:tree` verify.
> Sample design = [`P4-batchD-maxcompute-removal-design.md`](./P4-batchD-maxcompute-removal-design.md).

---

## 0. Scope decisions (user-signed 2026-06-20)

Two decisions were taken via AskUserQuestion after the feasibility dig:

- **D-PB1 — metastore-props mechanism = B1 (strip SDK in place).** The 7 STILL-CONSUMED
  `property/metastore/Paimon*` classes are **kept in fe-core** as thin SDK-free metastore-property
  descriptors; their paimon-SDK use (confined to dead catalog-building methods) is stripped. NOT
  physically relocated (B2 was rejected — it forces a generic `MetastoreProperties`-registry rework +
  cross-loader re-basing for no marginal benefit toward dropping deps, and iceberg/hive keep their
  metastore-props in fe-core *with* their engine SDK, so B1 makes paimon the clean outlier — parity-OK).
- **D-PB2 — sequencing = phased.** *(Refined 2026-06-20 after firsthand discovery — see note below.)*
  - **Batch 1 (this doc, safe core) — ✅ DONE (commit `7632a074e4b`):** delete the 33 DEAD files +
    reverse-ref cleanups + dead tests + decouple the metastore-props from the deleted
    `PaimonExternalCatalog` constants (inline `getPaimonCatalogType` literals). **paimon maven deps STAY.**
  - **Batch 2 (later, docker-e2e-gated, separate):** **B1-strip the 6 metastore-props** (remove their
    paimon-SDK catalog-building methods + imports + trim the 7 catalog-building test files) + migrate
    `PaimonVendedCredentialsProvider` out of fe-core + rework the generic `VendedCredentialsFactory`
    paimon seam (shared with iceberg) + **drop all 5 paimon maven deps**. All SDK-removal lands together.

  > **Refinement (user-signed 2026-06-20):** the B1 strip was originally slotted into Batch 1, but
  > firsthand recon showed it is *not* "deletions only" — it reshapes 6 LIVE classes (the strip-target
  > methods have zero live main callers, but the live `executionAuthenticator`/`initExecutionAuthenticator`
  > wiring at `PluginDrivenExternalCatalog:137-138` must be preserved) and gut-trims **7** metastore-props
  > test files that assert the dead catalog-building (`AbstractPaimonPropertiesTest`, `PaimonCatalogTest`
  > [@Disabled manual → delete], `Paimon{HMS,FileSystem,Jdbc,Rest,AliyunDLF}MetaStorePropertiesTest`).
  > It drops no dep by itself (it is a *prerequisite* for the Batch-2 dep-drop). So it was **moved to
  > Batch 2**, leaving Batch 1 as the clean, complete "remove dead legacy" PR.

End state after Batch 1+2 = fe-core fully paimon-SDK-free (zero `org.apache.paimon.*` imports), all 5
paimon maven deps gone.

### 0.1 Conflict resolution — `datasource/paimon/*` IS dead (firsthand)

The Plan-B synth claimed `datasource/paimon/*` is LIVE (that `PluginDrivenMvccExternalTable` uses
`PaimonUtil`, sys-table classes use `PaimonSysTable`), which would block the dep drop. **Refuted by
firsthand import check:** the live generic `PluginDrivenMvccExternalTable` / `PluginDrivenExternalTable`
/ `PluginDrivenSysExternalTable` / `systable/PluginDrivenSysTable` / `systable/NativeSysTable` import
**none** of `datasource.paimon.*`, `systable.PaimonSysTable`, or `metacache.paimon.*` — those were
javadoc/comment references. The only live generic importer is `ExternalMetaCacheMgr:35`
(`PaimonExternalMetaCache`, the dead `paimon()`/register branch already on the cleanup list). Plan-A's
DEAD classification stands.

### 0.2 The 31 paimon-SDK importers in fe-core, decomposed

`grep -rln "import org.apache.paimon\." fe/fe-core/src/main` = 31 files:
- **~23 DEAD subtree files** → deleted in Batch 1 (§2).
- **6 metastore-props** (`AbstractPaimonProperties` + 5 flavors) → B1-stripped in Batch 1 (§4). SDK is
  100% in dead catalog-building methods; live duties (Kerberos `executionAuthenticator`, type) are SDK-free.
- **`PaimonVendedCredentialsProvider`** → genuinely LIVE (uses paimon REST SDK at runtime via generic
  `VendedCredentialsFactory.getProviderType` `case PAIMON`). **Batch 2.**
- **`ShowPartitionsCommand`** → its lone SDK import (`org.apache.paimon.partition.Partition`) dies with
  the dead `handleShowPaimonTablePartitions()` method removed in Batch 1 (§3).

---

## 1. Batch 1 — DEAD file deletion set (33 files)

> Counts firsthand-verified on `branch-catalog-spi` 2026-06-20. **NOTE: 33, not 34** — the Plan-doc
> ledger's "30" for `datasource/paimon/` double-counts `PaimonVendedCredentialsProvider` (LIVE, keep).

**`datasource/paimon/` — 29 files** (the directory minus the LIVE `PaimonVendedCredentialsProvider.java`):
catalog/table/ops/util (11): `PaimonExternalCatalog`, `PaimonExternalCatalogFactory`,
`PaimonHMSExternalCatalog`, `PaimonFileExternalCatalog`, `PaimonRestExternalCatalog`,
`PaimonDLFExternalCatalog`, `PaimonExternalDatabase`, `PaimonExternalTable`, `PaimonSysExternalTable`,
`PaimonMetadataOps`, `PaimonExternalMetaCache`. Util (2): `PaimonUtil`, `PaimonUtils`. Cache/POJO (9):
`PaimonMvccSnapshot`, `PaimonSnapshot`, `PaimonSnapshotCacheValue`, `PaimonSchemaCacheKey`,
`PaimonSchemaCacheValue`, `PaimonTableCacheValue`, `PaimonPartition`, `PaimonPartitionInfo`,
`DorisToPaimonTypeVisitor`. profile/ (2): `profile/PaimonMetricRegistry`, `profile/PaimonScanMetricsReporter`.
source/ (5): `source/PaimonScanNode`, `source/PaimonSource`, `source/PaimonSplit`,
`source/PaimonPredicateConverter`, `source/PaimonValueConverter`.

**`datasource/metacache/paimon/` — 3 files:** `PaimonTableLoader`, `PaimonPartitionInfoLoader`,
`PaimonLatestSnapshotProjectionLoader`.

**`datasource/systable/` — 1 file:** `PaimonSysTable.java` (only consumer `PaimonExternalTable:395`, dead).

**KEEP (LIVE, do NOT delete):** `datasource/paimon/PaimonVendedCredentialsProvider.java` — reached via
generic `VendedCredentialsFactory.getProviderType()` `case PAIMON` ← `CatalogProperty:182`. Batch 2 target.

---

## 2. Batch 1 — reverse-reference cleanups (live files, sever compile-links to dead classes)

| File | action |
|---|---|
| `datasource/ExternalCatalog.java` | delete `case PAIMON -> new PaimonExternalDatabase` switch arm + import (PluginDriven forces logType=PLUGIN) |
| `datasource/ExternalMetaCacheMgr.java` | delete `paimon()` accessor + the metacache-local `ENGINE_PAIMON` const + `register(new PaimonExternalMetaCache(...))` line + import |
| `datasource/metacache/ExternalMetaCacheRouteResolver.java` | delete `instanceof PaimonExternalCatalog` block + const + import |
| `catalog/Env.java` | delete `getType()==PAIMON_EXTERNAL_TABLE` legacy branch + 2 imports |
| `nereids/rules/analysis/UserAuthentication.java` | delete `instanceof PaimonSysExternalTable` else-if + import (live `PluginDrivenSysExternalTable` branch handles it) |
| `nereids/trees/plans/commands/ShowPartitionsCommand.java` | **surgical:** drop the 3 dead-class clauses (`instanceof PaimonExternalCatalog`) + `handleShowPaimonTablePartitions()` method + 3 imports (incl `org.apache.paimon.partition.Partition`). **KEEP `hasPartitionStatsCapability()` + the 5-col body.** |

**KEEP — NOT reverse-refs to delete (LIVE, verified):**
- `credentials/VendedCredentialsFactory.java` `case PAIMON` — LIVE (Batch 2 target, not Batch 1).
- `persist/gson/GsonUtils.java` `registerCompatibleSubtype` **string** aliases (catalog/db/table) — upgrade-compat, string literals, zero compile-link. MUST KEEP (mirrors P4's kept `"MaxComputeExternalCatalog"`).
- `nereids/.../info/CreateTableInfo.ENGINE_PAIMON` — LIVE post-cutover engine name + distribution validation. KEEP.
- `PluginDrivenExternalTable` `case "paimon"` engine-name reporting, `TableType.PAIMON_EXTERNAL_TABLE` enum, `FileQueryScanNode.CACHEABLE_CATALOGS` `"paimon"` — all LIVE. KEEP.

**Javadoc scrubs (would break strict checkstyle/javadoc after deletion):**
- `datasource/PluginDrivenSysExternalTable.java:34` `{@link ...PaimonSysExternalTable}` → re-point/plain.
- `datasource/systable/PluginDrivenSysTable.java:27` `{@link PaimonSysTable}` → re-point/plain.
- `datasource/systable/NativeSysTable.java:36` `@see PaimonSysTable` → drop/re-point.

---

## 3. Batch 1 — dead tests

**DELETE (SUT is a DEAD class) — 5:** `datasource/paimon/PaimonExternalMetaCacheTest`,
`datasource/paimon/source/PaimonScanNodeTest`, `planner/PaimonPredicateConverterTest` (legacy DUP converter),
`datasource/paimon/PaimonMetadataOpsTest`, `datasource/paimon/PaimonUtilTest`.

**TRIM (dead class used only as fixture/mock) — 2:** `datasource/ExternalMetaCacheRouteResolverTest`
(replace `new PaimonExternalCatalog(...)` fixtures; tests LIVE `ExternalMetaCacheMgr`),
`nereids/StatementContextTest` (`testPreloadPaimonLatestSnapshotBeforeLock`: swap
`Mockito.mock(PaimonExternalTable.class)` → `PluginDrivenMvccExternalTable`).

**KEEP (LIVE) — `datasource/paimon/PaimonVendedCredentialsProviderTest`** (SUT LIVE, Batch 2).

---

## 4. Batch 2 — B1 strip the 6 metastore-props (paimon-SDK-free) — *moved out of Batch 1*

**Strip from `AbstractPaimonProperties` + 5 flavors:** the `org.apache.paimon.*` imports;
abstract+impl `initializeCatalog(...)`; `buildCatalogOptions()`/`appendCatalogOptions()`/abstract
`appendCustomCatalogOptions()`; abstract+impl `getMetastoreType()` (zero callers outside pkg, firsthand);
the `Options catalogOptions` field + Lombok `getCatalogOptions()`; `appendUserHadoopConfig(Configuration)`;
`getCatalogOptionsMap()`; `normalizeS3Config()` (dead). In Jdbc also drop `getBackendPaimonOptions` +
`registerJdbcDriver`/`appendRawJdbcCatalogOptions`/`DriverShim` if unreferenced after.

**Decouple from the deleted `PaimonExternalCatalog` constants:** `getPaimonCatalogType()` is dead-API
in MAIN (only dead-subtree callers) but is SDK-free and asserted by 5 metastore-props tests → **KEEP it,
inline its String-literal returns** (`"hms"`/`"filesystem"`/`"dlf"`/`"rest"`/`"jdbc"`) so it no longer
imports `PaimonExternalCatalog.PAIMON_*`. (Removing the dead-API method entirely is an optional follow-up;
out of Batch-1's minimal boundary.) Update the 2 tests asserting via `PaimonExternalCatalog.PAIMON_*`
(`PaimonJdbcMetaStorePropertiesTest:49`, `PaimonRestMetaStorePropertiesTest:41`) to assert the literal.

**KEEP (all SDK-free, LIVE):** `@ConnectorProperty` fields (`warehouse` …); `Type.PAIMON` enum +
`register(Type.PAIMON, new PaimonPropertiesFactory())` (`MetastoreProperties:90`); `PaimonPropertiesFactory`
(no paimon imports); `initNormalizeAndCheckProps`/`checkRequiredProperties`; `getExecutionAuthenticator`/
`initExecutionAuthenticator`/`initHdfsExecutionAuthenticator` (build `HadoopExecutionAuthenticator`, SDK-free);
`getPaimonCatalogType` (inlined literals). Examine `AbstractPaimonPropertiesTest` + the 5 flavor tests for
calls into stripped methods (e.g. `buildCatalogOptions`/`getCatalogOptionsMap`) and trim accordingly.

---

## 5. Commit plan

The dead subtree and the metastore-props are **mutually dependent** (subtree calls `initializeCatalog`;
props reference `PaimonExternalCatalog.PAIMON_*`). **Additionally** the dead subtree calls a *removed*
reverse-ref symbol: `PaimonUtils:57` → `ExternalMetaCacheMgr.paimon()`. So — exactly as P4 #64300 found
("reverse-ref removal and file deletion must land as one compiling unit") — severing the reverse-refs and
deleting the dead files **cannot** be split.

**Batch 1 = 1 commit — ✅ DONE (`7632a074e4b`):**
- **C1 (sever reverse-refs + delete dead, atomic):** §2 reverse-ref cleanups (6 files) + §2 javadoc
  scrubs (3) + §4-decouple only (inline `getPaimonCatalogType` literals in 5 flavors, drop their
  `PaimonExternalCatalog` import — NOT the SDK strip) + §3 fixture-test trims (2) + 2 constant-test repoints
  + `git rm` the 33 dead files (§1) + 5 dead test files (§3). After C1 the metastore-props keep their SDK
  catalog-building methods (now caller-less) and still compile against the present paimon deps.
  *Verified:* fe-core `test-compile` BUILD SUCCESS + checkstyle 0; 49 affected tests pass;
  `datasource/paimon/` holds only `PaimonVendedCredentialsProvider`.
  *(First attempt split this into prep-then-delete; the `PaimonUtils → paimon()` coupling broke the
  intermediate compile — merged per P4 precedent.)*

**Batch 2 = 1 code commit — ✅ DONE:** §4 strip SDK methods + imports from the 6 metastore-props + trim
the 8 catalog-building test files; **drop all 5 paimon deps**. *Target met:* `grep org.apache.paimon
fe-core/src/{main,test}` = ∅; `dependency:tree -Dincludes=org.apache.paimon` on fe-core = ∅.

> **🔱 Vended-provider deviation (firsthand recon `wf_12d67943-eeb` + user re-signed 2026-06-20 → GAMMA):**
> the design above (§0.2/§4) assumed `PaimonVendedCredentialsProvider` was LIVE end-to-end and had to be
> *migrated out* of fe-core with a cross-loader `VendedCredentialsFactory` seam. **Recon refuted that
> premise** (adversarially confirmed): the provider's paimon-SDK methods (`extractRawVendedCredentials`/
> `getTableName`) are **dead** — reachable only via `getStoragePropertiesMapWithVendedCredentials`, whose
> only callers are iceberg; the real paimon runtime vended path moved to the connector
> (`PaimonScanPlanProvider.extractVendedToken`) at cutover FIX-1. The provider's only LIVE duty was the
> SDK-free `isVendedCredentialsEnabled` gate (`instanceof PaimonRestMetaStoreProperties`) read by
> `CatalogProperty.initStorageProperties`. So the cross-loader migration was unnecessary. The user chose
> **GAMMA**: **delete `PaimonVendedCredentialsProvider` entirely** (+ its test), remove the
> `VendedCredentialsFactory` `case PAIMON` (+ import), and **relocate the gate** to a new SDK-free
> `MetastoreProperties.isVendedCredentialsEnabled()` (base = `false`, `PaimonRestMetaStoreProperties` →
> `true`). `CatalogProperty`'s gate now routes the provider path for iceberg (byte-identical) and the
> metastore-props path for everything else. A 3-agent adversarial review (`wf_ef1fd738-3b9`) verified the
> `checkStorageProperties` truth table is byte-identical to HEAD on all 6 paths and no LIVE Kerberos
> auth-wiring was severed. Also: `getBackendPaimonOptions` (Jdbc) was dropped (SDK-free but 0 live callers —
> connector has its own); `PaimonDlfRestCatalogTest` (paimon-SDK importer not in §3's named list) was deleted.
> *Verified:* fe-core `test-compile` BUILD SUCCESS + checkstyle 0; 32 affected tests green; import-gate OK;
> `s3-transfer-manager` retained (real consumer = hadoop-aws, comment corrected). **fe/pom.xml
> dependencyManagement `paimon.version` kept (R-007: fe-connector-paimon + BE still consume it).**
> live-e2e `enablePaimonTest=true` is **docker-gated → user-run (B9/P5-T30)**, NOT run here.

**Hard pre-commit (HANDOFF):** scrub `regression-test/conf/regression-conf.groovy` (plaintext key);
clean scratch (`.audit-scratch/`/`conf.cmy/`/`META-INF/`/`*.bak`). **Path-whitelist `git add`; NEVER `git add -A`.**
Each commit: `[P5-T29] <subj>` + root cause + fix + tests + `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## 6. Verification gates (mirror P4 #64300)

- [x] **Batch 1+2:** fe-core `test-compile` BUILD SUCCESS + checkstyle 0 (`validate` phase).
- [x] **Batch 1+2:** `tools/check-connector-imports.sh` exit 0.
- [x] **Batch 2:** `grep -rl "import org.apache.paimon\." fe/fe-core/src/{main,test}` = ∅ (was: only `PaimonVendedCredentialsProvider`).
- [x] **Batch 2:** `dependency:tree -Dincludes=org.apache.paimon` on fe-core = ∅; `s3-transfer-manager` retained.
- [x] **Batch 2:** 32 affected tests green (5 trimmed flavor tests + `VendedCredentialsFactoryTest`).
- [ ] paimon connector module UT green (`-pl :fe-connector-paimon -am package -Dassembly.skipAssembly=true`) — connector untouched; spot-check optional.
- [ ] regression-gated live-e2e (B9/P5-T30, **docker-gated, user-run**) after Batch 2 — 5-flavor read + sys-table + MTMV + DDL no regression.
