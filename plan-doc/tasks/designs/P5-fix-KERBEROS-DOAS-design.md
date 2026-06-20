# P5 fix design — `FIX-KERBEROS-DOAS` (rereview2 #6 = M-8 + M-11)

> Source findings: `plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md` (M-8, M-11; both 3/3 confirmed).
> Re-verified against **current** code (5-agent recon workflow `wf_2f6cdf48-cd6` + independent reads).
> User scope decisions (this session): **M-11 = full legacy parity** (wrap all reads), **M-8 = fix now in fe-core**.

---

## Problem

On **Kerberos-secured** deployments the cutover (plugin) paimon catalog loses the UGI `doAs` that legacy applied. Two distinct gaps, same `ExecutionAuthenticator` mechanism:

- **M-8** — `filesystem`/`jdbc` flavor catalogs over **Kerberized HDFS** run *every* op (catalog create + reads) with NO real `doAs`. `PaimonConnector` correctly wraps catalog-create in `context.executeAuthenticated` (`PaimonConnector.java:194`), but the authenticator behind it is the **base no-op** for these two flavors, so the wrap is inert.
- **M-11** — On a **Kerberos HMS** catalog, the connector's metadata **read** RPCs (`getTable`, `listTables`, `listDatabases`, `getDatabase`, `listPartitions`) run with NO `executeAuthenticated` wrap. The 4 DDL ops ARE wrapped (deliberate signed **D7=B** read-vs-DDL asymmetry). Legacy wrapped **all** reads per-call.

Both are **Kerberos-only**: on simple-auth the no-op authenticator is behaviorally identical to a real one (`ExecutionAuthenticator.execute` = `task.call()`), so non-secured deployments are unaffected.

**Out of scope (verified):** DLF (the review's "DLF" clause is **overstated** — `PaimonAliyunDLFMetaStoreProperties` never sets an authenticator and authenticates via Aliyun AK/SK/STS into HiveConf, not Kerberos UGI; OSS/OSS_HDFS-backed; there is no `doAs` to lose). HMS for M-8 (already correct). REST (no Kerberos).

## Root Cause

### M-8 (authenticator no-op on cutover path)
- `AbstractPaimonProperties.java:44-46` declares `@Getter protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator(){}` — a **no-op default** that shadows `MetastoreProperties.getExecutionAuthenticator()` (returns `NOOP_AUTH`, `MetastoreProperties.java:139`).
- **HMS** assigns the real `HadoopExecutionAuthenticator` in `initNormalizeAndCheckProps()` (`PaimonHMSMetaStoreProperties.java:70`), which `AbstractMetastorePropertiesFactory.createInternal:71` calls **unconditionally** at `MetastoreProperties.create()` → live authenticator → unaffected.
- **filesystem** (`PaimonFileSystemMetaStoreProperties.java:46`) and **jdbc** (`PaimonJdbcMetaStoreProperties.java:120`) assign it **only inside `initializeCatalog()`**.
- `initializeCatalog()` is **dead on the cutover path**: its only live caller is legacy `PaimonExternalCatalog.java:147`. The plugin catalog builds its own paimon `Catalog` via `PaimonCatalogFactory` (`PaimonConnector.createCatalog`), never calling `initializeCatalog`.
- So `PluginDrivenExternalCatalog.initPreExecutionAuthenticator()` reads `msp.getExecutionAuthenticator()` (`:130`) → the line-45 no-op → supplied to the connector via `DefaultConnectorContext(this::getExecutionAuthenticator)` (`:150`). `executeAuthenticated` then runs `task.call()` with **no `doAs`**.
- Legacy "worked" only because `createCatalog()`→`initializeCatalog(storageList)` ran **before** `initPreExecutionAuthenticator()`, mutating the field to the real authenticator first.

### M-11 (read-path RPCs unwrapped)
- `PaimonConnectorMetadata` wraps the 4 DDL ops in `context.executeAuthenticated` (`:687/712/754/783`) but issues the read RPCs bare. Legacy `PaimonMetadataOps` / `PaimonExternalCatalog` wrapped **every** read in `executionAuthenticator.execute` (`getPaimonPartitions:99` listPartitions, `getPaimonTable:137` getTable, plus listDatabases/listTables/getDatabase).
- This was a **deliberate signed decision** (B3 D7=B: wrap DDL, defer read-path wrap to the live-e2e gate). M-11 re-opens it. **User signed full legacy parity this session → new decision `[D-052]` supersedes D7=B's read-path clause.**

## Design

### M-8 — fe-core, mirror HMS, reuse the safely-created storage list (filesystem + jdbc only)

Wire the HDFS authenticator at **catalog-init time** (matching legacy timing — important because `KerberosHadoopAuthenticator`'s ctor logs in **eagerly** and throws on failure; we must NOT move that to every `MetastoreProperties.create()`/`checkProperties`). Reuse the already-safely-created `catalogProperty.getOrderedStoragePropertiesList()` (the exact list legacy passed to `initializeCatalog`) rather than rebuilding via `StorageProperties.createAll` (which would re-run + re-login on each call and add throw-risk legacy didn't have).

New generic hook on the metastore base, default no-op, called once on the plugin-catalog init path:

1. `MetastoreProperties.java` — add
   ```java
   public void initExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
       // default no-op; subtypes whose ExecutionAuthenticator is derived from the catalog's
       // storage properties (paimon filesystem/jdbc over kerberized HDFS) override this.
   }
   ```
2. `AbstractPaimonProperties.java` — add a shared helper:
   ```java
   protected void initHdfsExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
       if (storagePropertiesList == null) {
           return;
       }
       for (StorageProperties sp : storagePropertiesList) {
           if (sp.getType() == StorageProperties.Type.HDFS) {
               this.executionAuthenticator = new HadoopExecutionAuthenticator(
                       ((HdfsProperties) sp).getHadoopAuthenticator());
               return;
           }
       }
   }
   ```
3. `PaimonFileSystemMetaStoreProperties.java` + `PaimonJdbcMetaStoreProperties.java` — override:
   ```java
   @Override
   public void initExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
       initHdfsExecutionAuthenticator(storagePropertiesList);
   }
   ```
   (Legacy `initializeCatalog` left untouched — still serves the legacy path until B8 deletes it; the 3-line overlap is acceptable and avoids risk to the live legacy path.)
4. `PluginDrivenExternalCatalog.initPreExecutionAuthenticator()` — call the hook **before** reading the authenticator:
   ```java
   MetastoreProperties msp = catalogProperty.getMetastoreProperties();
   if (msp != null) {
       msp.initExecutionAuthenticator(catalogProperty.getOrderedStoragePropertiesList()); // NEW
       executionAuthenticator = msp.getExecutionAuthenticator();
       return;
   }
   ```
   Generic + safe: non-paimon msp and paimon hms/dlf/rest get the base no-op (HMS already real). No connector change (impossible — connector can't import fe-core; it already wraps create in `executeAuthenticated`).

### M-11 — connector, wrap all read RPCs, catch domain exceptions INSIDE the lambda

**Exception-flow constraint (load-bearing):** under Kerberos, `UGI.doAs` wraps a thrown checked `Catalog.{Table,Database}NotExistException` in `UndeclaredThrowableException` (only `IOException`/`RuntimeException`/`Error`/`InterruptedException` pass through — `SimpleHadoopAuthenticator.doAs:57`, `KerberosHadoopAuthenticator.doAs:111`). So catching the domain exception **outside** `executeAuthenticated` breaks under Kerberos. Mirror legacy: catch the domain exception **inside** the lambda (legacy `getPaimonPartitions:104` did exactly this), or — where the existing catch is already a catch-all `Exception` → wrap-as-RuntimeException — keep it outside (the catch-all absorbs the wrapped exception unchanged).

Seven read sites (`context` is available in both classes):

| # | site | RPC | wrap shape |
|---|------|-----|-----------|
| 1 | `PaimonConnectorMetadata.listDatabaseNames:96` | listDatabases | wrap call; existing outer `catch(Exception)→empty` absorbs it (no domain exception thrown) |
| 2 | `databaseExists:106` | getDatabase | catch `DatabaseNotExistException` **inside** → return false; outer `catch(Exception)`→ rethrow `DorisConnectorException` (preserve "propagate" for other failures) |
| 3 | `listTableNames:116` | listTables | catch `DatabaseNotExistException` **inside** → empty(+log); outer `catch(Exception)`→ empty(+log) |
| 4 | `getTableHandle:131` | getTable | catch `TableNotExistException` **inside** → `Optional.empty()`; outer `catch(Exception)`→ empty(+log) |
| 5 | `getSysTableHandle:292` | getTable(sysId) | catch `TableNotExistException` **inside** → null sentinel → `Optional.empty()` |
| 6 | `resolveTable:987` **and** `PaimonScanPlanProvider.resolveTable:187` | getTable reload (via `PaimonTableResolver.resolve`) | wrap the whole `resolve(...)` call; existing `catch(Exception)→RuntimeException` absorbs the wrapped exception. **DRY win** — covers every `resolveTable` caller (getTableSchema, getColumnHandles, collectPartitions, fetchRowCount's table-load, scan planScan, branch resolution, …) |
| 7 | `collectPartitions:894` | listPartitions | catch `TableNotExistException` **inside** → empty(+log); outer `catch(Exception)`→ RuntimeException (exact legacy `getPaimonPartitions` shape) |

**Do NOT wrap** snapshot/schema/`rowCount`/`planScan`/`getSplits` (FileIO reads, not HMS RPCs — legacy did not wrap `fetchRowCount`/split planning either; wrapping them is not a parity regression but is out of scope). The transient-`Table` fast path in `resolve` (no RPC) under `doAs` is harmless (cheap thread-local UGI swap, once per op).

## Implementation Plan

**fe-core (M-8) — 5 files:** `MetastoreProperties.java` (+ hook + import `StorageProperties`), `AbstractPaimonProperties.java` (+ helper + imports `HdfsProperties`/`HadoopExecutionAuthenticator`/`List`), `PaimonFileSystemMetaStoreProperties.java` (+ override), `PaimonJdbcMetaStoreProperties.java` (+ override), `PluginDrivenExternalCatalog.java` (1 line).

**connector (M-11) — 2 files:** `PaimonConnectorMetadata.java` (7 edits: sites 1–5,6,7), `PaimonScanPlanProvider.java` (1 edit: site 6 scan twin).

Connector import gate stays clean (`context.executeAuthenticated` is SPI; no fe-core import). One commit (#6), three sides (connector + fe-core; no SPI surface change — `executeAuthenticated`/`getExecutionAuthenticator` already exist).

## Risk Analysis

- **M-8 eager Kerberos login at catalog-init**: matches legacy timing (`initializeCatalog` also logged in eagerly). Building from the pre-created storage list avoids repeated logins. Non-kerberos/non-HDFS catalogs: helper finds no HDFS storage → stays no-op → no behavior change.
- **M-8 generic base hook**: default no-op → MaxCompute/jdbc/es and paimon hms/dlf/rest unaffected. `getOrderedStoragePropertiesList()` is already exercised on the same init path → no new throw surface.
- **M-11 exception semantics**: the inside-lambda catches preserve each site's exact today behavior on simple-auth AND make it correct on Kerberos. Sites 1/6 keep their existing catch-all outside (absorbs wrapped exceptions). Risk: site 2 `databaseExists` gains an outer `catch(Exception)→DorisConnectorException` it didn't have — but it previously let such failures propagate as unchecked anyway (fail-loud preserved).
- **Perf**: one `executeAuthenticated` frame per metadata op (not per-split). Negligible; legacy paid the same per-call `execute()`.

## Test Plan

### Unit Tests (runnable FE)
- **M-11** (`fe-connector-paimon`): new test(s) using `RecordingConnectorContext` (`authCount`/`failAuth`) + `RecordingPaimonCatalogOps` (logs `listPartitions:`/`getTable:` etc.), copying `PaimonConnectorMetadataDdlTest.createTableRunsSeamInsideAuthenticator`:
  - `failAuth=true` → `listPartitionNames`/`getTableHandle`/`listTableNames`/`databaseExists`/`getTableSchema`(resolveTable) each throw/return-without-reaching-the-seam, and the catalogOps log has **no** corresponding read entry (proves the seam is INSIDE the authenticator); `authCount` incremented.
  - **fail-before**: revert one wrap → the seam runs despite `failAuth` (log shows the read) → test goes red.
- **M-8** (`fe-core`): extend `PaimonFileSystemMetaStorePropertiesTest` (+ new `PaimonJdbcMetaStorePropertiesTest` if absent): assert `getExecutionAuthenticator()` returns `HadoopExecutionAuthenticator` after `initExecutionAuthenticator(StorageProperties.createAll(props))` **without** calling `initializeCatalog()`, for a non-kerberos HDFS (`file://`) config (no live KDC needed). **fail-before**: with the override removed, `getExecutionAuthenticator()` stays the base no-op → assert goes red.

### E2E (CI-gated, NOT run here)
- True end-to-end `doAs` (Kerberos HMS read RPCs + Kerberized-HDFS filesystem/jdbc) is **live-Kerberos-e2e only**; **no paimon-kerberos regression suite exists** (the 4 suites under `regression-test/.../kerberos/` cover hive+iceberg, gated by `enableKerberosTest`). Note as gated; do not claim it ran.

## Decisions / deviations to log
- `[D-052]` — wrap **all** connector metastore reads in `executeAuthenticated` (full legacy parity), superseding the **D7=B** read-path clause (user-signed this session). Update the B3 design note.
- `[D-053]` — M-8 fixed in fe-core for **filesystem+jdbc** only; DLF/HMS/REST excluded (verified). "DLF" clause in the review marked overstated.
- No SPI surface change (RFC unchanged): `ConnectorContext.executeAuthenticated` + `MetastoreProperties.getExecutionAuthenticator` already exist; `MetastoreProperties.initExecutionAuthenticator` is an internal fe-core hook, not connector SPI.
- Cross-connector follow-up: the read-vs-DDL `doAs` gap recurs in hudi/iceberg full-adopters (`cutover-fe-dispatch-gap` sibling) — note alongside `[DV-030]`.
