# FIX-C4 / R2-catalog / R3-catalog — combined design

> Source findings: `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §C4 (config), §R2 (catalog), §R3 (catalog).
> Three independent MINOR fixes, combined into one task-loop / one commit (HANDOFF "可合一").
> Single-task loop: design → design red-team → implement → impl-verify → build+UT → commit → summary.

## Scope & decisions

| Fix | Finding | Legacy class | Decision |
|-----|---------|--------------|----------|
| **C4** | HMS socket timeout hardcoded `"10"`, ignores `Config.hive_metastore_client_timeout_second` | missing-port | Thread the FE config value through `ConnectorContext.getEnvironment()` |
| **R2-catalog** | dead `meta.cache.paimon.table.*` keys silently accepted (legacy `CacheSpec` rejected malformed) | missing-port | **Warn-only in the paimon connector** (user-confirmed) — NOT strip, NOT in the generic bridge |
| **R3-catalog** | `listDatabaseNames` swallows remote failure → `emptyList()` (legacy rethrew) | intentional-deviation | **Rethrow `RuntimeException` with catalog name** (user-confirmed) — exact legacy parity |

Two judgment calls were put to the user and confirmed: R2 = warn-only (the report's "strip" + its cited
generic-bridge location both rejected: the key is paimon-specific, so it must live in the connector, not the
connector-agnostic `PluginDrivenExternalCatalog`); R3 = rethrow (matches legacy `PaimonMetadataOps:340` exactly
*and* every other connector — Hive/Hudi/JDBC/MC/Trino all propagate — and fixes a false "parity" comment).

---

## C4 — thread `hive_metastore_client_timeout_second`

**Root cause.** The HMS socket-timeout default moved from legacy `HMSBaseProperties.checkAndInit()` (which read
`Config.hive_metastore_client_timeout_second`) into `HmsMetaStorePropertiesImpl.toHiveConfOverrides()` step 4, which
hardcodes literal `"10"`. The metastore-spi module has no fe-common dependency, so it cannot read FE `Config`. Only
an operator who raises `fe.conf hive_metastore_client_timeout_second` *without* a per-catalog
`hive.metastore.client.socket.timeout` is affected (gets 10 instead of the configured value).

**Why parity holds when unset.** `Config.hive_metastore_client_timeout_second` default = `10`
(`Config.java:2106`), so the threaded value is `"10"` when unconfigured — byte-identical to today.

**Plumbing (4 modules, mirrors the existing env-key pattern).**

1. **fe-core** `DefaultConnectorContext.buildEnvironment()` — add one line, alongside `jdbc_drivers_dir` etc.:
   ```java
   env.put("hive_metastore_client_timeout_second",
           String.valueOf(Config.hive_metastore_client_timeout_second));
   ```
2. **metastore-api** `HmsMetaStoreProperties` — change the HMS-specific method signature:
   `Map<String,String> toHiveConfOverrides()` → `toHiveConfOverrides(String defaultClientSocketTimeoutSeconds)`.
   (HMS-specific interface method, single production caller — contained blast radius.)
3. **metastore-spi** `HmsMetaStorePropertiesImpl.toHiveConfOverrides(String)` — step 4 uses the param instead of
   `"10"`, keeping the existing user-override guard (`raw.get("hive.metastore.client.socket.timeout")` blank-check,
   verifier-confirmed equivalent to legacy guard-key). Defensive fallback to `"10"` if the param is blank/null:
   ```java
   if (StringUtils.isBlank(raw.get("hive.metastore.client.socket.timeout"))) {
       conf.put("hive.metastore.client.socket.timeout",
               StringUtils.isNotBlank(defaultClientSocketTimeoutSeconds)
                       ? defaultClientSocketTimeoutSeconds : "10");
   }
   ```
   Also update the `{@link #toHiveConfOverrides()}` javadoc reference at line 35 → `(String)`.
4. **paimon** `PaimonConnector` HMS branch (`:183`) — pass the env value:
   ```java
   HiveConf hc = PaimonCatalogFactory.assembleHiveConf(hiveConfFiles,
           hms.toHiveConfOverrides(
                   context.getEnvironment().getOrDefault("hive_metastore_client_timeout_second", "10")));
   ```

**Not affected.** DLF path uses `toDlfCatalogConf()` (no socket-timeout default — verified); REST/JDBC/FS have no
HMS socket timeout. Only the HMS branch threads the value.

**Tests.** Update the ~10 `toHiveConfOverrides()` call-sites (8 in `HmsMetaStorePropertiesTest`, 1 anon impl +
1 caller in `MetaStorePropertiesContractTest`) to the new signature — pass `"10"` to preserve existing assertions.
Add a C4 test: a non-default value (`"60"`) flows to `hive.metastore.client.socket.timeout`, and a user-set
`hive.metastore.client.socket.timeout` suppresses it (override wins) — encodes the *intent* (Rule 9).

---

## R2-catalog — warn on dead `meta.cache.paimon.table.*` keys

**Root cause.** Legacy `PaimonExternalCatalog.checkProperties()` ran `CacheSpec.check{Boolean,Long}Property` on
`meta.cache.paimon.table.{enable,ttl-second,capacity}` (threw `DdlException` for malformed values). On the plugin
path those checks are gone, so a malformed value is accepted. The keys are **100% dead** (the plugin path uses the
generic schema cache; `PaimonExternalMetaCache`/`ExternalMetaCacheMgr.paimon` have zero non-legacy callers), so even
a well-formed value is a no-op.

**Decision (user-confirmed): warn-only, in the connector.** The key is paimon-specific, so the connector-agnostic
`PluginDrivenExternalCatalog` (the report's cited location) is the wrong layer — handling it there violates the
"no source-specific code in the generic SPI layer" rule (memory `catalog-spi-plugindriven-no-source-specific-code`).
Re-imposing full `CacheSpec` validation is pointless (the report agrees) — it would reject malformed values for a
knob that does nothing. Stripping mutates persisted properties (SHOW CREATE CATALOG would no longer echo what the
user typed) and needs a non-validate hook. Warn-only delivers the real value — telling the operator the knob is dead
— at the right layer with the least change.

**Change.** `PaimonConnectorProvider.validateProperties(Map)` (already paimon-specific, called once per
CREATE/ALTER CATALOG via `ConnectorFactory.validateProperties`): before the existing `bind().validate()`, scan for
keys with prefix `meta.cache.paimon.table.` and, if any, `LOG.warn` that they no longer take effect on the paimon
plugin path. Add a log4j logger to the class (none today). Detect by prefix (no need to import the three legacy
fe-core constant strings).

```java
private static final String DEAD_TABLE_CACHE_PREFIX = "meta.cache.paimon.table.";
...
List<String> dead = properties.keySet().stream()
        .filter(k -> k.startsWith(DEAD_TABLE_CACHE_PREFIX)).sorted().collect(Collectors.toList());
if (!dead.isEmpty()) {
    LOG.warn("Paimon catalog property/properties {} no longer take effect (the plugin path uses the "
            + "generic metadata cache); they are ignored.", dead);
}
```

**Tests.** `PaimonConnectorProviderTest` (or new) — asserting a warn is logged is brittle; instead assert
`validateProperties` does **not throw** when a `meta.cache.paimon.table.capacity=-5` (legacy-malformed) key is
present (documents the deliberate no-reject), and still throws for a genuinely invalid catalog (unknown
`paimon.catalog.type`). The warn itself is observable-only; no behavioral assertion.

---

## R3-catalog — rethrow `listDatabaseNames` failure with catalog name

**Root cause.** `PaimonConnectorMetadata.listDatabaseNames` catches `Exception`, `LOG.warn`s (no catalog name),
returns `emptyList()` — a transient remote failure presents as "zero databases". Legacy
`PaimonMetadataOps.listDatabaseNames` (`:336-342`) rethrew `RuntimeException("Failed to list databases names,
catalog name: " + name, e)`. The connector's comment ("legacy ... wrapped it too. Full read-vs-DDL parity") is
**false**. Paimon is the sole connector that swallows (verifier-confirmed: all others propagate).

**Change.** `PaimonConnectorMetadata.listDatabaseNames` — rethrow with the catalog name, dropping the swallow:
```java
try {
    return context.executeAuthenticated(() -> catalogOps.listDatabases());
} catch (Exception e) {
    throw new RuntimeException(
            "Failed to list databases names, catalog name: " + context.getCatalogName(), e);
}
```
Keep the `executeAuthenticated` wrap (M-11 Kerberos UGI). Rewrite the false comment to state the real parity
(legacy rethrew). `context.getCatalogName()` exists on `ConnectorContext`. `RuntimeException` is unchecked →
no signature change; the bridge `PluginDrivenExternalCatalog.listDatabaseNames:226` does not catch → it propagates
to DB-init exactly as legacy did. `Collections` import stays (used in ~10 other spots).

**Tests.** `PaimonConnectorMetadataTest` (or existing) — when `catalogOps.listDatabases()` throws, assert
`listDatabaseNames` throws (not empty), and the message carries the catalog name. RED→GREEN: with the old swallow
the test sees `emptyList()` (red), with the rethrow it throws (green).

---

## Risk / blast-radius

- **C4** changes a metastore-api interface method signature, but `HmsMetaStoreProperties` is consumed only by paimon
  (sole cut-over connector) + tests → contained. Default-preserving when `fe.conf` unset.
- **R2** is warn-only → no behavior change beyond a log line at CREATE/ALTER CATALOG.
- **R3** is a real behavior change (swallow→throw) on a transient-failure edge: a flaky metastore now errors SHOW
  DATABASES instead of returning empty. This is the legacy behavior and matches all other connectors — the safer,
  less-surprising contract (empty-on-error masks failures). User-confirmed.
- All three are gated/CI-only for live e2e; UT + build are the verification gate.

## Design red-team resolution (wf_444e33b9-5c6 — 4 lenses, 12 findings, 9 confirmed / 3 refuted → GO-WITH-CHANGES)

The 3 production-code changes were judged sound; all confirmed defects were in the test plan / doc, now folded in:

- **R2 premise re-verified (the one substantive concern).** A verifier challenged "100% dead" citing
  `PaimonUtils.java:56-57` / `PaimonExternalMetaCache`. Traced and **refuted**: `ExternalTable.getMetaCacheEngine()`
  returns `"default"` and PluginDriven tables do **not** override it, so a cut-over paimon table routes
  `ExternalMetaCacheMgr.getSchemaCacheValue` to the generic `"default"` cache — never `PaimonExternalMetaCache`
  (engine `"paimon"`). `meta.cache.paimon.table.*` sizes only `PaimonExternalMetaCache.tableEntry`, reached solely
  via `getPaimonTable`/`getLatestSnapshotCacheValue` ← legacy `PaimonExternalTable`/`PaimonScanNode`. **Dead on the
  plugin path confirmed; warn message accurate.**
- **C4 call-sites = 9 (not 8)** in `HmsMetaStorePropertiesTest` (incl. inline `:219`, `:226`) + anon impl + caller
  in `MetaStorePropertiesContractTest` = 11 total. Clean signature change (no test-only overload). Also fix 3 stale
  `{@code …toHiveConfOverrides()}` mentions (`KerberosAuthSpec:34`, `PaimonCatalogFactory:53,311`) — doc hygiene.
- **R2 test home** = `PaimonConnectorValidatePropertiesTest` (no `PaimonConnectorProviderTest` exists); no-reject
  test uses a **well-formed** catalog so the dead key is the only variable; `rejectsUnknownFlavor()` already covers
  the throw case. Warn re-fires on each ALTER while the key persists — accepted (no strip, no old/new diffing).
- **R3 = MIGRATE the existing test, not add alongside.** `PaimonConnectorMetadataReadAuthTest`
  `listDatabaseNamesRunsSeamInsideAuthenticator:76`: `.isEmpty()` → `assertThrows(RuntimeException.class, …)`,
  KEEP `ops.log.isEmpty()` + `authCount==1` (M-11 seam coverage holds — `failAuth` throws before the seam) and also
  assert the message carries the catalog name (`ctx.getCatalogName()=="test"`). Rewrite the false comment.

## Verification plan

1. fe-core compiles; metastore-spi + metastore-api compile; paimon connector compiles (`-am`, build-cache off).
2. `HmsMetaStorePropertiesTest` (updated + new C4 test) green; `MetaStorePropertiesContractTest` green.
3. paimon connector tests green (incl. new R2 no-reject + R3 rethrow tests).
4. checkstyle + `tools/check-connector-imports.sh` clean.
5. Mutation check: revert each fix → its new test goes red.
