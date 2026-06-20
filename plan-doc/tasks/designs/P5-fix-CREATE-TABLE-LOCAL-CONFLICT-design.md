# Problem

A P3 coverage-gap audit (plugin-vs-legacy Paimon DDL parity, adversarially verified) found one real
divergence: the generic fe-core bridge `PluginDrivenExternalCatalog.createTable` silently drops legacy's
**local-conflict rejection** on the `CREATE TABLE` path.

When a table exists **only in the local FE cache** (i.e. absent on the remote) and the statement has **no
`IF NOT EXISTS`**, legacy rejects with `ERR_TABLE_EXISTS_ERROR` (1050). The plugin bridge instead falls
through and calls `metadata.createTable`, which — because the table is absent remotely — **creates a
duplicate remote table** rather than failing. This is silent metadata corruption.

This is the generic bridge shared by all plugin connectors (paimon today; MaxCompute + future iceberg/hudi),
so the gap is not paimon-specific. The trigger is narrow but real:
`lower_case_meta_names` set (non-default) + `CREATE TABLE <CaseVariant>` (no `IF NOT EXISTS`) whose folded
name already exists locally but whose exact case does **not** exist on a **case-sensitive** remote (paimon
filesystem/jdbc catalogs are case-sensitive; HMS lowercases, so it is unaffected).

# Root Cause (confirmed in current code)

**Legacy** `PaimonMetadataOps.performCreateTable` (`fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonMetadataOps.java:182-214`)
does an **ordered two-probe**:
1. remote (`tableExist`, `:190`) → on hit: `IF NOT EXISTS` returns `true`, else `ERR_TABLE_EXISTS_ERROR` (`:195`);
2. local (`db.getTableNullable`, `:206`) → on hit: `IF NOT EXISTS` returns `true`, else `ERR_TABLE_EXISTS_ERROR` (`:212`).

The comment at `:199-205` documents the local probe is **specifically** for `lower_case_meta_names` where a
case-variant name folds onto an existing local table while the case-sensitive remote has no such table.
`db.getTableNullable` does the case-fold lookup (`ExternalDatabase.java`,
`finalName = lowerCaseToTableName.get(tableName.toLowerCase())`).

**Plugin bridge** `PluginDrivenExternalCatalog.createTable`
(`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java:293-309`) collapses
both probes into one boolean:

```java
boolean exists = metadata.getTableHandle(session, db.getRemoteName(), tableName).isPresent()
        || db.getTableNullable(tableName) != null;
if (exists && createTableInfo.isIfNotExists()) {        // :296 — exists consumed ONLY here
    return true;
}
// !IF NOT EXISTS: falls straight through to metadata.createTable (:306)
```

`exists` is consumed **only** by the `IF NOT EXISTS` branch (`:296`). The `!IF NOT EXISTS` path (`:303-309`)
ignores `exists` and unconditionally calls `metadata.createTable`. So:

| case | remote | local | IF NOT EXISTS | legacy | plugin (current) |
|------|--------|-------|---------------|--------|------------------|
| A | hit | — | no | ERR_TABLE_EXISTS (1050) | reject via connector throw → generic `DdlException` ("already exists") |
| B | **miss** | **hit** | no | **ERR_TABLE_EXISTS (1050)** | **creates duplicate remote table** ← BUG |
| both | hit/miss | hit/miss | yes | return true | return true |

Only **case B** is a behavioral divergence (silent create vs reject). Case A already rejects with the same
error class and user-visible "already exists" message (only the typed error *code* differs — a pre-existing,
broadly-applicable cosmetic item the audit classified as non-divergent, out of scope here).

# Design

**Surgical (Rule 3): add the missing local-conflict guard; do not touch the remote-hit path.**

Split the single `exists` OR back into its two arms so the `!IF NOT EXISTS` path can distinguish a
local-only conflict (must reject at FE) from a remote conflict (let the connector throw, unchanged):

```java
boolean remoteExists = metadata.getTableHandle(session, db.getRemoteName(),
        createTableInfo.getTableName()).isPresent();
boolean localExists = db.getTableNullable(createTableInfo.getTableName()) != null;
if (remoteExists || localExists) {
    if (createTableInfo.isIfNotExists()) {
        LOG.info("create table[...] which already exists; skipping (IF NOT EXISTS)", ...);
        return true;
    }
    // !IF NOT EXISTS: a table present ONLY in the local FE cache (folded onto an existing name
    // under lower_case_meta_names while the case-sensitive remote has none) must be rejected
    // HERE — connector.createTable would otherwise CREATE it remotely. Mirrors legacy
    // PaimonMetadataOps.performCreateTable:206-214 (local arm). A remote conflict still falls
    // through to connector.createTable, which throws "already exists" → DdlException (unchanged).
    if (localExists) {
        ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR,
                createTableInfo.getTableName());
    }
}
```

`ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, name)` is the exact legacy call; it throws
`DdlException` (subtype of the method's declared `UserException`). For case A (`remoteExists && !localExists`),
the guard does not fire and control falls through to `metadata.createTable` exactly as before.

**Why not full parity (Option 1, rejected):** throwing `ERR_TABLE_EXISTS_ERROR` for the whole
`exists && !isIfNotExists` set would also retype case A's error (generic → 1050), but it (a) changes a case
that is **not** the confirmed divergence (case A already rejects correctly), (b) breaks an existing,
intentional test that codifies the remote-hit→connector behavior, and (c) is broader than the finding. The
residual case-A error-*code* genericness is pre-existing, applies to all four DDL ops uniformly, and was
marked non-divergent by the audit — left out of scope.

# Implementation Plan

**1. `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java`**
- Add imports `org.apache.doris.common.ErrorCode`, `org.apache.doris.common.ErrorReport` (file already imports `DdlException` from the same package).
- Replace the single `exists` OR + `if (exists && isIfNotExists)` block (`:293-302`) with the split-probe + local-conflict-guard shown above. Update the now-stale inline comment at `:301-302`.

Pure fe-core bridge change. **No SPI, no connector, no BE, no RFC.** No connector-import-rule concern (the
change is in fe-core).

**2. `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`**
- Add one test `testCreateTableLocalConflictWithoutIfNotExistsRejects` (mirrors the existing local-arm test
  `testCreateTableIfNotExistsExistingLocalTableReturnsTrue` but with `isIfNotExists=false`): remote
  `getTableHandle` empty + local `getTableNullable` non-null + `!isIfNotExists` → asserts a `DdlException`
  ("already exists") is thrown AND `metadata.createTable` is **never** called AND no edit log written.
- The static converter is mocked (as in `testCreateTableExistingTableWithoutIfNotExistsStillErrors`) so the
  fail-before run cleanly distinguishes "fell through and created" from "rejected".

# Risk Analysis

- **Shared-code blast radius:** the change is in the generic bridge used by every plugin connector. It only
  **adds** a rejection on a path that was previously a silent create; it cannot make a previously-succeeding
  *correct* create fail (a local-only-conflict create was always wrong). Case A (remote-hit) is byte-for-byte
  unchanged — the existing test `testCreateTableExistingTableWithoutIfNotExistsStillErrors` (remote-hit +
  `!isIfNotExists`, no local stub → `localExists=false`) stays green. The two `IF NOT EXISTS` tests
  (`...ExistingRemoteTableReturnsTrue...`, `...ExistingLocalTableReturnsTrue`) are unaffected (the
  `isIfNotExists` branch is structurally identical).
- **`ErrorReport.reportDdlException` always throws**, so the subsequent fall-through to `metadata.createTable`
  is reached only when `localExists` is false (remote-only conflict) — correct.
- **Parity scope:** restores case-B correctness to legacy. Residual: case-A error *code* stays generic
  (pre-existing, out of scope, audit-classified cosmetic).
- **No editlog/replay impact:** the guard throws before any editlog write; rejected creates produce no log
  entry on either side.

# Test Plan

## Unit Tests
- **New (fail-before / pass-after):** `PluginDrivenExternalCatalogDdlRoutingTest
  .testCreateTableLocalConflictWithoutIfNotExistsRejects` — local-hit + remote-miss + `!IF NOT EXISTS` must
  throw `DdlException` and never call `metadata.createTable`. WHY (Rule 9): encodes that a local-only
  name-collision is rejected at FE, not silently created remotely. **Fail-before:** against unmodified source
  the bridge falls through, calls `metadata.createTable`, returns false → `assertThrows` fails (nothing
  thrown) and `verify(never createTable)` fails → RED. **Pass-after:** the guard throws → GREEN.
- **Regression (must stay green):** `testCreateTableExistingTableWithoutIfNotExistsStillErrors` (case A),
  `testCreateTableIfNotExistsExistingRemoteTableReturnsTrueAndSkipsSideEffects`,
  `testCreateTableIfNotExistsExistingLocalTableReturnsTrue`, and the rest of
  `PluginDrivenExternalCatalogDdlRoutingTest`.

Build/verify: `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-core -am
-Dmaven.build.cache.enabled=false -DfailIfNoTests=false -Dtest=PluginDrivenExternalCatalogDdlRoutingTest test`;
read surefire XML + `MVN_EXIT`. fe-core checkstyle: `mvn -pl :fe-core checkstyle:check`.

## E2E Tests
Live-only / CI-gated (real case-sensitive paimon filesystem/jdbc catalog + `lower_case_meta_names`):
`CREATE TABLE tbl1; CREATE TABLE TBL1;` (no `IF NOT EXISTS`) under `lower_case_meta_names=1` must fail with
"Table 'TBL1' already exists" rather than creating a second remote directory. Not runnable in the offline
unit harness (needs a live writable catalog); covered by the legacy paimon DDL regression contract.

---

# ✅ IMPL SUMMARY (2026-06-12)

**Status: DONE — fe-core build+UT green (`PluginDrivenExternalCatalogDdlRoutingTest` 26/0/0); checkstyle 0; committed.**

## Fix (fe-core bridge only; zero SPI / connector / BE / RFC)
- `fe/fe-core/.../datasource/PluginDrivenExternalCatalog.java`: split the single `exists` OR into
  `remoteExists` / `localExists`; under `!IF NOT EXISTS`, when `localExists` is true call
  `ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName)` (legacy local-arm parity,
  `PaimonMetadataOps:206-214`). A remote-only conflict still falls through to `metadata.createTable`
  (**case A unchanged**). +2 imports (`ErrorCode`, `ErrorReport`).

## Tests
- New: `PluginDrivenExternalCatalogDdlRoutingTest.testCreateTableLocalConflictWithoutIfNotExistsRejects`
  — local-hit + remote-miss + `!IF NOT EXISTS` → asserts `DdlException` thrown + `metadata.createTable`
  never called + no edit log.
- **fail-before**: against unmodified source the new test is the only red
  ("Expected DdlException…nothing was thrown") — 26 run, **1 fail**. **pass-after**: **26/0/0**.
  The existing case-A test (`...ExistingTableWithoutIfNotExistsStillErrors`) + both IF-NOT-EXISTS tests
  stay green (no regression in the shared bridge).

## Scope boundary
- Option-2 surgical ([D-056](../../decisions-log.md)): only case-B correctness restored.
- Residual case-A (and all-DDL-op) typed-error-code collapse to generic `DdlException` = pre-existing,
  out of scope = [DV-034](../../deviations-log.md), deferred to P4 cleanup / cross-connector batch.
- Generic bridge → the fix is inherently cross-connector (MaxCompute / future iceberg/hudi benefit),
  partially closing P3 item-5 (cross-connector follow-up) for this specific seam.
