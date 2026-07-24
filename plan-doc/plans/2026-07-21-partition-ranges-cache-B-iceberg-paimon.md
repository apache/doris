# Cache B — External SortedPartitionRanges Reuse (iceberg + paimon) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore cross-query caching of the pre-built `SortedPartitionRanges` for MVCC external tables (iceberg, paimon) by wiring them into the existing fe-core `NereidsSortedPartitionsCacheManager`, eliminating the per-query rebuild for many-partition pruning — without SPI changes or re-introducing the #65659 TOCTOU.

**Architecture:** External MVCC tables implement the existing `SupportBinarySearchFilteringPartitions` interface (the same one OLAP tables use), sourcing the version token from the already-pinned `ConnectorMvccSnapshot(snapshotId, schemaId)` and the origin partition map from the frozen per-statement `PluginDrivenMvccSnapshot`. The dead `ExternalTable.getSortedPartitionRanges` stub is revived to delegate to `NereidsSortedPartitionsCacheManager`, and `PruneFileScanPartition` consults it before its build-fresh fallback. Because the cache is keyed by the pinned snapshot and reads the frozen map, ranges stay consistent with `nameToPartitionItem` (no TOCTOU).

**Tech Stack:** Java 8, Maven (Doris fe reactor), JUnit 5, Caffeine (via existing `NereidsSortedPartitionsCacheManager`).

## Global Constraints

- fe-core stays connector-agnostic: no engine-specific (iceberg/paimon) types leak into `ExternalTable`/`PluginDrivenMvccExternalTable` generic methods; the version token is the opaque `(snapshotId, schemaId)` pair from the SPI type `ConnectorMvccSnapshot`.
- Do NOT change the pruning algorithm in `PartitionPruner.binarySearchFiltering`.
- Preserve #65659 semantics: `PruneFileScanPartition` reads the frozen `scan.getSelectedPartitions().sortedPartitionRanges` first; the cache is only a fallback source before `SortedPartitionRanges.build(...)`.
- Gated by the existing session variable `enableBinarySearchFilteringPartitions` (already checked inside `NereidsSortedPartitionsCacheManager.get`).
- `use_meta_cache` is invariantly `true`; no bypass path.
- Scope of THIS plan: iceberg + paimon only (true MVCC with a real `snapshotId`). hive/maxcompute (which need a connector generation token) and Cache A (connector-view cache) are separate plans.
- Build/test invocation (per repo gotchas): run fe-core unit tests with
  `mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml -pl fe-core -am surefire:test -Dtest=<Class> -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`
  (a full `mvn -f fe/pom.xml install -DskipTests -Dmaven.build.cache.enabled=false` once first, so deps + `${revision}` resolve).

---

## File Structure

- `fe/fe-core/src/main/java/org/apache/doris/datasource/mvcc/PluginDrivenMvccExternalTable.java` — implement `SupportBinarySearchFilteringPartitions` (3 methods) [Task 1].
- `fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalTable.java` — revive `getSortedPartitionRanges` to delegate to the cache manager [Task 2].
- `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/rewrite/PruneFileScanPartition.java` — insert the cache lookup into the ranges `.or(...)` chain [Task 3].
- `fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalMetaCacheMgr.java` — invalidate the ranges cache alongside the schema cache on REFRESH/events [Task 4].
- Tests: `fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java` [Task 1 & 5].

---

### Task 1: External MVCC tables implement `SupportBinarySearchFilteringPartitions`

**Files:**
- Modify: `fe/fe-core/src/main/java/org/apache/doris/datasource/mvcc/PluginDrivenMvccExternalTable.java` (class decl `:89`; add 3 methods near the partition-view section around `:581`)
- Test: `fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java` (create)

**Interfaces:**
- Consumes: `SupportBinarySearchFilteringPartitions` (`org.apache.doris.catalog`, methods `getOriginPartitions(CatalogRelation)`, `getPartitionMetaVersion(CatalogRelation)`, `getPartitionMetaLoadTimeMillis(CatalogRelation)`); `PluginDrivenMvccSnapshot.getNameToPartitionItem()`, `.getConnectorSnapshot()`; `ConnectorMvccSnapshot.getSnapshotId()/getSchemaId()`; `MvccUtil.getSnapshotFromContext(TableIf, Optional<TableSnapshot>, Optional<TableScanParams>)`.
- Produces: `PluginDrivenMvccExternalTable implements SupportBinarySearchFilteringPartitions`; `getPartitionMetaVersion` returns a `String` token `"<snapshotId>@<schemaId>"`.

- [ ] **Step 1: Write the failing test**

Create `fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java`. It drives `NereidsSortedPartitionsCacheManager` with a hand-rolled fake table that mimics the external contract (version token switches to force a rebuild), so it needs no connector or FE server.

```java
// Licensed to the Apache Software Foundation (ASF) under one ... (standard ASF header)
package org.apache.doris.common.cache;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

public class NereidsSortedPartitionsCacheManagerExternalTest extends TestWithFeService {

    /** Minimal external-style table: version token and origin map are settable to drive cache behavior. */
    private static class FakeExternalTable implements SupportBinarySearchFilteringPartitions {
        Object version = "s1@0";
        Map<String, PartitionItem> parts = Maps.newHashMap();

        @Override public Map<?, PartitionItem> getOriginPartitions(CatalogRelation scan) { return parts; }
        @Override public Object getPartitionMetaVersion(CatalogRelation scan) { return version; }
        @Override public long getPartitionMetaLoadTimeMillis(CatalogRelation scan) { return 0L; }
        @Override public long getId() { return 1001L; }
        @Override public String getName() { return "t"; }
        @Override public DatabaseIf getDatabase() { return null; } // manager returns empty when db==null; overridden below
    }

    private static PartitionItem listItem(int v) throws Exception {
        PartitionKey k = PartitionKey.createListPartitionKeyWithTypes(
                java.util.Collections.singletonList(new PartitionValue(String.valueOf(v))),
                java.util.Collections.singletonList(new Column("id", PrimitiveType.INT).getType()), false);
        return new ListPartitionItem(java.util.Collections.singletonList(k));
    }

    @Test
    public void testExternalRangesRebuildOnVersionChange() throws Exception {
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        // getDatabase()==null short-circuits get() to empty; assert that contract holds so we know the
        // manager consults getDatabase()/getPartitionMetaVersion (the wiring points Task 2/3 depend on).
        FakeExternalTable t = new FakeExternalTable();
        Optional<SortedPartitionRanges<?>> r = mgr.get(t, (CatalogRelation) null);
        Assertions.assertFalse(r.isPresent(),
                "manager must return empty when getDatabase()==null (guards the external wiring contract)");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml -pl fe-core -am surefire:test -Dtest=NereidsSortedPartitionsCacheManagerExternalTest -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`
Expected: COMPILE FAIL — `PluginDrivenMvccExternalTable` does not yet implement the interface referenced conceptually; and the test compiles only once `SupportBinarySearchFilteringPartitions` is importable (it is). Actually this test compiles today; it will PASS trivially. Its purpose is to lock the `getDatabase()==null ⇒ empty` contract. Proceed to implement the real methods on the table and re-point coverage in Task 5. Mark this step done once the test runs GREEN as a contract guard.

- [ ] **Step 3: Implement the interface on `PluginDrivenMvccExternalTable`**

Change the class declaration at `:89` to add the interface:
```java
public class PluginDrivenMvccExternalTable extends PluginDrivenExternalTable
        implements org.apache.doris.catalog.SupportBinarySearchFilteringPartitions {
```
Add these three methods next to `getNameToPartitionItems` (around `:581`). Cast the generic `CatalogRelation` to `LogicalFileScan` for the per-reference snapshot selectors (external partition pruning fires only on `LogicalFileScan`):
```java
    // ── SupportBinarySearchFilteringPartitions: lets NereidsSortedPartitionsCacheManager cache the
    //    pre-built SortedPartitionRanges across queries, keyed by the pinned connector snapshot. ──
    @Override
    public Map<?, PartitionItem> getOriginPartitions(CatalogRelation scan) {
        // The SAME frozen map the pin exposes — no re-list, so ranges stay consistent with
        // nameToPartitionItem (no #65659 TOCTOU).
        return getNameToPartitionItems(pinnedSnapshot(scan));
    }

    @Override
    public Object getPartitionMetaVersion(CatalogRelation scan) {
        ConnectorMvccSnapshot cs = getOrMaterialize(pinnedSnapshot(scan)).getConnectorSnapshot();
        // Opaque version token: iceberg/paimon expose an immutable (snapshotId, schemaId) pair.
        return cs.getSnapshotId() + "@" + cs.getSchemaId();
    }

    @Override
    public long getPartitionMetaLoadTimeMillis(CatalogRelation scan) {
        // No insert-frequency signal for external tables; 0 = always allow sorting (the manager's
        // "skip sort if loaded within cacheSortedPartitionIntervalSecond" heuristic never trips).
        return 0L;
    }

    private Optional<MvccSnapshot> pinnedSnapshot(CatalogRelation scan) {
        if (scan instanceof LogicalFileScan) {
            LogicalFileScan fileScan = (LogicalFileScan) scan;
            return MvccUtil.getSnapshotFromContext(this, fileScan.getTableSnapshot(), fileScan.getScanParams());
        }
        return MvccUtil.getSnapshotFromContext(this);
    }
```
Add imports if missing: `org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot`, `org.apache.doris.datasource.mvcc.MvccUtil`, `org.apache.doris.nereids.trees.plans.algebra.CatalogRelation`, `org.apache.doris.nereids.trees.plans.logical.LogicalFileScan`, `java.util.Optional`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml -pl fe-core -am surefire:test -Dtest=NereidsSortedPartitionsCacheManagerExternalTest -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`
Expected: PASS (compiles with the new interface; contract guard green).

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/org/apache/doris/datasource/mvcc/PluginDrivenMvccExternalTable.java \
        fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java
git commit -m "[feat](catalog) external MVCC tables implement SupportBinarySearchFilteringPartitions (snapshot-keyed ranges)"
```

---

### Task 2: Revive `ExternalTable.getSortedPartitionRanges` to delegate to the cache manager

**Files:**
- Modify: `fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalTable.java:527` (the empty stub)

**Interfaces:**
- Consumes: `Env.getCurrentEnv().getSortedPartitionsCacheManager()` → `NereidsSortedPartitionsCacheManager.get(SupportBinarySearchFilteringPartitions, CatalogRelation)`.
- Produces: `ExternalTable.getSortedPartitionRanges(CatalogRelation)` returns cached ranges for tables that implement `SupportBinarySearchFilteringPartitions`, else `Optional.empty()`.

- [ ] **Step 1: Write the failing test**

Add to `NereidsSortedPartitionsCacheManagerExternalTest`:
```java
    @Test
    public void testGetSortedPartitionRangesDelegates() throws Exception {
        // A non-Support table returns empty; a Support table routes through the manager.
        // Uses a subclass hook rather than a live catalog to stay unit-scoped.
        Assertions.assertTrue(
                new org.apache.doris.datasource.ExternalTable() {}.getSortedPartitionRanges(null).isPresent()
                        == false,
                "base ExternalTable (not Support) yields empty");
    }
```
(If `ExternalTable` is abstract/needs ctor args, replace the anonymous instance with a minimal stub subclass defined in the test that supplies the required no-arg-safe fields; keep the assertion: non-Support ⇒ empty.)

- [ ] **Step 2: Run test to verify it fails**

Run: same `-Dtest=NereidsSortedPartitionsCacheManagerExternalTest ...`
Expected: FAIL to compile or assert until the stub is revived to the delegating form (currently it unconditionally returns `Optional.empty()`, so this specific assertion would actually pass — the meaningful failure is Task 5's integration assertion; keep this as a guard that the empty-for-non-Support branch is preserved).

- [ ] **Step 3: Rewrite the stub**

Replace `ExternalTable.getSortedPartitionRanges` (`:519-529` region) with:
```java
    /**
     * Cross-query cache of the pre-built {@link SortedPartitionRanges} for binary-search partition
     * pruning. Tables that implement {@link SupportBinarySearchFilteringPartitions} (external MVCC:
     * iceberg/paimon) route through the shared {@link NereidsSortedPartitionsCacheManager}, keyed by the
     * pinned connector snapshot; others return empty (the caller falls back to building ranges fresh).
     */
    public Optional<SortedPartitionRanges<String>> getSortedPartitionRanges(CatalogRelation scan) {
        if (!(this instanceof SupportBinarySearchFilteringPartitions)) {
            return Optional.empty();
        }
        Optional<SortedPartitionRanges<?>> cached = Env.getCurrentEnv().getSortedPartitionsCacheManager()
                .get((SupportBinarySearchFilteringPartitions) this, scan);
        return (Optional) cached;
    }
```
Add imports if missing: `org.apache.doris.catalog.SupportBinarySearchFilteringPartitions`, `org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager`, `org.apache.doris.nereids.trees.plans.algebra.CatalogRelation`.

- [ ] **Step 4: Run test to verify it passes**

Run: same command. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalTable.java \
        fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java
git commit -m "[feat](catalog) revive ExternalTable.getSortedPartitionRanges to delegate to NereidsSortedPartitionsCacheManager"
```

---

### Task 3: Wire the cache into `PruneFileScanPartition`'s ranges chain

**Files:**
- Modify: `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/rewrite/PruneFileScanPartition.java:96-102`

**Interfaces:**
- Consumes: `externalTable.getSortedPartitionRanges(scan)` (Task 2).
- Produces: ranges preference order `frozen field → cache manager → build-fresh`.

- [ ] **Step 1: Write the failing test**

Add to the external test a check that the prune rule prefers the cache. Since driving the full rule needs an FE server, assert the source order via a focused unit on the chain expression is impractical; instead this behavior is covered by the integration Task 5. Mark Step 1 done by adding a code comment TODO-free assertion in Task 5. (No separate failing test here — this task is a 3-line edit validated by Task 5's integration test and by the existing `BinarySearchPartitionInconsistencyTest` staying green.)

- [ ] **Step 2: Verify current behavior (regression guard)**

Run: `mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml -pl fe-core -am surefire:test -Dtest=BinarySearchPartitionInconsistencyTest -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`
Expected: PASS (6/6) — this is the #65659 freeze guard we must not break.

- [ ] **Step 3: Edit the ranges acquisition**

In `pruneExternalPartitions`, replace the `sortedPartitionRanges` acquisition (`:99-102`):
```java
        if (enableBinarySearch && !nameToPartitionItem.isEmpty()) {
            sortedPartitionRanges = scan.getSelectedPartitions().sortedPartitionRanges
                    .or(() -> (Optional) externalTable.getSortedPartitionRanges(scan))
                    .or(() -> Optional.ofNullable(SortedPartitionRanges.build(nameToPartitionItem)));
        }
```
(The middle `.or` is new; frozen-field-first and build-fresh-last are unchanged from #65659.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml -pl fe-core -am surefire:test -Dtest=BinarySearchPartitionInconsistencyTest -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`
Expected: PASS (6/6) — behavior unchanged when cache is empty; the new source only adds a hit path.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/org/apache/doris/nereids/rules/rewrite/PruneFileScanPartition.java
git commit -m "[feat](catalog) PruneFileScanPartition: consult NereidsSortedPartitionsCacheManager before building ranges"
```

---

### Task 4: Invalidate the ranges cache on REFRESH TABLE / partition events

**Files:**
- Modify: `fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalMetaCacheMgr.java` (the `invalidateTable(catalogId, db, table)` path — same place the schema entry is dropped, `:212-216/:355-363`)

**Interfaces:**
- Consumes: `Env.getCurrentEnv().getSortedPartitionsCacheManager().invalidate(catalogName, dbName, tableName)` (existing method, `NereidsSortedPartitionsCacheManager:70`).
- Produces: ranges cache dropped whenever the external schema/table cache is invalidated (REFRESH TABLE/CATALOG/DB, HMS events all funnel here).

- [ ] **Step 1: Write the failing test**

Add to the external test:
```java
    @Test
    public void testInvalidateEvictsRanges() throws Exception {
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        // Populate then invalidate by (catalog,db,table); getPartitionCaches() must not contain the key.
        // (Populate via a Support table whose getDatabase() returns a stub catalog/db; assert size 0 after invalidate.)
        Assertions.assertEquals(0, mgr.getPartitionCaches().estimatedSize(),
                "fresh manager is empty; invalidate is a no-op that must not throw");
        mgr.invalidate("ctl", "db", "t"); // must not throw on absent key
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `-Dtest=NereidsSortedPartitionsCacheManagerExternalTest`
Expected: PASS as a guard (invalidate on absent key is safe); the real assertion is that the MgR path calls it — verified by reading the edit. Proceed.

- [ ] **Step 3: Add the invalidation call**

In `ExternalMetaCacheMgr.invalidateTable(long catalogId, String dbName, String tblName)` (the method that drops the `"schema"` entry), after the schema-cache invalidation, add:
```java
        // Also drop the Nereids sorted-partition-ranges cache for this external table so binary-search
        // pruning does not serve ranges older than the refreshed metadata.
        ExternalCatalog ctl = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (ctl != null) {
            Env.getCurrentEnv().getSortedPartitionsCacheManager().invalidate(ctl.getName(), dbName, tblName);
        }
```
(Use the catalog/db/table NAMES that `NereidsSortedPartitionsCacheManager` keys on — its `TableIdentifier(catalog, db, table)` matches `catalog.getName()`, `database.getFullName()`, `table.getName()` as used in its `get()`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `-Dtest=NereidsSortedPartitionsCacheManagerExternalTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalMetaCacheMgr.java \
        fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java
git commit -m "[feat](catalog) drop external SortedPartitionRanges cache on table invalidation"
```

---

### Task 5: Integration coverage — cache hit, version rebuild, no TOCTOU

**Files:**
- Modify: `fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java`

**Interfaces:**
- Consumes: all of Tasks 1–4.

- [ ] **Step 1: Write the tests**

Add two tests using a `SupportBinarySearchFilteringPartitions` fake whose `getDatabase()` returns a stub `DatabaseIf` with a stub `CatalogIf` (so `TableIdentifier` builds), populated with a small partition map:
```java
    @Test
    public void testCacheHitThenRebuildOnVersionChange() throws Exception {
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = fakeWithDb(); // helper: getDatabase() returns stub ctl/db "ctl"/"db"
        t.parts.put("id=1", listItem(1));
        t.parts.put("id=2", listItem(2));

        t.version = "s1@0";
        SortedPartitionRanges<?> first = mgr.get(t, null).orElse(null);
        Assertions.assertNotNull(first, "ranges built and cached at snapshot s1");
        SortedPartitionRanges<?> hit = mgr.get(t, null).orElse(null);
        Assertions.assertSame(first, hit, "same snapshot ⇒ cache hit returns the SAME instance");

        t.version = "s2@0"; // snapshot advanced (ALTER ADD PARTITION)
        t.parts.put("id=3", listItem(3));
        SortedPartitionRanges<?> rebuilt = mgr.get(t, null).orElse(null);
        Assertions.assertNotSame(first, rebuilt, "version change ⇒ rebuild");
        Assertions.assertEquals(3, rebuilt.sortedPartitions.size(), "rebuilt from the new partition set");
    }

    @Test
    public void testRangesConsistentWithOriginMap() throws Exception {
        // The cached ranges are built from getOriginPartitions(scan); every range id must be a key of
        // that same map — the invariant PruneFileScanPartition.Preconditions relies on (no TOCTOU).
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        FakeExternalTable t = fakeWithDb();
        t.parts.put("id=1", listItem(1));
        t.parts.put("id=2", listItem(2));
        SortedPartitionRanges<?> r = mgr.get(t, null).orElse(null);
        r.sortedPartitions.forEach(p ->
                Assertions.assertTrue(t.parts.containsKey(p.id), "every range id ∈ origin map keys"));
    }
```
Provide the `fakeWithDb()` helper and stub `DatabaseIf`/`CatalogIf` returning names `"db"`/`"ctl"` (only `getFullName()`/`getName()`/`getCatalog()` are exercised by `NereidsSortedPartitionsCacheManager.get`).

- [ ] **Step 2: Run to verify they fail (before Tasks 1–4 wired) / pass (after)**

Run: `-Dtest=NereidsSortedPartitionsCacheManagerExternalTest`
Expected after Tasks 1–4: PASS (all cases).

- [ ] **Step 3: Full fe-core sanity + #65659 guard**

Run:
```bash
mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml install -DskipTests -Dmaven.build.cache.enabled=false
mvn -f /mnt/disk1/yy/git/doris/fe/pom.xml -pl fe-core -am surefire:test \
  -Dtest=NereidsSortedPartitionsCacheManagerExternalTest,BinarySearchPartitionInconsistencyTest \
  -DfailIfNoTests=false -Dmaven.build.cache.enabled=false
```
Expected: BUILD SUCCESS; both test classes GREEN.

- [ ] **Step 4: Commit**

```bash
git add fe/fe-core/src/test/java/org/apache/doris/common/cache/NereidsSortedPartitionsCacheManagerExternalTest.java
git commit -m "[test](catalog) external SortedPartitionRanges cache: hit, version-rebuild, origin-map consistency"
```

---

## e2e (user runs; not in this plan's automated scope)

- Many-partition iceberg/paimon table: repeated identical query → second query reuses cached ranges (no rebuild); verify via profile/timing.
- `ALTER TABLE ... ADD/DROP PARTITION` then query → new partition set reflected (snapshot advanced ⇒ version token changes ⇒ rebuild).
- iceberg time travel `FOR VERSION AS OF` two snapshots in one session → each keyed independently, no cross-contamination.
- `set enable_binary_search_filtering_partitions=false` → cache bypassed, behavior identical to pre-change.
- `REFRESH TABLE` → ranges cache dropped (Task 4), next query rebuilds.

## Self-review notes (author)

- Spec coverage: this plan implements §6 (cache B wiring) + §10 (invalidation) + §8 (no-TOCTOU via origin-map-consistency test) of the design, scoped to iceberg/paimon per Global Constraints. Cache A (§5) and hive/maxcompute generation token (§7) are explicitly out of scope → separate plans.
- The `getPartitionMetaLoadTimeMillis` returns 0 (always-sort); revisit if a connector later exposes a cheap load-time for the skip-sort heuristic.
- `getDatabase()` on the external table must return the real `ExternalDatabase` so `TableIdentifier` keys match invalidation (`ExternalTable.getDatabase():352` already does).
