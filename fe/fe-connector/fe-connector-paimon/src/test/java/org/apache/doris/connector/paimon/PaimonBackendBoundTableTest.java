// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.AllGrantedPrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The table {@link PaimonScanPlanProvider#getScanNodeProperties} serializes to the BE must carry no
 * Paimon {@code CatalogLoader}: deserializing one drags the whole Hive metastore stack onto the BE
 * classpath ({@code HiveConf}, the metastore API, and for a version-managed catalog even the
 * metastore client). These tests pin BOTH halves of that: the loader really is gone, and everything
 * the loader used to do on the BE — snapshot resolution, query authorization, the fallback-branch
 * pair, the relation's own scan params — is done on the FE instead, so dropping it changes no rows.
 *
 * <p>No mocking framework (this module has none by convention): the divergence a version-managed
 * catalog creates is reproduced for real by {@link VersionManagedCatalog}, whose {@code loadSnapshot}
 * answers a pointer the test sets while the snapshot directory on disk holds something newer —
 * exactly the publication window / post-rollback shape the pin exists for.
 */
public class PaimonBackendBoundTableTest {

    private static final DataField C1 = new DataField(0, "c1", DataTypes.INT());
    private static final DataField C2 = new DataField(1, "c2", DataTypes.INT());

    private static PaimonScanPlanProvider provider() {
        return new PaimonScanPlanProvider(PaimonCatalogProperties.of(Collections.emptyMap()), null);
    }

    private static PaimonConnectorMetadata metadata(RecordingPaimonCatalogOps ops) {
        return new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
    }

    // ==================== the loader itself ====================

    @Test
    public void dropCatalogLoaderKeepsEverythingButTheLoader(@TempDir Path warehouse) {
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable table = newTable(warehouse, catalogEnvironment(catalog), C1);

        FileStoreTable catalogLess = PaimonScanPlanProvider.dropCatalogLoader(table);

        // The loader is the only thing the BE must not deserialize; everything a FileStoreTable is
        // defined by has to survive, or the BE reads a different table than the FE planned.
        Assertions.assertNull(catalogLess.catalogEnvironment().catalogLoader(),
                "the catalog loader must not reach the BE");
        Assertions.assertEquals(table.rowType(), catalogLess.rowType());
        Assertions.assertEquals(table.location(), catalogLess.location());
        Assertions.assertEquals(table.schema().id(), catalogLess.schema().id());
        Assertions.assertEquals(table.schema().options(), catalogLess.schema().options());
    }

    @Test
    public void dataTableReachesTheBackendWithoutItsCatalogLoader(@TempDir Path warehouse) {
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable table = newTable(warehouse, catalogEnvironment(catalog), C1);

        Table forBackend = provider().tableForBackend(dataHandle(), table);

        Assertions.assertNull(((FileStoreTable) forBackend).catalogEnvironment().catalogLoader(),
                "a plain data table must also be stripped, not only system tables");
    }

    @Test
    public void theSerializedTablePropertyIsTheStrippedOne(@TempDir Path warehouse) throws Exception {
        // The wiring, end to end: whatever tableForBackend returns is what actually lands in
        // paimon.serialized_table and gets Java-deserialized on the BE. Asserting only on
        // tableForBackend would leave getScanNodeProperties free to serialize the catalog-ful table.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable table = newTable(warehouse, catalogEnvironment(catalog), C1);
        PaimonTableHandle handle = dataHandle();
        handle.setPaimonTable(table);
        Assertions.assertNotNull(table.catalogEnvironment().catalogLoader(),
                "precondition: the FE-side table really does carry a loader");

        Map<String, String> props = provider().getScanNodeProperties(null, handle,
                Collections.emptyList(), Optional.empty());

        FileStoreTable onBackend = deserializeTable(props.get("paimon.serialized_table"));
        Assertions.assertNull(onBackend.catalogEnvironment().catalogLoader(),
                "the BE must never deserialize a CatalogLoader (it drags in the Hive metastore stack)");
        Assertions.assertEquals(table.rowType(), onBackend.rowType());
    }

    // ==================== schema generation ====================

    @Test
    public void systemTableForBackendFollowsTheFeWrapperSchemaGeneration(@TempDir Path warehouse) {
        // $audit_log / $binlog / $ro derive their row type from the base table schema. The FE plans
        // on the wrapper the sys handle carries; rebuilding the table serialized to the BE over a
        // DIFFERENT generation of that base makes BE reject the query in
        // PaimonJniScanner#getProjected ("RequiredField c2 not found in schema") or read a stale
        // type under the same name. Both wrappers must come from one generation.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable planned = newTable(warehouse, catalogEnvironment(catalog), C1, C2);
        Table feWrapper = SystemTableLoader.load("audit_log", planned);

        Table forBackend = provider().tableForBackend(sysHandle("audit_log", planned), feWrapper);

        Assertions.assertEquals(feWrapper.rowType(), forBackend.rowType());
        Assertions.assertTrue(forBackend.rowType().getFieldNames().contains("c2"),
                "the BE must see the generation the FE planned with");
    }

    @Test
    public void runtimeCapDoesNotReapplyInheritedSnapshotToSystemTable(@TempDir Path warehouse)
            throws Exception {
        FileStoreTable firstGeneration = commit(newRealTable(warehouse, "runtime_cap_schema"), 1);
        new SchemaManager(firstGeneration.fileIO(), firstGeneration.location())
                .commitChanges(SchemaChange.addColumn("c2", DataTypes.INT()));
        FileStoreTable latestGeneration = FileStoreTableFactory.create(
                firstGeneration.fileIO(), firstGeneration.location());
        Map<String, String> inheritedOptions = new HashMap<>();
        inheritedOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        inheritedOptions.put(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1");
        FileStoreTable statementSource = latestGeneration.copyWithoutTimeTravel(inheritedOptions);
        Table systemTable = SystemTableLoader.load("audit_log", statementSource);

        Table safe = PaimonReaderOptions.runtimeSafeSystemTable(
                "audit_log", systemTable, statementSource, Collections.emptyMap());

        Assertions.assertTrue(safe.rowType().getFieldNames().contains("c2"),
                "lowering an execution bound must not time-travel the bound system-table schema");
    }

    @Test
    public void systemOptionsDoNotReapplyFenceSnapshotToBoundSchema(@TempDir Path warehouse)
            throws Exception {
        FileStoreTable firstGeneration = commit(newRealTable(warehouse, "system_options_schema"), 1);
        new SchemaManager(firstGeneration.fileIO(), firstGeneration.location())
                .commitChanges(SchemaChange.addColumn("c2", DataTypes.INT()));
        FileStoreTable latestGeneration = FileStoreTableFactory.create(
                firstGeneration.fileIO(), firstGeneration.location());
        Map<String, String> rawOptions = Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1");
        Map<String, String> inheritedOptions = PaimonScanParams.pinOptionsToSnapshot(rawOptions, 1);
        FileStoreTable statementSource = PaimonScanParams.applyOptionsWithoutTimeTravel(
                latestGeneration, inheritedOptions);
        Table systemTable = SystemTableLoader.load("audit_log", statementSource);

        Table safe = PaimonReaderOptions.runtimeSafeSystemTable(
                "audit_log", systemTable, statementSource,
                PaimonScanParams.markAsOptions(inheritedOptions));

        Assertions.assertTrue(safe.rowType().getFieldNames().contains("c2"),
                "relation options must retain the schema generation captured by the statement fence");
    }

    @Test
    public void explicitTagSelectsSystemTableSchema(@TempDir Path warehouse) throws Exception {
        FileStoreTable firstGeneration = commit(newRealTable(warehouse, "system_options_tag_schema"), 1);
        firstGeneration.createTag("old_schema", 1L);
        new SchemaManager(firstGeneration.fileIO(), firstGeneration.location())
                .commitChanges(SchemaChange.addColumn("c2", DataTypes.INT()));
        FileStoreTable latestGeneration = FileStoreTableFactory.create(
                firstGeneration.fileIO(), firstGeneration.location());
        Table latestSystemTable = SystemTableLoader.load("audit_log", latestGeneration);
        Map<String, String> tagOptions = PaimonScanParams.markAsOptions(
                Collections.singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "old_schema"));

        Table safe = PaimonReaderOptions.runtimeSafeSystemTable(
                "audit_log", latestSystemTable, latestGeneration, tagOptions);

        Assertions.assertFalse(safe.rowType().getFieldNames().contains("c2"),
                "an explicit tag must select the system table's historical schema");
    }

    @Test
    public void systemTableWithoutACapturedBaseIsHandedOverUntouched(@TempDir Path warehouse) {
        // A format / object table has no FileStoreTable base, so getSysTableHandle falls back to the
        // catalog and captures nothing. Rebuilding from some other base would be a guess; the
        // pre-#65867 behaviour (hand the wrapper over as it stands) is the safe answer.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable planned = newTable(warehouse, catalogEnvironment(catalog), C1);
        Table feWrapper = SystemTableLoader.load("audit_log", planned);
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable("db", "tbl", "audit_log", true);

        Assertions.assertSame(feWrapper, provider().tableForBackend(handle, feWrapper));
    }

    // ==================== capturing that base in the first place ====================

    @Test
    public void sysTableHandleCarriesTheBaseItsWrapperWasBuiltOver(@TempDir Path warehouse) {
        // The wrapper and the base must come from ONE load, or "the generation the FE planned with"
        // is unknowable downstream. Loading the wrapper through the catalog's 4-arg sys Identifier
        // would give it a generation of its own, so the wrapper is built here, over the base handle's
        // own Table, and that base travels with the sys handle.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable base = newTable(warehouse, catalogEnvironment(catalog), C1, C2);
        PaimonTableHandle baseHandle = dataHandle();
        baseHandle.setPaimonTable(base);
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        PaimonTableHandle sysHandle = (PaimonTableHandle) metadata(ops)
                .getSysTableHandle(null, baseHandle, "audit_log").orElseThrow(AssertionError::new);

        Assertions.assertSame(base, sysHandle.getSysBaseTable(),
                "the sys handle must carry the very base its wrapper was built over");
        Assertions.assertEquals(base.rowType().getFieldNames().size() + 1,
                sysHandle.getPaimonTable().rowType().getFieldNames().size(),
                "$audit_log is the base row type plus rowkind, i.e. built over that same base");
        Assertions.assertNull(ops.lastGetTableId,
                "a FileStoreTable base needs no second catalog round-trip for its system table");
    }

    @Test
    public void sysTableHandleBuildsTheWrapperOverThePeeledFallbackPair(@TempDir Path warehouse)
            throws Exception {
        // With file based privileges enabled the meta cache holds Privileged(FallbackRead(..)) - a
        // shape paimon's own catalog never lets a system table see, because CatalogUtils#loadTable
        // builds one over the raw FileStoreTableFactory result. ReadOptimizedTable#newScan dispatches
        // on a DIRECT instanceof, so with the privilege wrapper still in between it plans the main
        // branch alone through the pair's inherited newSnapshotReader() and silently drops every
        // fallback-only partition.
        FileStoreTable[] branches = fallbackPairWithNewerGenerationOnDisk(warehouse, "fb_sys");
        FileStoreTable pair = new FallbackReadFileStoreTable(branches[0], branches[1]);
        FileStoreTable decorated = PrivilegedFileStoreTable.wrap(pair,
                new AllGrantedPrivilegeChecker(), Identifier.create("db", "tbl"));
        PaimonTableHandle baseHandle = dataHandle();
        baseHandle.setPaimonTable(decorated);

        PaimonTableHandle sysHandle = (PaimonTableHandle) metadata(new RecordingPaimonCatalogOps())
                .getSysTableHandle(null, baseHandle, "ro").orElseThrow(AssertionError::new);

        Assertions.assertSame(pair, sysHandle.getSysBaseTable(),
                "the decorator must be peeled, but not the fallback pair under it");
        Assertions.assertTrue(((ReadOptimizedTable) sysHandle.getPaimonTable()).newScan()
                        instanceof FallbackReadFileStoreTable.FallbackReadScan,
                "the FE must plan tbl$ro over BOTH branches");
        // What that stands for: leaving the decorator on takes newScan down its single-branch path.
        Assertions.assertFalse(((ReadOptimizedTable) SystemTableLoader.load("ro", decorated)).newScan()
                        instanceof FallbackReadFileStoreTable.FallbackReadScan);
    }

    @Test
    public void runtimeCapKeepsFallbackImmediateUnderReadOptimizedWrapper(@TempDir Path warehouse)
            throws Exception {
        FileStoreTable[] branches = fallbackPairWithNewerGenerationOnDisk(warehouse, "fb_runtime_cap");
        FileStoreTable pair = new FallbackReadFileStoreTable(branches[0], branches[1]);
        FileStoreTable decorated = PrivilegedFileStoreTable.wrap(pair,
                new AllGrantedPrivilegeChecker(), Identifier.create("db", "tbl"));
        FileStoreTable configured = decorated.copyWithoutTimeTravel(Collections.singletonMap(
                org.apache.paimon.CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));

        Table safe = PaimonReaderOptions.runtimeSafeSystemTable(
                "ro", SystemTableLoader.load("ro", pair), configured, Collections.emptyMap());

        Assertions.assertTrue(((ReadOptimizedTable) safe).newScan()
                        instanceof FallbackReadFileStoreTable.FallbackReadScan,
                "runtime rebuilding must keep the fallback pair as $ro's immediate child");
    }

    @Test
    public void runtimeCapReappliesIncrementalRangeBeforeRebuildingSystemWrapper(
            @TempDir Path warehouse) {
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable dataTable = newTable(warehouse, catalogEnvironment(catalog), C1)
                .copyWithoutTimeTravel(Collections.singletonMap(
                        org.apache.paimon.CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        Map<String, String> incremental = Collections.singletonMap("incremental-between", "1,2");

        Table safe = PaimonReaderOptions.runtimeSafeSystemTable(
                "ro", SystemTableLoader.load("ro", dataTable), dataTable, incremental);

        Assertions.assertEquals("1,2", safe.options().get("incremental-between"),
                "rebuilding a capped system table must retain the relation's incremental range");
    }

    @Test
    public void pinningASysHandleKeepsItsCapturedBase(@TempDir Path warehouse) {
        // applySnapshot rebuilds the handle through withScanOptions for every @options / @incr read.
        // Losing the captured base there would send the BE a catalog-ful wrapper for exactly the
        // pinned reads that need the rebuilt one most.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable base = newTable(warehouse, catalogEnvironment(catalog), C1);

        PaimonTableHandle pinned = sysHandle("ro", base).withScanOptions(
                Collections.singletonMap("scan.snapshot-id", "7"));

        Assertions.assertSame(base, pinned.getSysBaseTable());
    }

    // ==================== the snapshot the BE plans on ====================

    @Test
    public void pinsTheCatalogVisibleSnapshotForAVersionManagedCatalog(@TempDir Path warehouse)
            throws Exception {
        // A version-managed (REST / DLF REST) catalog owns the committed snapshot pointer. Without
        // the loader the BE resolves "latest" by listing the snapshot directory, so it can plan on a
        // snapshot the catalog has not published yet - or one a rollback left behind - while the FE
        // planned on the previous one.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable onDisk = commit(newRealTable(warehouse, "t"), 2);
        // The catalog still points at snapshot 1 while 2 already sits in the snapshot directory.
        catalog.pointAt(onDisk, 1L);
        FileStoreTable versionManaged = withCatalogEnvironment(onDisk, catalogEnvironment(catalog));

        FileStoreTable pinned = PaimonScanPlanProvider.pinCatalogSnapshot(
                PaimonScanPlanProvider.dropCatalogLoader(versionManaged), versionManaged);

        Assertions.assertEquals("1", pinned.options().get("scan.snapshot-id"),
                "the BE must be pinned to the catalog's pointer, not to the newest snapshot file");
        Assertions.assertEquals(2L, onDisk.snapshotManager().latestSnapshotIdFromFileSystem(),
                "the divergence this pins against must be real, not an artifact of the fixture");
    }

    @Test
    public void pinsEachFallbackBranchToItsOwnCatalogVisibleSnapshot(@TempDir Path warehouse)
            throws Exception {
        FileStoreTable onDisk = commit(newRealTable(warehouse, "fallback_pin"), 2);
        VersionManagedCatalog mainCatalog = new VersionManagedCatalog(warehouse, true);
        VersionManagedCatalog fallbackCatalog = new VersionManagedCatalog(warehouse, true);
        mainCatalog.pointAt(onDisk, 2L);
        fallbackCatalog.pointAt(onDisk, 1L);

        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put("scan.fallback-branch", "fb");
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        FileStoreTable main = withCatalogEnvironment(
                onDisk.copyWithoutTimeTravel(mainOptions), catalogEnvironment(mainCatalog));
        FileStoreTable fallback = withCatalogEnvironment(
                onDisk.copyWithoutTimeTravel(fallbackOptions), catalogEnvironment(fallbackCatalog));
        FileStoreTable catalogPair = new FallbackReadFileStoreTable(main, fallback);

        FileStoreTable pinned = PaimonScanPlanProvider.pinCatalogSnapshot(
                PaimonScanPlanProvider.dropCatalogLoader(catalogPair), catalogPair);

        Assertions.assertTrue(pinned instanceof FallbackReadFileStoreTable);
        FallbackReadFileStoreTable pinnedPair = (FallbackReadFileStoreTable) pinned;
        Assertions.assertEquals("2", pinnedPair.wrapped().options().get("scan.snapshot-id"));
        Assertions.assertEquals("1", pinnedPair.fallback().options().get("scan.snapshot-id"),
                "the fallback branch must use its own catalog pointer, not its newest snapshot file");
        Assertions.assertTrue(mainCatalog.loadSnapshotCalls > 0);
        Assertions.assertTrue(fallbackCatalog.loadSnapshotCalls > 0);
    }

    @Test
    public void leavesTheSnapshotUnpinnedForAFilesystemCatalog(@TempDir Path warehouse) throws Exception {
        // A catalog that does not manage versions keeps filesystem semantics on both sides, so there
        // is nothing to pin and the BE must stay on "latest".
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable plain = withCatalogEnvironment(
                commit(newRealTable(warehouse, "t"), 1), catalogEnvironment(catalog));

        Assertions.assertNull(PaimonScanPlanProvider
                .pinCatalogSnapshot(PaimonScanPlanProvider.dropCatalogLoader(plain), plain)
                .options().get("scan.snapshot-id"));
    }

    @Test
    public void pinsEverySystemTableThatResolvesItsSnapshotOnTheBackend(@TempDir Path warehouse)
            throws Exception {
        // $files / $partitions re-plan the base table inside the BE reader; $manifests /
        // $table_indexes / $statistics resolve their snapshot directly there (TimeTravelUtil
        // #tryTravelOrLatest). All five honor scan.snapshot-id and have a fixed row type, so all five
        // must be bound to what the catalog sees before they leave the FE.
        for (String sysTableType : Arrays.asList("files", "partitions", "manifests", "statistics",
                "table_indexes")) {
            VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
            FileStoreTable onDisk = commit(newRealTable(warehouse, "t_" + sysTableType), 1);
            catalog.pointAt(onDisk, 1L);
            FileStoreTable versionManaged = withCatalogEnvironment(onDisk, catalogEnvironment(catalog));

            provider().tableForBackend(sysHandle(sysTableType, versionManaged),
                    SystemTableLoader.load(sysTableType, versionManaged));

            Assertions.assertTrue(catalog.loadSnapshotCalls > 0,
                    "$" + sysTableType + " must be bound to the catalog's snapshot on the FE");
        }
    }

    @Test
    public void keepsThePinOffSystemTablesWhoseRowTypeFollowsTheBaseTable(@TempDir Path warehouse)
            throws Exception {
        // PaimonJniScanner#initTable calls table.copy(table.options()) unconditionally, and every
        // system-table wrapper delegates copy to FileStoreTable#copy, which time-travels the schema
        // to scan.snapshot-id. $audit_log / $ro / $binlog / $row_tracking derive their row type from
        // the base table, so pinning them would rewind the BE schema: c2, added after the pinned
        // snapshot, is planned by the FE and then rejected by getProjected() with "RequiredField c2
        // not found in schema".
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable versionManaged = newTable(warehouse, catalogEnvironment(catalog), C1, C2);

        Table forBackend = provider().tableForBackend(sysHandle("audit_log", versionManaged),
                SystemTableLoader.load("audit_log", versionManaged));

        Assertions.assertNull(forBackend.options().get("scan.snapshot-id"),
                "a row-type-following system table must not be time-travelled on the BE");
        Assertions.assertTrue(forBackend.rowType().getFieldNames().contains("c2"));
        Assertions.assertEquals(0, catalog.loadSnapshotCalls,
                "not merely unpinned: the catalog's pointer must never even be consulted");
    }

    // ==================== authorization the BE can no longer do ====================

    @Test
    public void authorizesTheDeferredScanBeforeDroppingTheCatalogLoader(@TempDir Path warehouse) {
        // $files only plans partition-level splits on the FE and re-plans the base table on the BE,
        // where Catalog#authTableQuery is what enforces query-auth. A loader-less table turns that
        // check into a permanent allow, so a denied query would come back as a successful metadata
        // read: authorize here instead, while the loader is still around.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        catalog.denyAuthWith("no privilege for db.tbl");
        FileStoreTable authorized = newTable(warehouse, catalogEnvironment(catalog),
                queryAuthEnabled(), C1);

        RuntimeException denied = Assertions.assertThrows(RuntimeException.class,
                () -> provider().tableForBackend(sysHandle("files", authorized),
                        SystemTableLoader.load("files", authorized)));

        Assertions.assertTrue(rootCauseMessage(denied).contains("no privilege for db.tbl"),
                "a denied query must not reach the BE as a catalog-less table");
    }

    @Test
    public void leavesDataSystemTablesTheirOwnProjectedAuthorization(@TempDir Path warehouse) {
        // $ro / $row_tracking / $audit_log / $binlog keep planning on the FE through
        // DataTableBatchScan, which already calls authTableQuery with the slot projection
        // (auth(readType.getFieldNames())). A second auth(null) here means "every column", so a user
        // allowed to read only c1 would be rejected for "SELECT c1 FROM tbl$ro".
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable dataTable = newTable(warehouse, catalogEnvironment(catalog),
                queryAuthEnabled(), C1);

        provider().tableForBackend(sysHandle("ro", dataTable), SystemTableLoader.load("ro", dataTable));

        Assertions.assertTrue(catalog.authCalls.isEmpty(),
                "only $files, which loses its authorization by re-planning on the BE, may transfer it");
    }

    // ==================== the fallback-branch pair ====================

    @Test
    public void fallbackBranchKeepsTheGenerationTheFeCaptured(@TempDir Path warehouse) throws Exception {
        // FallbackReadFileStoreTable#schema() only exposes its main branch, so rebuilding it through
        // FileStoreTableFactory#create re-reads the fallback branch from
        // SchemaManager(fallbackBranch).latest() instead of the object the FE planned with. After
        // external DDL publishes a new generation the BE would get main M1 + fallback F2 and die in
        // FallbackReadFileStoreTable#validateSchema.
        FileStoreTable[] captured = fallbackPairWithNewerGenerationOnDisk(warehouse, "fb_generation");

        FileStoreTable forBackend = PaimonScanPlanProvider.dropCatalogLoader(
                new FallbackReadFileStoreTable(captured[0], captured[1]));

        assertPairMatchesTheFeGeneration(forBackend, captured[0], captured[1]);
    }

    @Test
    public void fallbackBranchSurvivesAPaimonTableDecorator(@TempDir Path warehouse) throws Exception {
        // With file based privileges enabled PrivilegedCatalog#getTable hands out
        // Privileged(FallbackRead(..)). A direct instanceof looks straight past that wrapper and
        // rebuilds an ordinary table from the delegated main branch, so the BE loses the
        // FallbackReadFileStoreTable.Read that dispatches a fallback split to the fallback branch -
        // while the FE keeps planning the wrapper and can still emit one.
        FileStoreTable[] captured = fallbackPairWithNewerGenerationOnDisk(warehouse, "fb_decorated");
        FileStoreTable decorated = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(captured[0], captured[1]),
                new AllGrantedPrivilegeChecker(), Identifier.create("db", "tbl"));

        assertPairMatchesTheFeGeneration(PaimonScanPlanProvider.dropCatalogLoader(decorated),
                captured[0], captured[1]);
    }

    @Test
    public void authorizesBothBranchesOfAFallbackPair(@TempDir Path warehouse) {
        // FallbackReadFileStoreTable#newScan builds a FallbackReadScan over both branches' own scans,
        // so each authorizes itself, and FileStoreTableFactory#create gives the fallback branch a
        // CatalogEnvironment of its own carrying a branch-qualified Identifier. The pair delegates
        // catalogEnvironment() to its main branch, so authorizing the pair checks main and silently
        // skips the fallback branch - and once the loaders are dropped that missing check is a
        // permanent allow, letting a user denied on the fallback branch read the fallback rows.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        Identifier mainIdentifier = Identifier.create("db", "tbl");
        Identifier fallbackIdentifier = new Identifier("db", "tbl", "fb");
        Map<String, String> fallbackOptions = queryAuthEnabled();
        fallbackOptions.put("branch", "fb");
        FileStoreTable main = newTable(warehouse,
                new CatalogEnvironment(mainIdentifier, null, () -> catalog, null, null, true),
                queryAuthEnabled(), C1);
        FileStoreTable fallback = newTable(warehouse,
                new CatalogEnvironment(fallbackIdentifier, null, () -> catalog, null, null, true),
                fallbackOptions, C1);

        PaimonScanPlanProvider.authorizeDeferredScan(new FallbackReadFileStoreTable(main, fallback));

        Assertions.assertEquals(Arrays.asList(mainIdentifier, fallbackIdentifier), catalog.authCalls,
                "both branches must be authorized, each against its own identifier");
    }

    // ==================== the relation's own scan params ====================

    @Test
    public void relationOptionsSurviveTheRebuiltSystemTable(@TempDir Path warehouse) {
        // The FE applies @options to the wrapper the handle carries (resolveScanTable ->
        // PaimonScanParams#applyOptions), but the BE is handed a wrapper rebuilt over a catalog-less
        // base - a different object, on which that copy() never ran. Without re-applying them the
        // reader that materializes this table's splits falls back to the unpinned latest state.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable dataTable = newTable(warehouse, catalogEnvironment(catalog), C1);
        Map<String, String> pinned = PaimonScanParams.markAsOptions(Collections.singletonMap(
                org.apache.paimon.CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "4"));
        PaimonTableHandle handle = sysHandle("ro", dataTable).withScanOptions(pinned);

        Table forBackend = provider().tableForBackend(handle,
                PaimonScanParams.applyOptions(SystemTableLoader.load("ro", dataTable), pinned));

        // $ro delegates options() to the data table it wraps, so this reads the rebuilt base.
        Assertions.assertEquals("4", forBackend.options()
                .get(org.apache.paimon.CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void relationOptionsResolveFallbackSnapshotBeforeDroppingLoaders(@TempDir Path warehouse)
            throws Exception {
        FileStoreTable onDisk = commit(newRealTable(warehouse, "fallback_options_pin"), 2);
        VersionManagedCatalog mainCatalog = new VersionManagedCatalog(warehouse, true);
        VersionManagedCatalog fallbackCatalog = new VersionManagedCatalog(warehouse, true);
        mainCatalog.pointAt(onDisk, 2L);
        fallbackCatalog.pointAt(onDisk, 1L);
        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put("scan.fallback-branch", "fb");
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        FileStoreTable pair = new FallbackReadFileStoreTable(
                withCatalogEnvironment(onDisk.copyWithoutTimeTravel(mainOptions),
                        catalogEnvironment(mainCatalog)),
                withCatalogEnvironment(onDisk.copyWithoutTimeTravel(fallbackOptions),
                        catalogEnvironment(fallbackCatalog)));
        Map<String, String> pinned = PaimonScanParams.markAsOptions(
                Collections.singletonMap("scan.snapshot-id", "2"));
        PaimonTableHandle handle = sysHandle("ro", pair).withScanOptions(pinned);

        Table forBackend = provider().tableForBackend(handle,
                PaimonScanParams.applyOptions(SystemTableLoader.load("ro", pair), pinned));

        Field wrapped = ReadOptimizedTable.class.getDeclaredField("wrapped");
        wrapped.setAccessible(true);
        FallbackReadFileStoreTable backendPair =
                (FallbackReadFileStoreTable) wrapped.get(forBackend);
        Assertions.assertEquals("2", backendPair.wrapped().options().get("scan.snapshot-id"));
        Assertions.assertEquals("1", backendPair.fallback().options().get("scan.snapshot-id"));
        Assertions.assertTrue(fallbackCatalog.loadSnapshotCalls > 0,
                "fallback translation must consult its catalog before the loader is removed");
    }

    @Test
    public void incrementalRangeIsResolvedOnTheCatalogVisibleSnapshot(@TempDir Path warehouse)
            throws Exception {
        // Paimon selects the incremental scanner from incremental-between*, so the snapshot pin
        // cannot bound this scan - and isolateIncrementalRead clears it anyway. The timestamp form
        // would then resolve its endpoints inside the BE reader, through
        // SnapshotManager#earlierOrEqualTimeMills, whose search runs up to latestSnapshotId(): the
        // snapshot directory once the loader is gone. Resolve them here instead.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable onDisk = commit(newRealTable(warehouse, "t"), 2);
        catalog.pointAt(onDisk, 1L);
        FileStoreTable versionManaged = withCatalogEnvironment(onDisk, catalogEnvironment(catalog));
        long firstCommitMillis = onDisk.snapshotManager().snapshot(1L).timeMillis();

        Map<String, String> unbounded = new HashMap<>();
        unbounded.put("incremental-between-timestamp", firstCommitMillis + "," + Long.MAX_VALUE);
        Map<String, String> bound =
                PaimonIncrementalScanParams.bindRangeToCatalog(unbounded, versionManaged);

        // The range closes on the catalog's snapshot 1, never on snapshot 2 sitting on disk. The
        // start is the snapshot at or before that wall clock - snapshot 1 itself here - exactly what
        // IncrementalDeltaStartingScanner#betweenTimestamps would have resolved on the BE.
        Assertions.assertEquals("1,1", bound.get("incremental-between"));
        // Cleared rather than dropped: Paimon's copy() removes a key only when it maps to null.
        Assertions.assertTrue(bound.containsKey("incremental-between-timestamp"));
        Assertions.assertNull(bound.get("incremental-between-timestamp"));

        // And a range that opens BEFORE the earliest snapshot keeps paimon's exclusive-start rule:
        // the id before the earliest one, so the earliest snapshot itself is included.
        Map<String, String> fromTheStart = new HashMap<>();
        fromTheStart.put("incremental-between-timestamp", (firstCommitMillis - 1) + "," + Long.MAX_VALUE);
        Assertions.assertEquals("0,1", PaimonIncrementalScanParams
                .bindRangeToCatalog(fromTheStart, versionManaged).get("incremental-between"));
    }

    @Test
    public void incrementalRangeAlreadyOlderThanTheCatalogSnapshotIsLeftAlone(@TempDir Path warehouse)
            throws Exception {
        // An end older than the catalog's snapshot already resolves to the same id on both sides,
        // because every snapshot the catalog has not published yet is younger than it.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable onDisk = commit(newRealTable(warehouse, "t"), 2);
        catalog.pointAt(onDisk, 2L);
        FileStoreTable versionManaged = withCatalogEnvironment(onDisk, catalogEnvironment(catalog));

        Map<String, String> past = new HashMap<>();
        past.put("incremental-between-timestamp",
                "0," + (onDisk.snapshotManager().snapshot(2L).timeMillis() - 1));

        Assertions.assertSame(past,
                PaimonIncrementalScanParams.bindRangeToCatalog(past, versionManaged));
    }

    @Test
    public void incrementalRangeIsLeftAloneForAFilesystemCatalog(@TempDir Path warehouse) throws Exception {
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, false);
        FileStoreTable plain = withCatalogEnvironment(
                commit(newRealTable(warehouse, "t"), 1), catalogEnvironment(catalog));

        Map<String, String> range = new HashMap<>();
        range.put("incremental-between-timestamp", "0," + Long.MAX_VALUE);

        Assertions.assertSame(range, PaimonIncrementalScanParams.bindRangeToCatalog(range, plain));
    }

    @Test
    public void incrementalRangeIsNotBoundOnAFallbackBranchPair(@TempDir Path warehouse) throws Exception {
        // The two branches keep independent snapshot id sequences, and
        // FallbackReadFileStoreTable#rewriteFallbackOptions translates only scan.snapshot-id, so
        // incremental-between reaches the fallback branch verbatim. A main-branch id range bound here
        // would be validated against the fallback branch's own range in
        // IncrementalDeltaStartingScanner#betweenSnapshotIds and either fail out of range or select
        // unrelated commits. The timestamp form is branch-agnostic and each branch resolves it
        // against its own SnapshotManager, so it has to reach the BE untouched.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable onDisk = commit(newRealTable(warehouse, "t"), 2);
        catalog.pointAt(onDisk, 1L);
        // Stubbed so that the main branch alone WOULD have produced a range: without the guard this
        // test sees "0,1" written into incremental-between, not an unchanged map.
        FileStoreTable main = withCatalogEnvironment(onDisk, catalogEnvironment(catalog));
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        FileStoreTable fallback = newTable(warehouse, catalogEnvironment(catalog), fallbackOptions, C1);
        FileStoreTable pair = new FallbackReadFileStoreTable(main, fallback);

        Map<String, String> range = new HashMap<>();
        range.put("incremental-between-timestamp",
                onDisk.snapshotManager().snapshot(1L).timeMillis() + "," + Long.MAX_VALUE);

        Assertions.assertSame(range, PaimonIncrementalScanParams.bindRangeToCatalog(range, pair));
        // And through the decorator PrivilegedCatalog adds, since that is the shape Doris receives.
        Assertions.assertSame(range, PaimonIncrementalScanParams.bindRangeToCatalog(range,
                PrivilegedFileStoreTable.wrap(pair, new AllGrantedPrivilegeChecker(),
                        Identifier.create("db", "tbl"))));
        // Not merely "the output equals the input": the main branch's snapshots must never be read,
        // because reading them is what produces an id range that cannot be carried to the fallback.
        Assertions.assertEquals(0, catalog.loadSnapshotCalls);
    }

    @Test
    public void incrementalPartitionsScanBindsItsRangeBeforeTheBackend(@TempDir Path warehouse)
            throws Exception {
        // $partitions is the one system table that both re-plans on the BE (PartitionsRead
        // #createReader -> newScan().listPartitionEntries()) and accepts @incr, so it is the one that
        // has to reach the BE with an already-resolved range.
        VersionManagedCatalog catalog = new VersionManagedCatalog(warehouse, true);
        FileStoreTable onDisk = commit(newRealTable(warehouse, "t"), 2);
        catalog.pointAt(onDisk, 1L);
        FileStoreTable versionManaged = withCatalogEnvironment(onDisk, catalogEnvironment(catalog));
        Map<String, String> incremental = new HashMap<>();
        incremental.put("incremental-between-timestamp",
                onDisk.snapshotManager().snapshot(1L).timeMillis() + "," + Long.MAX_VALUE);
        PaimonTableHandle handle = sysHandle("partitions", versionManaged).withScanOptions(incremental);

        provider().tableForBackend(handle,
                SystemTableLoader.load("partitions", versionManaged).copy(incremental));

        // The range was closed here, on the catalog's snapshot, instead of inside the BE reader:
        // pinCatalogSnapshot reads the pointer once and bindRangeToCatalog reads it again.
        Assertions.assertTrue(catalog.loadSnapshotCalls >= 2,
                "an @incr $partitions read must resolve its endpoints against the catalog");
    }

    // ==================== fixtures ====================

    private static PaimonTableHandle dataHandle() {
        return new PaimonTableHandle("db", "tbl", Collections.emptyList(), Collections.emptyList());
    }

    private static PaimonTableHandle sysHandle(String sysTableType, FileStoreTable base) {
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable("db", "tbl", sysTableType,
                PaimonScanParams.requiresPaimonReader(sysTableType));
        handle.setSysBaseTable(base);
        return handle;
    }

    private static Map<String, String> queryAuthEnabled() {
        Map<String, String> options = new HashMap<>();
        options.put("query-auth.enabled", "true");
        return options;
    }

    private static CatalogEnvironment catalogEnvironment(VersionManagedCatalog catalog) {
        return new CatalogEnvironment(Identifier.create("db", "tbl"), null, () -> catalog, null, null,
                catalog.supportsVersionManagement());
    }

    /** A table object with the given schema; no files are written, so nothing touches the disk. */
    private static FileStoreTable newTable(Path warehouse, CatalogEnvironment env, DataField... fields) {
        return newTable(warehouse, env, new HashMap<>(), fields);
    }

    private static FileStoreTable newTable(Path warehouse, CatalogEnvironment env,
            Map<String, String> options, DataField... fields) {
        List<DataField> fieldList = Arrays.asList(fields);
        TableSchema schema = new TableSchema(0L, fieldList, fieldList.size() - 1,
                Collections.emptyList(), Collections.emptyList(), options, "");
        return FileStoreTableFactory.create(LocalFileIO.create(),
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/tbl"), schema, env);
    }

    /** A real, on-disk table so its {@code SnapshotManager} answers from real snapshot files. */
    private static FileStoreTable newRealTable(Path warehouse, String name) throws Exception {
        org.apache.paimon.fs.Path tablePath =
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/" + name);
        LocalFileIO fileIO = LocalFileIO.create();
        new SchemaManager(fileIO, tablePath).createTable(new Schema(
                Collections.singletonList(C1), Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), ""));
        return FileStoreTableFactory.create(fileIO, tablePath);
    }

    private static FileStoreTable commit(FileStoreTable table, int snapshots) throws Exception {
        for (int i = 0; i < snapshots; i++) {
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                write.write(GenericRow.of(i));
                commit.commit(write.prepareCommit());
            }
        }
        return table;
    }

    /** Re-binds an on-disk table to a catalog environment, the way a catalog load would. */
    private static FileStoreTable withCatalogEnvironment(FileStoreTable table, CatalogEnvironment env) {
        return FileStoreTableFactory.createWithoutFallbackBranch(table.fileIO(), table.location(),
                table.schema(), new Options(), env);
    }

    /**
     * The {@code {main, fallback}} pair the FE captured (M1 / F1), with a newer fallback generation
     * (F2, one column wider) already published on the filesystem - so a rebuild that re-reads the
     * branch instead of reusing the captured object shows up in the row type.
     */
    private static FileStoreTable[] fallbackPairWithNewerGenerationOnDisk(Path warehouse, String name)
            throws Exception {
        org.apache.paimon.fs.Path tablePath =
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/" + name);
        // F2: the generation external DDL has already published on the fallback branch.
        new SchemaManager(LocalFileIO.create(), tablePath, "fb").createTable(new Schema(
                Arrays.asList(C1, C2), Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), ""));

        // F1 / M1: what the FE captured and planned this query with.
        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put("scan.fallback-branch", "fb");
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        return new FileStoreTable[] {
                branchTable(warehouse, tablePath, mainOptions),
                branchTable(warehouse, tablePath, fallbackOptions)};
    }

    /**
     * One branch of a {@code scan.fallback-branch} pair, built the way Paimon builds them: without
     * re-expanding the fallback branch, so the caller can wrap the two itself.
     */
    private static FileStoreTable branchTable(Path warehouse, org.apache.paimon.fs.Path tablePath,
            Map<String, String> options) {
        TableSchema schema = new TableSchema(0L, Collections.singletonList(C1), 0,
                Collections.emptyList(), Collections.emptyList(), options, "");
        return FileStoreTableFactory.createWithoutFallbackBranch(LocalFileIO.create(), tablePath,
                schema, new Options(),
                catalogEnvironment(new VersionManagedCatalog(warehouse, false)));
    }

    private static void assertPairMatchesTheFeGeneration(FileStoreTable forBackend, FileStoreTable main,
            FileStoreTable fallback) {
        Assertions.assertTrue(forBackend instanceof FallbackReadFileStoreTable,
                "the pair itself must reach the BE, or a fallback split has no reader there");
        FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) forBackend;
        // The fallback branch must stay on F1 - not the c2 generation sitting on the filesystem.
        Assertions.assertEquals(fallback.rowType(), pair.fallback().rowType());
        Assertions.assertEquals(main.rowType(), pair.wrapped().rowType());
        // And neither branch may carry the loader that drags the metastore stack onto the BE.
        Assertions.assertNull(pair.wrapped().catalogEnvironment().catalogLoader());
        Assertions.assertNull(pair.fallback().catalogEnvironment().catalogLoader());
    }

    /** Decodes {@code paimon.serialized_table} the way {@code PaimonJniScanner#initTable} does. */
    private static FileStoreTable deserializeTable(String encoded) throws Exception {
        byte[] raw = encoded.getBytes(StandardCharsets.UTF_8);
        byte[] bytes;
        try {
            bytes = Base64.getUrlDecoder().decode(raw);
        } catch (IllegalArgumentException urlReject) {
            bytes = Base64.getDecoder().decode(raw);
        }
        return InstantiationUtil.deserializeObject(bytes,
                PaimonBackendBoundTableTest.class.getClassLoader());
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable cause = t;
        StringBuilder messages = new StringBuilder();
        while (cause != null) {
            messages.append(cause.getMessage()).append('\n');
            cause = cause.getCause();
        }
        return messages.toString();
    }

    /**
     * A real {@link FileSystemCatalog} that additionally answers the two calls the BE would have made
     * through the catalog loader: {@code loadSnapshot} (the committed-snapshot pointer a
     * version-managed catalog owns, which the test sets independently of the snapshot directory) and
     * {@code authTableQuery} (recorded, and optionally denied). Both are {@code
     * UnsupportedOperationException} on the base class, so overriding them is what makes an offline
     * version-managed catalog possible at all.
     */
    private static final class VersionManagedCatalog extends FileSystemCatalog {

        private final boolean supportsVersionManagement;
        private final Map<String, TableSnapshot> pointers = new HashMap<>();
        private final List<Identifier> authCalls = new ArrayList<>();
        private String authDenialMessage;
        private int loadSnapshotCalls;

        private VersionManagedCatalog(Path warehouse, boolean supportsVersionManagement) {
            super(LocalFileIO.create(), new org.apache.paimon.fs.Path("file://" + warehouse));
            this.supportsVersionManagement = supportsVersionManagement;
        }

        /** Publishes {@code snapshotId} as the committed pointer, whatever the directory holds. */
        private void pointAt(FileStoreTable table, long snapshotId) {
            Snapshot snapshot = table.snapshotManager().snapshot(snapshotId);
            pointers.put(table.location().getName(), new TableSnapshot(snapshot, 0L, 0L, 0L, 0L));
        }

        private void denyAuthWith(String message) {
            this.authDenialMessage = message;
        }

        @Override
        public boolean supportsVersionManagement() {
            return supportsVersionManagement;
        }

        @Override
        public Optional<TableSnapshot> loadSnapshot(Identifier identifier) {
            loadSnapshotCalls++;
            // Keyed by the on-disk table directory rather than the Identifier: the fixtures bind one
            // catalog to tables built straight from a path, so the Identifier is the same for all.
            return pointers.isEmpty()
                    ? Optional.empty()
                    : Optional.of(pointers.values().iterator().next());
        }

        @Override
        public List<String> authTableQuery(Identifier identifier, List<String> select) {
            authCalls.add(identifier);
            if (authDenialMessage != null) {
                throw new RuntimeException(authDenialMessage);
            }
            return Collections.emptyList();
        }

        @Override
        public void close() {
            // CatalogEnvironment#tableQueryAuth and SnapshotLoaderImpl#load both close the catalog
            // they load; this instance is shared across those calls, so closing must not disarm it.
        }
    }
}
