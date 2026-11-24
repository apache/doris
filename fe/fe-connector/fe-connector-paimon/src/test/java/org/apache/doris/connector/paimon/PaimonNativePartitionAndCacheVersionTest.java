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

import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorSqlCacheVersion;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PaimonNativePartitionAndCacheVersionTest {

    @TempDir
    Path warehouse;

    @Test
    public void supportsNativePartitionPlanningHonorsSystemTableAndSessionFlag() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        PaimonConnectorMetadata md = new PaimonConnectorMetadata(
                ops, PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
        PaimonTableHandle dataHandle = new PaimonTableHandle(
                "db1", "t1", Collections.singletonList("dt"), Collections.emptyList());
        PaimonTableHandle sysHandle = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);

        Assertions.assertTrue(md.supportsNativePartitionPlanning(null, dataHandle));
        Assertions.assertFalse(md.supportsNativePartitionPlanning(null, sysHandle));

        String key = "enable_paimon_internal_partition_pruning";
        Assertions.assertTrue(md.supportsNativePartitionPlanning(
                session(Collections.emptyMap(), new MapStatementScope()), dataHandle));
        Assertions.assertTrue(md.supportsNativePartitionPlanning(
                session(Collections.singletonMap(key, "false"), new MapStatementScope()), dataHandle));
        Assertions.assertFalse(md.supportsNativePartitionPlanning(
                session(Collections.singletonMap(key, "true"), new MapStatementScope()), dataHandle));
        Assertions.assertFalse(md.supportsNativePartitionPlanning(
                session(Collections.singletonMap(key, "false"), new MapStatementScope()), sysHandle));
    }

    @Test
    public void sqlCacheAndPartitionViewVersionsTrackSnapshotsSchemaEvolutionAndRollback() throws Exception {
        try (Catalog catalog = newCatalog()) {
            catalog.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            catalog.createTable(id, partitionedSchema(Collections.emptyMap()), false);

            PaimonCatalogOps ops = new CountingCatalogOps(catalog);
            PaimonConnectorMetadata md1 = liveMetadata(ops);
            ConnectorSession s1 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle base1 = md1.getTableHandle(s1, "db1", "t1").orElseThrow();

            ConnectorTableHandle emptyPinned = pinLatest(md1, s1, base1);
            ConnectorSqlCacheVersion emptyVersion = md1.getSqlCacheVersion(s1, emptyPinned).orElseThrow();
            Assertions.assertEquals(0L, emptyVersion.getUpdateTimeMillis());
            Assertions.assertEquals(emptyVersion.getVersion(),
                    md1.getPartitionViewVersion(s1, emptyPinned).orElseThrow());

            FileStoreTable table = (FileStoreTable) catalog.getTable(id);
            writeRow(table, 1, "2026-01-01");
            long snap1Millis = table.snapshot(1L).timeMillis();

            PaimonConnectorMetadata md2 = liveMetadata(ops);
            ConnectorSession s2 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle snap1Pinned = pinLatest(md2, s2, md2.getTableHandle(s2, "db1", "t1").orElseThrow());
            ConnectorSqlCacheVersion v1 = md2.getSqlCacheVersion(s2, snap1Pinned).orElseThrow();
            Assertions.assertNotEquals(emptyVersion.getVersion(), v1.getVersion());
            Assertions.assertEquals(snap1Millis, v1.getUpdateTimeMillis());
            Assertions.assertEquals(v1.getVersion(), md2.getPartitionViewVersion(s2, snap1Pinned).orElseThrow());

            writeRow((FileStoreTable) catalog.getTable(id), 2, "2026-01-02");
            long snap2Millis = ((FileStoreTable) catalog.getTable(id)).snapshot(2L).timeMillis();

            PaimonConnectorMetadata md3 = liveMetadata(ops);
            ConnectorSession s3 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle base3 = md3.getTableHandle(s3, "db1", "t1").orElseThrow();
            ConnectorTableHandle snap2Pinned = pinLatest(md3, s3, base3);
            ConnectorSqlCacheVersion v2 = md3.getSqlCacheVersion(s3, snap2Pinned).orElseThrow();
            Assertions.assertNotEquals(v1.getVersion(), v2.getVersion());
            Assertions.assertEquals(snap2Millis, v2.getUpdateTimeMillis());

            ConnectorMvccSnapshot historicalSnap1 = md3.resolveTimeTravel(
                    s3, base3, ConnectorTimeTravelSpec.snapshotId("1")).orElseThrow();
            ConnectorTableHandle historicalHandle1 = md3.applySnapshot(s3, base3, historicalSnap1);
            ConnectorSqlCacheVersion historicalV1 = md3.getSqlCacheVersion(s3, historicalHandle1).orElseThrow();

            // Schema-only ALTER without a new data snapshot: existing pinned handles stay stable,
            // next latest query sees a new version, while historical FOR VERSION AS OF 1 stays unchanged.
            catalog.alterTable(id, Collections.singletonList(
                    SchemaChange.addColumn("extra", DataTypes.STRING())), false);
            Assertions.assertEquals(v2.getVersion(),
                    md3.getSqlCacheVersion(s3, snap2Pinned).orElseThrow().getVersion());

            PaimonConnectorMetadata md4 = liveMetadata(ops);
            ConnectorSession s4 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle base4 = md4.getTableHandle(s4, "db1", "t1").orElseThrow();
            ConnectorTableHandle evolvedPinned = pinLatest(md4, s4, base4);
            ConnectorSqlCacheVersion v2Evolved = md4.getSqlCacheVersion(s4, evolvedPinned).orElseThrow();
            Assertions.assertNotEquals(v2.getVersion(), v2Evolved.getVersion());

            ConnectorMvccSnapshot historicalSnap1AfterAlter = md4.resolveTimeTravel(
                    s4, base4, ConnectorTimeTravelSpec.snapshotId("1")).orElseThrow();
            Assertions.assertEquals(historicalV1.getVersion(),
                    md4.getSqlCacheVersion(s4, md4.applySnapshot(s4, base4, historicalSnap1AfterAlter))
                            .orElseThrow().getVersion());

            // Rollback and reuse of snapshot id 2 with different content must produce distinct versions.
            ((FileStoreTable) catalog.getTable(id)).rollbackTo(1L);
            PaimonConnectorMetadata md5 = liveMetadata(ops);
            ConnectorSession s5 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle rolledBack = pinLatest(md5, s5, md5.getTableHandle(s5, "db1", "t1").orElseThrow());
            ConnectorSqlCacheVersion vRolledBack = md5.getSqlCacheVersion(s5, rolledBack).orElseThrow();
            Assertions.assertNotEquals(v2Evolved.getVersion(), vRolledBack.getVersion());

            writeRowWithExtra((FileStoreTable) catalog.getTable(id), 3, "2026-01-03", "new");
            PaimonConnectorMetadata md6 = liveMetadata(ops);
            ConnectorSession s6 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle rewrittenSnap2 = pinLatest(md6, s6, md6.getTableHandle(s6, "db1", "t1").orElseThrow());
            ConnectorSqlCacheVersion vRewrittenSnap2 = md6.getSqlCacheVersion(s6, rewrittenSnap2).orElseThrow();
            Assertions.assertNotEquals(v2.getVersion(), vRewrittenSnap2.getVersion());
            Assertions.assertNotEquals(v2Evolved.getVersion(), vRewrittenSnap2.getVersion());
            Assertions.assertEquals(0, ((CountingCatalogOps) ops).listPartitionsCount);
        }
    }

    @Test
    public void sqlCacheVersionDistinguishesRecreatedTablesTagsBranchesAndFallbackBranches() throws Exception {
        try (Catalog catalog = newCatalog()) {
            catalog.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            catalog.createTable(id, partitionedSchema(Collections.emptyMap()), false);
            PaimonCatalogOps ops = new CountingCatalogOps(catalog);

            // Empty table drop + recreate must not reuse the old empty version.
            PaimonConnectorMetadata mdEmpty1 = liveMetadata(ops);
            ConnectorSession s1 = session(Collections.emptyMap(), new MapStatementScope());
            String firstEmpty = mdEmpty1.getSqlCacheVersion(
                    s1, pinLatest(mdEmpty1, s1, mdEmpty1.getTableHandle(s1, "db1", "t1").orElseThrow()))
                    .orElseThrow().getVersion();
            Thread.sleep(5L);
            catalog.dropTable(id, false);
            catalog.createTable(id, partitionedSchema(Collections.emptyMap()), false);

            PaimonConnectorMetadata mdEmpty2 = liveMetadata(ops);
            ConnectorSession s2 = session(Collections.emptyMap(), new MapStatementScope());
            String secondEmpty = mdEmpty2.getSqlCacheVersion(
                    s2, pinLatest(mdEmpty2, s2, mdEmpty2.getTableHandle(s2, "db1", "t1").orElseThrow()))
                    .orElseThrow().getVersion();
            Assertions.assertNotEquals(firstEmpty, secondEmpty);

            // Mutable name-only tag pins (both direct tag time travel and @options scan.tag-name) decline SQL
            // cache / partition-view versioning to prevent mutable tag replacement races (including A -> B -> A
            // and fallback-branch tag divergence), while direct tag partition listing still reflects the tag's
            // retained partitions even after snapshot 1's snapshot file was expired.
            FileStoreTable table = (FileStoreTable) catalog.getTable(id);
            writeRow(table, 10, "2026-02-01");
            table.createTag("v1", 1L);
            writeRow((FileStoreTable) catalog.getTable(id), 20, "2026-02-02");
            FileStoreTable refreshed = (FileStoreTable) catalog.getTable(id);
            refreshed.fileIO().deleteQuietly(refreshed.snapshotManager().snapshotPath(1L));

            PaimonConnectorMetadata mdTag1 = liveMetadata(ops);
            ConnectorSession sTag1 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle baseTag1 = mdTag1.getTableHandle(sTag1, "db1", "t1").orElseThrow();
            ConnectorMvccSnapshot tagPin1 = mdTag1.resolveTimeTravel(
                    sTag1, baseTag1, ConnectorTimeTravelSpec.tag("v1")).orElseThrow();
            ConnectorTableHandle tagHandle1 = mdTag1.applySnapshot(sTag1, baseTag1, tagPin1);
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> mdTag1.getSqlCacheVersion(sTag1, tagHandle1));
            Assertions.assertFalse(mdTag1.getPartitionViewVersion(sTag1, tagHandle1).isPresent());
            List<ConnectorPartitionInfo> tagPartitions = mdTag1.listPartitions(sTag1, tagHandle1, Optional.empty());
            Assertions.assertEquals(Collections.singletonList("dt=2026-02-01"),
                    tagPartitions.stream()
                            .map(ConnectorPartitionInfo::getPartitionName)
                            .collect(Collectors.toList()));

            ConnectorMvccSnapshot tagOptionsPin = mdTag1.resolveTimeTravel(
                    sTag1, baseTag1, ConnectorTimeTravelSpec.options(
                            Collections.singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "v1"))).orElseThrow();
            ConnectorTableHandle tagOptionsHandle = mdTag1.applySnapshot(sTag1, baseTag1, tagOptionsPin);
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> mdTag1.getSqlCacheVersion(sTag1, tagOptionsHandle));
            Assertions.assertFalse(mdTag1.getPartitionViewVersion(sTag1, tagOptionsHandle).isPresent());

            // Branch + fallback-branch sensitivity: writing to the fallback branch invalidates main's version.
            Identifier fbId = Identifier.create("db1", "fb_tbl");
            catalog.createTable(fbId, partitionedSchema(Collections.emptyMap()), false);
            ((FileStoreTable) catalog.getTable(fbId)).createBranch("batch");
            catalog.alterTable(fbId, Collections.singletonList(
                    SchemaChange.setOption(CoreOptions.SCAN_FALLBACK_BRANCH.key(), "batch")), false);

            PaimonConnectorMetadata mdFb1 = liveMetadata(ops);
            ConnectorSession sFb1 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle fbBase1 = mdFb1.getTableHandle(sFb1, "db1", "fb_tbl").orElseThrow();
            ConnectorSqlCacheVersion fbBefore = mdFb1.getSqlCacheVersion(
                    sFb1, pinLatest(mdFb1, sFb1, fbBase1)).orElseThrow();

            FileStoreTable batchBranch = (FileStoreTable) catalog.getTable(
                    new Identifier("db1", "fb_tbl", "batch"));
            writeRow(batchBranch, 99, "2026-03-01");

            PaimonConnectorMetadata mdFb2 = liveMetadata(ops);
            ConnectorSession sFb2 = session(Collections.emptyMap(), new MapStatementScope());
            ConnectorTableHandle fbBase2 = mdFb2.getTableHandle(sFb2, "db1", "fb_tbl").orElseThrow();
            ConnectorSqlCacheVersion fbAfter = mdFb2.getSqlCacheVersion(
                    sFb2, pinLatest(mdFb2, sFb2, fbBase2)).orElseThrow();
            Assertions.assertNotEquals(fbBefore.getVersion(), fbAfter.getVersion());
            Assertions.assertTrue(fbAfter.getUpdateTimeMillis() > 0L);

            // Reading @branch('batch') produces an independent version from main.
            ConnectorMvccSnapshot branchSnap = mdFb2.resolveTimeTravel(
                    sFb2, fbBase2, ConnectorTimeTravelSpec.branch("batch")).orElseThrow();
            ConnectorSqlCacheVersion branchVer = mdFb2.getSqlCacheVersion(
                    sFb2, mdFb2.applySnapshot(sFb2, fbBase2, branchSnap)).orElseThrow();
            Assertions.assertNotEquals(fbAfter.getVersion(), branchVer.getVersion());

            // Nested ChainGroupReadTable (chain table delta branch): delta branch writes must invalidate main's cache.
            Identifier chainId = Identifier.create("db1", "chain_tbl");
            catalog.createTable(chainId, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("dt", DataTypes.STRING())
                    .partitionKeys("dt")
                    .primaryKey("id", "dt")
                    .option("bucket", "1")
                    .build(), false);
            ((FileStoreTable) catalog.getTable(chainId)).createBranch("snapshot");
            ((FileStoreTable) catalog.getTable(chainId)).createBranch("delta");
            FileStoreTable mainTable = (FileStoreTable) catalog.getTable(chainId);
            FileStoreTable snapTable = (FileStoreTable) catalog.getTable(
                    new Identifier("db1", "chain_tbl", "snapshot"));
            FileStoreTable deltaTable = (FileStoreTable) catalog.getTable(
                    new Identifier("db1", "chain_tbl", "delta"));
            FallbackReadFileStoreTable chain1 = new FallbackReadFileStoreTable(
                    mainTable, new ChainGroupReadTable(snapTable, deltaTable), true);
            Map<String, String> chainOpts1 = PaimonSchemaPin.capture(chain1, mainTable.schema().id(), -1L);
            chainOpts1.put("doris.internal.paimon.empty-scan", "true");
            ConnectorSqlCacheVersion chainBefore = PaimonSchemaPin.resolveSqlCacheVersion(
                    chain1, null, chainOpts1).orElseThrow();

            writeRow(deltaTable, 100, "2026-03-02");
            FileStoreTable deltaReloaded = (FileStoreTable) catalog.getTable(
                    new Identifier("db1", "chain_tbl", "delta"));
            FallbackReadFileStoreTable chain2 = new FallbackReadFileStoreTable(
                    mainTable, new ChainGroupReadTable(snapTable, deltaReloaded), true);
            Map<String, String> chainOpts2 = PaimonSchemaPin.capture(chain2, mainTable.schema().id(), -1L);
            chainOpts2.put("doris.internal.paimon.empty-scan", "true");
            ConnectorSqlCacheVersion chainAfter = PaimonSchemaPin.resolveSqlCacheVersion(
                    chain2, null, chainOpts2).orElseThrow();
            Assertions.assertNotEquals(chainBefore.getVersion(), chainAfter.getVersion());
        }
    }

    @Test
    public void unsupportedHandlesAndQueryAuthDisableSqlCacheAndGuardPartitionViewVersion() throws Exception {
        RecordingPaimonCatalogOps offlineOps = new RecordingPaimonCatalogOps();
        FakePaimonTable fakeTable = new FakePaimonTable("t1",
                RowType.builder().field("id", DataTypes.INT()).build(),
                Collections.emptyList(), Collections.emptyList());
        offlineOps.table = fakeTable;
        offlineOps.latestSnapshotId = java.util.OptionalLong.of(1L);
        RecordingConnectorContext authCtx = new RecordingConnectorContext();
        PaimonConnectorMetadata offlineMd = new PaimonConnectorMetadata(
                offlineOps, PaimonCatalogProperties.of(Collections.emptyMap()), authCtx);
        ConnectorSession scopedSession = session(Collections.emptyMap(), new MapStatementScope());

        PaimonTableHandle sysHandle = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> offlineMd.getSqlCacheVersion(scopedSession, sysHandle));
        Assertions.assertFalse(offlineMd.getPartitionViewVersion(scopedSession, sysHandle).isPresent());

        PaimonTableHandle unpinned = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        unpinned.setPaimonTable(fakeTable);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> offlineMd.getSqlCacheVersion(scopedSession, unpinned));
        Assertions.assertFalse(offlineMd.getPartitionViewVersion(scopedSession, unpinned).isPresent());

        PaimonTableHandle incrHandle = unpinned.withScanOptions(
                Collections.singletonMap(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> offlineMd.getSqlCacheVersion(scopedSession, incrHandle));
        Assertions.assertFalse(offlineMd.getPartitionViewVersion(scopedSession, incrHandle).isPresent());

        // Non-DataTable (e.g. FormatTable / FakePaimonTable without explicit seam override) fails loud for SQL cache.
        ConnectorTableHandle fakePinned = pinLatest(offlineMd, scopedSession, unpinned);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> offlineMd.getSqlCacheVersion(scopedSession, fakePinned));

        // Authentication failure propagates before touching catalogOps.
        authCtx.failAuth = true;
        Assertions.assertThrows(RuntimeException.class,
                () -> offlineMd.getSqlCacheVersion(scopedSession, fakePinned));

        // Partition view version requires a non-null session with an active statement scope, and rejects
        // query-auth-enabled tables for SQL cache.
        try (Catalog catalog = newCatalog()) {
            catalog.createDatabase("db1", false);
            Identifier authId = Identifier.create("db1", "auth_tbl");
            catalog.createTable(authId, partitionedSchema(Collections.singletonMap(
                    CoreOptions.QUERY_AUTH_ENABLED.key(), "true")), false);
            PaimonConnectorMetadata liveMd = liveMetadata(new CountingCatalogOps(catalog));
            ConnectorTableHandle authPinned = pinLatest(
                    liveMd, scopedSession, liveMd.getTableHandle(scopedSession, "db1", "auth_tbl").orElseThrow());
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> liveMd.getSqlCacheVersion(scopedSession, authPinned));

            Identifier plainId = Identifier.create("db1", "plain_tbl");
            catalog.createTable(plainId, partitionedSchema(Collections.emptyMap()), false);
            ConnectorTableHandle plainPinned = pinLatest(
                    liveMd, scopedSession, liveMd.getTableHandle(scopedSession, "db1", "plain_tbl").orElseThrow());
            Assertions.assertFalse(liveMd.getPartitionViewVersion(null, plainPinned).isPresent());
            Assertions.assertFalse(liveMd.getPartitionViewVersion(
                    session(Collections.emptyMap(), ConnectorStatementScope.NONE), plainPinned).isPresent());
            Assertions.assertTrue(liveMd.getPartitionViewVersion(scopedSession, plainPinned).isPresent());

            // Stale cached schema-0 handle (query-auth.enabled=false) after external schema-only ALTER enables
            // query-auth.enabled=true at schema 1: beginQuerySnapshot binds schema 1, and eligibility must inspect
            // that effective bound schema rather than the stale cached schema 0.
            Identifier evolvedAuthId = Identifier.create("db1", "evolved_auth_tbl");
            catalog.createTable(evolvedAuthId, partitionedSchema(Collections.emptyMap()), false);
            PaimonTableHandle staleBase = (PaimonTableHandle) liveMd.getTableHandle(
                    scopedSession, "db1", "evolved_auth_tbl").orElseThrow();
            staleBase.setPaimonTable(catalog.getTable(evolvedAuthId));
            catalog.alterTable(evolvedAuthId, Collections.singletonList(
                    SchemaChange.setOption(CoreOptions.QUERY_AUTH_ENABLED.key(), "true")), false);
            PaimonConnectorMetadata evolvedMd = liveMetadata(new CountingCatalogOps(catalog));
            ConnectorTableHandle evolvedPinned = pinLatest(evolvedMd, scopedSession, staleBase);
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> evolvedMd.getSqlCacheVersion(scopedSession, evolvedPinned));
            Assertions.assertFalse(evolvedMd.getPartitionViewVersion(scopedSession, evolvedPinned).isPresent());

            // Reverse schema-only ALTER (cached schema 0 had query-auth.enabled=true, schema 1 disables it):
            // effective bound schema 1 is eligible for SQL cache.
            PaimonTableHandle staleEnabledBase = (PaimonTableHandle) evolvedMd.getTableHandle(
                    scopedSession, "db1", "evolved_auth_tbl").orElseThrow();
            staleEnabledBase.setPaimonTable(catalog.getTable(evolvedAuthId));
            catalog.alterTable(evolvedAuthId, Collections.singletonList(
                    SchemaChange.setOption(CoreOptions.QUERY_AUTH_ENABLED.key(), "false")), false);
            PaimonConnectorMetadata disabledMd = liveMetadata(new CountingCatalogOps(catalog));
            ConnectorTableHandle disabledPinned = pinLatest(disabledMd, scopedSession, staleEnabledBase);
            Assertions.assertTrue(disabledMd.getSqlCacheVersion(scopedSession, disabledPinned).isPresent());

            // Fallback branch stale cached schema 0 (query-auth.enabled=false) after external ALTER enables
            // query-auth.enabled=true on the fallback branch at schema 1 must also disable SQL cache.
            Identifier fbAuthId = Identifier.create("db1", "fb_auth_tbl");
            catalog.createTable(fbAuthId, partitionedSchema(Collections.emptyMap()), false);
            ((FileStoreTable) catalog.getTable(fbAuthId)).createBranch("batch");
            catalog.alterTable(fbAuthId, Collections.singletonList(
                    SchemaChange.setOption(CoreOptions.SCAN_FALLBACK_BRANCH.key(), "batch")), false);
            PaimonTableHandle staleFbBase = (PaimonTableHandle) liveMd.getTableHandle(
                    scopedSession, "db1", "fb_auth_tbl").orElseThrow();
            staleFbBase.setPaimonTable(catalog.getTable(fbAuthId));
            catalog.alterTable(new Identifier("db1", "fb_auth_tbl", "batch"), Collections.singletonList(
                    SchemaChange.setOption(CoreOptions.QUERY_AUTH_ENABLED.key(), "true")), false);
            PaimonConnectorMetadata fbAuthMd = liveMetadata(new CountingCatalogOps(catalog));
            ConnectorTableHandle fbAuthPinned = pinLatest(fbAuthMd, scopedSession, staleFbBase);
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> fbAuthMd.getSqlCacheVersion(scopedSession, fbAuthPinned));
        }
    }

    private Catalog newCatalog() {
        return new FileSystemCatalog(LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()));
    }

    private static Schema partitionedSchema(Map<String, String> extraOptions) {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .partitionKeys("dt");
        extraOptions.forEach(builder::option);
        return builder.build();
    }

    private static PaimonConnectorMetadata liveMetadata(PaimonCatalogOps ops) {
        return new PaimonConnectorMetadata(
                ops, PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext(),
                new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE),
                new PaimonLatestSnapshotCache(0L, 1), null);
    }

    private static ConnectorTableHandle pinLatest(
            PaimonConnectorMetadata md, ConnectorSession session, ConnectorTableHandle base) {
        ConnectorMvccSnapshot snap = md.beginQuerySnapshot(session, base).orElseThrow();
        return md.applySnapshot(session, base, snap);
    }

    private static void writeRow(FileStoreTable table, int id, String dt) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(dt)));
            commit.commit(write.prepareCommit());
        }
    }

    private static void writeRowWithExtra(FileStoreTable table, int id, String dt, String extra) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString(dt), BinaryString.fromString(extra)));
            commit.commit(write.prepareCommit());
        }
    }

    private static ConnectorSession session(Map<String, String> props, ConnectorStatementScope scope) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q-paimon";
            }

            @Override
            public String getUser() {
                return "root";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public String getCatalogName() {
                return "paimon_cat";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return props;
            }

            @Override
            public ConnectorStatementScope getStatementScope() {
                return scope;
            }
        };
    }

    private static final class MapStatementScope implements ConnectorStatementScope {
        private final Map<String, Object> entries = new ConcurrentHashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return (T) entries.computeIfAbsent(key, ignored -> loader.get());
        }
    }

    private static final class CountingCatalogOps extends PaimonCatalogOps.CatalogBackedPaimonCatalogOps {
        int listPartitionsCount;

        CountingCatalogOps(Catalog catalog) {
            super(catalog);
        }

        @Override
        public java.util.List<org.apache.paimon.partition.Partition> listPartitions(
                Identifier identifier, org.apache.paimon.table.Table table)
                throws Catalog.TableNotExistException {
            listPartitionsCount++;
            return super.listPartitions(identifier, table);
        }
    }
}
