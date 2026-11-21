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

package org.apache.doris.catalog;

import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsRequest;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.MetaServiceResponseStatus;
import org.apache.doris.cloud.proto.Cloud.TabletIndexPB;
import org.apache.doris.cloud.proto.Cloud.TabletStatsPB;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TabletStatMgrTest {
    @Test
    public void testLocalChangelogReportsPreserveUnknownAndReplaceRetainedCount() throws Exception {
        InternalCatalog catalog = new InternalCatalog();
        MaterializedIndex changelog = createTable(catalog);
        OlapTable table = (OlapTable) catalog.getDbNullable(10L).getTableNullable(20L);
        MaterializedIndex base = table.getPartition(30L).getBaseIndex();
        LocalReplica baseReplica = localReplica(100L, 10L, true);
        LocalTablet baseTablet = new LocalTablet(100L);
        baseTablet.addReplica(baseReplica, true);
        base.addTablet(baseTablet, null, true);
        LocalReplica reportedReplica = localReplica(200L, 8L, false);
        LocalReplica defaultReplica = localReplica(201L, 0L, false);
        LocalReplica otherTabletReplica = localReplica(300L, 0L, false);
        LocalTablet first = new LocalTablet(200L);
        first.addReplica(reportedReplica, true);
        first.addReplica(defaultReplica, true);
        LocalTablet second = new LocalTablet(300L);
        second.addReplica(otherTabletReplica, true);
        changelog.addTablet(first, null, true);
        changelog.addTablet(second, null, true);
        changelog.setRowCount(12L);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of());
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            TabletStatMgr manager = new TabletStatMgr();
            manager.runAfterCatalogReady();
            Assertions.assertEquals(12L, changelog.getRowCount());
            Assertions.assertFalse(changelog.getRowCountReported());

            reportedReplica.setLastReportVersion(2L);
            manager.runAfterCatalogReady();
            Assertions.assertEquals(8L, changelog.getRowCount());
            Assertions.assertFalse(changelog.getRowCountReported());
            Assertions.assertEquals(10L, base.getRowCount());

            // Pick one reported replica per tablet, preserving the existing minimum-row policy.
            defaultReplica.setRowCount(3L);
            defaultReplica.setLastReportVersion(2L);
            otherTabletReplica.setRowCount(4L);
            otherTabletReplica.setLastReportVersion(2L);
            manager.runAfterCatalogReady();
            Assertions.assertEquals(7L, changelog.getRowCount());
            Assertions.assertTrue(changelog.getRowCountReported());

            // A later retained count can decrease after TTL, including a known empty index.
            reportedReplica.setRowCount(0L);
            defaultReplica.setRowCount(0L);
            otherTabletReplica.setRowCount(0L);
            manager.runAfterCatalogReady();
            Assertions.assertEquals(0L, changelog.getRowCount());
            Assertions.assertTrue(changelog.getRowCountReported());
        }
    }

    @Test
    public void testCloudChangelogNeedsValidRowsAndAllowsPartialAndEmptyReports() throws Exception {
        InternalCatalog catalog = new InternalCatalog();
        MaterializedIndex changelog = createTable(catalog);
        CloudReplica firstReplica = cloudReplica(200L);
        CloudReplica secondReplica = cloudReplica(300L);
        CloudTablet first = new CloudTablet(200L);
        first.addReplica(firstReplica, true);
        CloudTablet second = new CloudTablet(300L);
        second.addReplica(secondReplica, true);
        changelog.addTablet(first, null, true);
        changelog.addTablet(second, null, true);
        changelog.setRowCount(12L);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(invertedIndex.getReplicasByTabletId(200L)).thenReturn(Collections.singletonList(firstReplica));
        Mockito.when(invertedIndex.getReplicasByTabletId(300L)).thenReturn(Collections.singletonList(secondReplica));
        Method aggregate = CloudTabletStatMgr.class.getDeclaredMethod("updateStatInfo", List.class);
        aggregate.setAccessible(true);
        Method update = CloudTabletStatMgr.class.getDeclaredMethod("updateTabletStat",
                GetTabletStatsResponse.class, boolean.class);
        update.setAccessible(true);
        int oldStatsVersion = Config.cloud_get_tablet_stats_version;
        Config.cloud_get_tablet_stats_version = 1;
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
            CloudTabletStatMgr manager = new CloudTabletStatMgr();
            changelog.setRowCount(-1L);
            aggregate.invoke(manager, Collections.singletonList(10L));
            Assertions.assertEquals(-1L, changelog.getRowCount());
            Assertions.assertFalse(changelog.getRowCountReported());
            changelog.setRowCount(12L);
            aggregate.invoke(manager, Collections.singletonList(10L));
            Assertions.assertEquals(12L, changelog.getRowCount());
            Assertions.assertFalse(changelog.getRowCountReported());

            // An older response without num_rows conveys no row-count information.
            update.invoke(manager, GetTabletStatsResponse.newBuilder().addTabletStats(
                    TabletStatsPB.newBuilder().setIdx(TabletIndexPB.newBuilder().setTabletId(200L))).build(), false);
            aggregate.invoke(manager, Collections.singletonList(10L));
            Assertions.assertFalse(firstReplica.isRowCountReported());
            Assertions.assertEquals(12L, changelog.getRowCount());
            Assertions.assertFalse(changelog.getRowCountReported());

            update.invoke(manager, cloudRows(200L, 8L), true);
            aggregate.invoke(manager, Collections.singletonList(10L));
            Assertions.assertEquals(8L, changelog.getRowCount());
            Assertions.assertFalse(changelog.getRowCountReported());

            update.invoke(manager, cloudRows(300L, 4L), false);
            aggregate.invoke(manager, Collections.singletonList(10L));
            Assertions.assertEquals(12L, changelog.getRowCount());
            Assertions.assertTrue(changelog.getRowCountReported());

            update.invoke(manager, cloudRows(200L, 0L), false);
            update.invoke(manager, cloudRows(300L, 0L), false);
            aggregate.invoke(manager, Collections.singletonList(10L));
            Assertions.assertEquals(0L, changelog.getRowCount());
            Assertions.assertTrue(changelog.getRowCountReported());
            Assertions.assertEquals(0L, manager.getCloudTableStats().get(0).getRowCount());
        } finally {
            Config.cloud_get_tablet_stats_version = oldStatsVersion;
        }
    }

    @Test
    public void testCloudCountsReplicasOnce() throws Exception {
        InternalCatalog catalog = new InternalCatalog();
        MaterializedIndex changelog = createTable(catalog);
        CloudReplica firstReplica = cloudReplica(200L);
        CloudReplica secondReplica = cloudReplica(201L);
        firstReplica.setRowCount(8L);
        secondReplica.setRowCount(9L);
        firstReplica.setRowCountReported(true);
        secondReplica.setRowCountReported(true);
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(200L);
        Mockito.when(tablet.getReplicas()).thenReturn(Arrays.asList(firstReplica, secondReplica));
        changelog.addTablet(tablet, null, true);
        Method aggregate = CloudTabletStatMgr.class.getDeclaredMethod("updateStatInfo", List.class);
        aggregate.setAccessible(true);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            aggregate.invoke(new CloudTabletStatMgr(), Collections.singletonList(10L));
            Assertions.assertEquals(9L, changelog.getRowCount());
            Assertions.assertTrue(changelog.getRowCountReported());
        }
    }

    @Test
    public void testCloudReplicaRecoveryDoesNotManufactureReportCompleteness() {
        CloudReplica replica = cloudReplica(200L);
        replica.setRowCount(8L);
        replica.setRowCountReported(true);
        CloudReplica restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(replica), CloudReplica.class);
        Assertions.assertEquals(8L, restored.getRowCount());
        Assertions.assertFalse(restored.isRowCountReported());
    }

    @Test
    public void testCloudModeTwoRevalidatesRestoredRowsBeforePollingBackoff() throws Exception {
        InternalCatalog catalog = new InternalCatalog();
        MaterializedIndex changelog = createTable(catalog);
        OlapTable table = (OlapTable) catalog.getDbNullable(10L).getTableNullable(20L);
        MaterializedIndex base = table.getPartition(30L).getBaseIndex();
        CloudReplica baseReplica = new CloudReplica(100L, -1L, Replica.ReplicaState.NORMAL,
                2L, 0, 10L, 20L, 30L, 40L, -1L);
        CloudReplica changelogReplica = cloudReplica(200L);
        baseReplica.setRowCount(8L);
        changelogReplica.setRowCount(13L);
        baseReplica.setStatsIntervalIndex(8); // Stable tablets can persist an infinite interval.
        changelogReplica.setStatsIntervalIndex(7); // Also cover an unexpired finite interval.
        for (CloudReplica replica : Arrays.asList(baseReplica, changelogReplica)) {
            replica.setLastGetTabletStatsTime(System.currentTimeMillis());
            replica.setRowCountReported(true);
        }
        CloudReplica restoredBase = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(baseReplica), CloudReplica.class);
        CloudReplica restoredChangelog = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(changelogReplica), CloudReplica.class);
        CloudTablet baseTablet = new CloudTablet(100L);
        baseTablet.addReplica(restoredBase, true);
        base.addTablet(baseTablet, null, true);
        CloudTablet changelogTablet = new CloudTablet(200L);
        changelogTablet.addReplica(restoredChangelog, true);
        changelog.addTablet(changelogTablet, null, true);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        Mockito.when(invertedIndex.getReplicas(100L)).thenReturn(Collections.singletonList(restoredBase));
        Mockito.when(invertedIndex.getReplicas(200L)).thenReturn(Collections.singletonList(restoredChangelog));
        Mockito.when(invertedIndex.getReplicasByTabletId(100L)).thenReturn(Collections.singletonList(restoredBase));
        Mockito.when(invertedIndex.getReplicasByTabletId(200L)).thenReturn(Collections.singletonList(restoredChangelog));
        List<GetTabletStatsRequest> requests = new ArrayList<>();
        Method update = CloudTabletStatMgr.class.getDeclaredMethod("updateTabletStat",
                GetTabletStatsResponse.class, boolean.class);
        update.setAccessible(true);
        int oldStatsVersion = Config.cloud_get_tablet_stats_version;
        Config.cloud_get_tablet_stats_version = 2;
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<FrontendOptions> frontend = Mockito.mockStatic(FrontendOptions.class)) {
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
            frontend.when(FrontendOptions::getLocalHostAddressCached).thenReturn("127.0.0.1");
            CloudTabletStatMgr manager = Mockito.spy(new CloudTabletStatMgr());
            // Replace only transport/executor submission. Exercise the actual mode-2 selection,
            // request construction, successful report handling and index aggregation.
            Mockito.doAnswer(invocation -> {
                GetTabletStatsRequest request = invocation.getArgument(0);
                requests.add(request);
                if (requests.size() > 1) {
                    update.invoke(manager, GetTabletStatsResponse.newBuilder()
                            .setStatus(MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK))
                            .addAllTabletStats(cloudRows(100L, 8L).getTabletStatsList())
                            .addAllTabletStats(cloudRows(200L, 13L).getTabletStatsList()).build(), false);
                }
                return CompletableFuture.completedFuture(null);
            }).when(manager).submitGetTabletStatsTask(Mockito.any(), Mockito.anyBoolean());
            // A failed first fetch leaves both replicas unknown; the next cycle must retry them.
            manager.runAfterCatalogReady();
            Assertions.assertEquals(1, requests.size());
            Assertions.assertEquals(2, requests.get(0).getTabletIdxCount());
            Assertions.assertFalse(restoredBase.isRowCountReported());
            Assertions.assertFalse(restoredChangelog.isRowCountReported());
            Assertions.assertFalse(base.getRowCountReported());
            Assertions.assertFalse(changelog.getRowCountReported());
            manager.runAfterCatalogReady();
            Assertions.assertEquals(2, requests.size());
            Assertions.assertEquals(2, requests.get(1).getTabletIdxCount());
            Assertions.assertTrue(base.getRowCountReported());
            Assertions.assertTrue(changelog.getRowCountReported());
            Assertions.assertEquals(8L, table.getRowCountForIndex(40L, true));
            Assertions.assertEquals(13L, table.getRowCountForPartitionIndex(30L, 50L, true));
            // Valid, unchanged replicas resume their persisted backoff instead of polling every cycle.
            manager.runAfterCatalogReady();
            Assertions.assertEquals(2, requests.size());
        } finally {
            Config.cloud_get_tablet_stats_version = oldStatsVersion;
        }
    }

    private MaterializedIndex createTable(InternalCatalog catalog) {
        Database db = new Database(10L, "changelog_periodic_stats");
        catalog.unprotectCreateDb(db);
        OlapTable table = new OlapTable(20L, "changelog_rows", Collections.emptyList(), KeysType.DUP_KEYS,
                Mockito.mock(PartitionInfo.class), null);
        table.setBaseIndexId(40L);
        MaterializedIndex base = new MaterializedIndex(40L, MaterializedIndex.IndexState.NORMAL);
        MaterializedIndex changelog = new MaterializedIndex(50L, MaterializedIndex.IndexState.NORMAL);
        changelog.setIsChangelog(true);
        Partition partition = new Partition(30L, "p1", base, null);
        partition.setVisibleVersionAndTime(2L, 100L);
        partition.createRollupIndex(changelog);
        table.addPartition(partition);
        db.registerTable(table);
        return changelog;
    }

    private LocalReplica localReplica(long id, long rows, boolean reported) {
        LocalReplica replica = new LocalReplica(id, id, Replica.ReplicaState.NORMAL, 2L, 0);
        replica.setRowCount(rows);
        if (reported) {
            replica.setLastReportVersion(2L);
        }
        return replica;
    }

    private CloudReplica cloudReplica(long id) {
        return new CloudReplica(id, -1L, Replica.ReplicaState.NORMAL, 2L, 0, 10L, 20L, 30L, 50L, -1L);
    }

    private GetTabletStatsResponse cloudRows(long tabletId, long rows) {
        return GetTabletStatsResponse.newBuilder().addTabletStats(TabletStatsPB.newBuilder()
                .setIdx(TabletIndexPB.newBuilder().setTabletId(tabletId)).setNumRows(rows)).build();
    }
}
