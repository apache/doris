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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.common.Config;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.stream.Collectors;

public class TabletHealthProcDirTest extends TestWithFeService {
    private Database db;

    @Override
    protected int backendNum() {
        return 2;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        for (Table table : db.getTables()) {
            dropTable(table.getName(), true);
        }
    }

    @Test
    public void testCloudModeCountsTabletAsHealthy() throws Exception {
        String originDeployMode = Config.deploy_mode;
        createTable("CREATE TABLE tbl_proc_health (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '1')");

        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl_proc_health");
        Partition partition = table.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        partition.updateVisibleVersion(10L);
        Replica replica = tablet.getReplicas().get(0);
        replica.updateVersion(10L);
        replica.setBad(true);

        TabletStatus localStatus = tablet.getHealth(Env.getCurrentSystemInfo(), partition.getVisibleVersion(),
                table.getPartitionInfo().getReplicaAllocation(partition.getId()),
                Env.getCurrentSystemInfo().getAllBackendIds(true)).status;
        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, localStatus);

        Config.deploy_mode = "cloud";
        try (MockedStatic<Partition> mockedPartition = Mockito.mockStatic(Partition.class, Mockito.CALLS_REAL_METHODS)) {
            mockedPartition.when(() -> Partition.getVisibleVersions(Mockito.anyList())).thenAnswer(invocation -> {
                List<? extends Partition> partitions = invocation.getArgument(0);
                return partitions.stream().map(Partition::getVisibleVersion).collect(Collectors.toList());
            });
            TabletHealthProcDir.DBTabletStatistic statistic = new TabletHealthProcDir.DBTabletStatistic(db);
            List<String> row = statistic.toRow();
            int tabletNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("TabletNum");
            int healthyNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("HealthyNum");
            int unrecoverableNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("UnrecoverableNum");
            int rowBinlogMismatchNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("RowBinlogMismatchNum");
            int rowBinlogRedundantNumIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("RowBinlogRedundantNum");

            Assertions.assertEquals(TabletHealthProcDir.TITLE_NAMES.size(), row.size());
            Assertions.assertEquals("1", row.get(tabletNumIdx));
            Assertions.assertEquals("1", row.get(healthyNumIdx));
            Assertions.assertEquals("0", row.get(unrecoverableNumIdx));
            Assertions.assertEquals("0", row.get(rowBinlogMismatchNumIdx));
            Assertions.assertEquals("0", row.get(rowBinlogRedundantNumIdx));
        } finally {
            Config.deploy_mode = originDeployMode;
        }
    }

    @Test
    public void testRowBinlogBackendMismatchIsNotCountedAsColocateMismatch() throws Exception {
        RowBinlogTabletPair pair = createRowBinlogTabletPair("tbl_proc_backend_mismatch");
        Replica rowBinlogReplica = pair.rowBinlogTablet.getReplicas().get(0);
        long originalBackendId = rowBinlogReplica.getBackendIdWithoutException();
        long otherBackendId = Env.getCurrentSystemInfo().getAllBackendIds(true).stream()
                .filter(backendId -> backendId != originalBackendId)
                .findFirst()
                .orElseThrow();
        rowBinlogReplica.setBackendId(otherBackendId);
        try {
            assertRowBinlogProcClassification(pair.rowBinlogTablet.getId(), 1, 0);
        } finally {
            rowBinlogReplica.setBackendId(originalBackendId);
        }
    }

    @Test
    public void testRowBinlogPathMismatchIsNotCountedAsColocateMismatch() throws Exception {
        RowBinlogTabletPair pair = createRowBinlogTabletPair("tbl_proc_path_mismatch");
        Replica baseReplica = pair.baseTablet.getReplicas().get(0);
        Replica rowBinlogReplica = pair.rowBinlogTablet.getReplicas().get(0);
        long originalBasePathHash = baseReplica.getPathHash();
        long originalRowBinlogPathHash = rowBinlogReplica.getPathHash();
        baseReplica.setPathHash(70001L);
        rowBinlogReplica.setPathHash(70002L);
        try {
            assertRowBinlogProcClassification(pair.rowBinlogTablet.getId(), 1, 0);
        } finally {
            baseReplica.setPathHash(originalBasePathHash);
            rowBinlogReplica.setPathHash(originalRowBinlogPathHash);
        }
    }

    @Test
    public void testRowBinlogRedundantIsNotCountedAsColocateRedundant() throws Exception {
        RowBinlogTabletPair pair = createRowBinlogTabletPair("tbl_proc_redundant");
        Replica existingReplica = pair.rowBinlogTablet.getReplicas().get(0);
        long otherBackendId = Env.getCurrentSystemInfo().getAllBackendIds(true).stream()
                .filter(backendId -> backendId != existingReplica.getBackendIdWithoutException())
                .findFirst()
                .orElseThrow();
        Replica redundantReplica = new LocalReplica(pair.rowBinlogTablet.getId() + 1000L, otherBackendId,
                ReplicaState.NORMAL, pair.partition.getVisibleVersion(), 0);
        redundantReplica.setPathHash(70002L);
        pair.rowBinlogTablet.addReplica(redundantReplica, false);

        assertRowBinlogProcClassification(pair.rowBinlogTablet.getId(), 0, 1);
    }

    private RowBinlogTabletPair createRowBinlogTabletPair(String tableName) throws Exception {
        createTable("CREATE TABLE " + tableName + " (k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'binlog.enable'='true','binlog.format'='ROW')");
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableName);
        Partition partition = table.getPartitions().iterator().next();
        MaterializedIndex rowBinlogIndex = partition.getMaterializedIndices(IndexExtState.ALL, true).stream()
                .filter(MaterializedIndex::isRowBinlog)
                .findFirst()
                .orElseThrow();
        Tablet baseTablet = partition.getBaseIndex().getTablets().get(0);
        Tablet rowBinlogTablet = rowBinlogIndex.getTablets().get(0);
        Assertions.assertEquals(baseTablet.getId(), rowBinlogTablet.getRowBinlogBaseTabletId());
        Assertions.assertEquals(rowBinlogTablet.getId(), baseTablet.getRowBinlogTabletId());
        return new RowBinlogTabletPair(partition, baseTablet, rowBinlogTablet);
    }

    private void assertRowBinlogProcClassification(long rowBinlogTabletId,
            int expectedMismatchNum, int expectedRedundantNum) throws Exception {
        ProcResult healthResult = new TabletHealthProcDir(Env.getCurrentEnv()).fetchResult();
        List<String> row = findDbRow(healthResult, db.getId());
        Assertions.assertEquals(TabletHealthProcDir.TITLE_NAMES.size(), row.size());
        Assertions.assertEquals(String.valueOf(expectedMismatchNum),
                row.get(TabletHealthProcDir.TITLE_NAMES.indexOf("RowBinlogMismatchNum")));
        Assertions.assertEquals(String.valueOf(expectedRedundantNum),
                row.get(TabletHealthProcDir.TITLE_NAMES.indexOf("RowBinlogRedundantNum")));
        Assertions.assertEquals("0", row.get(TabletHealthProcDir.TITLE_NAMES.indexOf("ColocateMismatchNum")));
        Assertions.assertEquals("0", row.get(TabletHealthProcDir.TITLE_NAMES.indexOf("ColocateRedundantNum")));

        ProcResult incompleteResult = new IncompleteTabletsProcNode(db).fetchResult();
        Assertions.assertEquals(IncompleteTabletsProcNode.TITLE_NAMES.size(),
                incompleteResult.getRows().get(0).size());
        Assertions.assertEquals(expectedMismatchNum == 0 ? "" : String.valueOf(rowBinlogTabletId),
                incompleteResult.getRows().get(0).get(
                        IncompleteTabletsProcNode.TITLE_NAMES.indexOf("RowBinlogMismatchTablets")));
        Assertions.assertEquals(expectedRedundantNum == 0 ? "" : String.valueOf(rowBinlogTabletId),
                incompleteResult.getRows().get(0).get(
                        IncompleteTabletsProcNode.TITLE_NAMES.indexOf("RowBinlogRedundantTablets")));
        Assertions.assertEquals("", incompleteResult.getRows().get(0).get(
                IncompleteTabletsProcNode.TITLE_NAMES.indexOf("ColocateMismatchTablets")));
        Assertions.assertEquals("", incompleteResult.getRows().get(0).get(
                IncompleteTabletsProcNode.TITLE_NAMES.indexOf("ColocateRedundantTablets")));
    }

    private static class RowBinlogTabletPair {
        private final Partition partition;
        private final Tablet baseTablet;
        private final Tablet rowBinlogTablet;

        private RowBinlogTabletPair(Partition partition, Tablet baseTablet, Tablet rowBinlogTablet) {
            this.partition = partition;
            this.baseTablet = baseTablet;
            this.rowBinlogTablet = rowBinlogTablet;
        }
    }

    private List<String> findDbRow(ProcResult result, long dbId) {
        int dbIdIdx = TabletHealthProcDir.TITLE_NAMES.indexOf("DbId");
        return result.getRows().stream()
                .filter(row -> String.valueOf(dbId).equals(row.get(dbIdIdx)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("can not find db row in tablet health proc result"));
    }
}
