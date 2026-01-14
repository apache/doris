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

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class CatalogRecycleBinTest {

    private static String runningDir;
    private static Env env;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        runningDir = "fe/mocked/CatalogRecycleBinTest/" + UUID.randomUUID() + "/";
        UtFrameUtils.createDorisCluster(runningDir);
    }

    @Before
    public void setUp() throws Exception {
        env = CatalogTestUtil.createTestCatalog();
        Env.getCurrentRecycleBin().clearAll();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test(expected = IllegalStateException.class)
    public void testRecycleNotEmptyDatabase() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Set<String> tableNames = Sets.newHashSet(CatalogTestUtil.testTable1);
        Set<Long> tableIds = Sets.newHashSet(CatalogTestUtil.testTableId1);

        recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);
        Assert.fail("recycle no empty database should fail");
    }

    @Test
    public void testRecycleEmptyDatabase() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database emptyDb1 = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);

        Set<String> emptyTableNames = Sets.newHashSet();
        Set<Long> emptyTableIds = Sets.newHashSet();

        Assert.assertTrue(recycleBin.recycleDatabase(emptyDb1, emptyTableNames, emptyTableIds, false, false, 0));
        Assert.assertTrue(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
    }

    @Test
    public void testRecycleSameNameDatabase() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        // keep the newest one in recycle bin
        Config.max_same_name_catalog_trash_num = 1;

        Database emptyDb1 = new Database(1001, CatalogTestUtil.testDb1);
        Database emptyDb2 = new Database(1002, CatalogTestUtil.testDb1);
        Database emptyDb3 = new Database(1003, CatalogTestUtil.testDb1);

        Set<String> emptyTableNames = Sets.newHashSet();
        Set<Long> emptyTableIds = Sets.newHashSet();

        Assert.assertTrue(recycleBin.recycleDatabase(emptyDb1, emptyTableNames, emptyTableIds, false, false, 0));
        Assert.assertTrue(recycleBin.recycleDatabase(emptyDb2, emptyTableNames, emptyTableIds, false, false, 0));
        Assert.assertTrue(recycleBin.isRecycleDatabase(1001));
        Assert.assertTrue(recycleBin.isRecycleDatabase(1002));

        // sleep 0.1 second to make sure the recycle time is different
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(recycleBin.recycleDatabase(emptyDb3, emptyTableNames, emptyTableIds, false, false, 0));
        Assert.assertTrue(recycleBin.isRecycleDatabase(1003));

        recycleBin.runAfterCatalogReady();

        // verify that only newest one is left in recycle bin
        Assert.assertFalse(recycleBin.isRecycleDatabase(1001));
        Assert.assertFalse(recycleBin.isRecycleDatabase(1002));
        Assert.assertTrue(recycleBin.isRecycleDatabase(1003));
    }

    @Test
    public void testForceDropDatabase() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database emptyDb = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);

        Set<String> tableNames = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();

        Assert.assertTrue(recycleBin.recycleDatabase(emptyDb, tableNames, tableIds, false, true, 0));
        Assert.assertTrue(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));

        Long recycleTime = recycleBin.getRecycleTimeById(CatalogTestUtil.testDbId1);
        Assert.assertNotNull(recycleTime);
        Assert.assertEquals(0L, recycleTime.longValue());

        recycleBin.runAfterCatalogReady();
        // verify that the db has been immediately dropped from recycle bin
        Assert.assertFalse(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
        // verify recycle time is no longer present
        Assert.assertNull(recycleBin.getRecycleTimeById(CatalogTestUtil.testDbId1));
    }

    @Test
    public void testRecycleTable() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        Assert.assertTrue(recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0));
        Assert.assertTrue(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));

        // test recycling same table again should fail
        Assert.assertFalse(recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0));
    }

    @Test
    public void testForceDropTable() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        Assert.assertTrue(recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, true, 0));

        Long recycleTime = recycleBin.getRecycleTimeById(CatalogTestUtil.testTableId1);
        Assert.assertNotNull(recycleTime);
        Assert.assertEquals(0L, recycleTime.longValue());
        Assert.assertTrue(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));

        recycleBin.runAfterCatalogReady();
        // verify that the table has been immediately dropped from recycle bin
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));
        // verify recycle time is no longer present
        Assert.assertNull(recycleBin.getRecycleTimeById(CatalogTestUtil.testTableId1));
    }

    @Test
    public void testRecyclePartition() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        Partition partition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);

        boolean result = recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
            );
        Assert.assertTrue(result);
        Assert.assertTrue(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartitionId1));

        result = recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
            );
        // test recycling same partition again should fail
        Assert.assertFalse(result);
    }

    @Test
    public void testRecycleSameNamePartition() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        // keep the newest one in recycle bin
        Config.max_same_name_catalog_trash_num = 1;

        int recyclePartitionNum = 1;
        do {
            MaterializedIndex index = new MaterializedIndex(1005, IndexState.NORMAL);
            RandomDistributionInfo distributionInfo = new RandomDistributionInfo(1);
            Partition partition = new Partition(recyclePartitionNum, "same name", index, distributionInfo);
            boolean result = recycleBin.recyclePartition(
                    CatalogTestUtil.testDbId1,
                    CatalogTestUtil.testTableId1,
                    CatalogTestUtil.testTable1,
                    partition,
                    null,
                    null,
                    new DataProperty(TStorageMedium.HDD),
                    new ReplicaAllocation((short) 3),
                    false,
                    false
            );
            Assert.assertTrue(result);
            Assert.assertTrue(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, recyclePartitionNum));
        } while (recyclePartitionNum++ < 100);

        // sleep 0.1 second to make sure the recycle time is different
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        MaterializedIndex index = new MaterializedIndex(1005, IndexState.NORMAL);
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(1);
        Partition partition = new Partition(recyclePartitionNum, "same name", index, distributionInfo);
        boolean result = recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
            );
        Assert.assertTrue(result);
        Assert.assertTrue(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, recyclePartitionNum));

        recycleBin.runAfterCatalogReady();

        // verify that only newest one is left in recycle bin
        Set<Long> dbIds = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();
        recycleBin.getRecycleIds(dbIds, tableIds, partitionIds);

        Assert.assertEquals(0, dbIds.size());
        Assert.assertEquals(0, tableIds.size());
        Assert.assertEquals(1, partitionIds.size());
        Assert.assertTrue(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, recyclePartitionNum));
    }

    @Test
    public void testRecoverEmptyDatabase() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database emptyDb = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);

        Set<String> tableNames = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();

        recycleBin.recycleDatabase(emptyDb, tableNames, tableIds, false, false, 0);

        Database recoveredDb = recycleBin.recoverDatabase(CatalogTestUtil.testDb1, -1);
        Assert.assertNotNull(recoveredDb);
        Assert.assertEquals(CatalogTestUtil.testDbId1, recoveredDb.getId());
        Assert.assertFalse(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
    }

    @Test
    public void testRecoverDatabaseWithTable() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Set<String> tableNames = db.getTableNames();
        Set<Long> tableIds = Sets.newHashSet(db.getTableIds());

        recycleAllTables(db, recycleBin);
        recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);

        // test recovering database with table
        Database recoveredDb = recycleBin.recoverDatabase(CatalogTestUtil.testDb1, -1);
        Assert.assertNotNull(recoveredDb);
        Assert.assertEquals(CatalogTestUtil.testDbId1, recoveredDb.getId());
        Assert.assertFalse(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
        Assert.assertTrue(recoveredDb.getTable(CatalogTestUtil.testTableId1).isPresent());
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));
        Assert.assertTrue(recoveredDb.getTable(CatalogTestUtil.testTableId2).isPresent());
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId2));
        // non olap table should not be recovered
        Assert.assertFalse(recoveredDb.getTable(CatalogTestUtil.testEsTableId1).isPresent());
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testEsTableId1));
    }

    @Test
    public void testRecoverTable() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0);
        Assert.assertTrue(recycleBin.recoverTable(db, CatalogTestUtil.testTable1, -1, null));
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));
        Assert.assertNotNull(db.getTable(CatalogTestUtil.testTableId1));
    }

    @Test
    public void testRecoverPartition() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        Partition partition = olapTable.getPartition(CatalogTestUtil.testPartition1);

        recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
        );

        recycleBin.recoverPartition(CatalogTestUtil.testDbId1, olapTable, CatalogTestUtil.testPartition1, -1, null);
        Assert.assertFalse(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartitionId1));
        Assert.assertNotNull(olapTable.getPartition(CatalogTestUtil.testPartition1));
    }

    @Test
    public void testGetRecycleIds() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        Partition partition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);

        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0);

        recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
        );

        Set<Long> dbIds = Sets.newHashSet();
        Set<Long> tableIdsResult = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();

        recycleBin.getRecycleIds(dbIds, tableIdsResult, partitionIds);

        Assert.assertEquals(0, dbIds.size());
        Assert.assertTrue(tableIdsResult.contains(CatalogTestUtil.testTableId1));
        Assert.assertTrue(partitionIds.contains(CatalogTestUtil.testPartitionId1));
    }

    @Test
    public void testAllTabletsInRecycledStatus() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0);

        // get tablet ids from the table
        List<Long> recycleTabletIds = Lists.newArrayList();
        for (Partition partition : olapTable.getAllPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    recycleTabletIds.add(tablet.getId());
                }
            }
        }

        List<Long> nonRecycledTabletIds = Lists.newArrayList(999L, 1000L);
        Assert.assertTrue(recycleBin.allTabletsInRecycledStatus(recycleTabletIds));
        Assert.assertFalse(recycleBin.allTabletsInRecycledStatus(nonRecycledTabletIds));
    }

    @Test
    public void testEraseDatabaseInstantly() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database emptyDb = new Database(CatalogTestUtil.testDbId1, CatalogTestUtil.testDb1);

        Set<String> tableNames = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();

        recycleBin.recycleDatabase(emptyDb, tableNames, tableIds, false, false, 0);
        recycleBin.eraseDatabaseInstantly(CatalogTestUtil.testDbId1);
        Assert.assertFalse(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
    }

    @Test
    public void testEraseTableInstantly() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0);
        recycleBin.eraseTableInstantly(CatalogTestUtil.testTableId1);

        // verify table is no longer in recycle bin
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));
    }

    @Test
    public void testErasePartitionInstantly() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        Partition partition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);

        recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
        );

        recycleBin.erasePartitionInstantly(CatalogTestUtil.testPartitionId1);

        // verify partition is no longer in recycle bin
        Assert.assertFalse(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartitionId1));
    }

    @Test
    public void testReplayOperations() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Set<String> tableNames = Sets.newHashSet(db.getTableNames());
        Set<Long> tableIds = Sets.newHashSet(db.getTableIds());

        Optional<Table> table1 = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table1.isPresent());
        Assert.assertTrue(table1.get() instanceof OlapTable);
        OlapTable olapTable1 = (OlapTable) table1.get();

        Partition partition = olapTable1.getPartition(CatalogTestUtil.testPartitionId1);
        recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
        );

        recycleAllTables(db, recycleBin);
        recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);

        recycleBin.replayEraseDatabase(CatalogTestUtil.testDbId1);
        recycleBin.replayEraseTable(CatalogTestUtil.testTableId1);
        recycleBin.replayEraseTable(CatalogTestUtil.testTableId2);
        recycleBin.replayEraseTable(CatalogTestUtil.testEsTableId1);
        recycleBin.replayErasePartition(CatalogTestUtil.testPartitionId1);

        // verify objects are no longer in recycle bin
        Assert.assertFalse(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId2));
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testEsTableId1));
        Assert.assertFalse(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartitionId1));
    }

    @Test
    public void testGetInfo() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Set<String> tableNames = Sets.newHashSet(db.getTableNames());
        Set<Long> tableIds = Sets.newHashSet(db.getTableIds());

        Optional<Table> table1 = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table1.isPresent());
        Assert.assertTrue(table1.get() instanceof OlapTable);
        OlapTable olapTable1 = (OlapTable) table1.get();

        Partition partition = olapTable1.getPartition(CatalogTestUtil.testPartitionId1);
        recycleBin.recyclePartition(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testTable1,
                partition,
                null,
                null,
                new DataProperty(TStorageMedium.HDD),
                new ReplicaAllocation((short) 3),
                false,
                false
        );

        recycleAllTables(db, recycleBin);
        recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);

        List<List<String>> info = recycleBin.getInfo();
        Assert.assertNotNull(info);
        Assert.assertFalse(info.isEmpty());

        // verify info contains database information
        Set<String> itemTypes = info.stream().map(item -> item.get(0)).collect(Collectors.toSet());
        Assert.assertTrue(itemTypes.contains("Database"));
        Assert.assertTrue(itemTypes.contains("Table"));
        Assert.assertTrue(itemTypes.contains("Partition"));
    }

    @Test
    public void testGetDbToRecycleSize() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Set<String> tableNames = Sets.newHashSet(db.getTableNames());
        Set<Long> tableIds = Sets.newHashSet(db.getTableIds());

        recycleAllTables(db, recycleBin);
        recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);

        Map<Long, Pair<Long, Long>> sizeMap = recycleBin.getDbToRecycleSize();
        Assert.assertNotNull(sizeMap);
        Assert.assertTrue(sizeMap.containsKey(CatalogTestUtil.testDbId1));

        Pair<Long, Long> sizes = sizeMap.get(CatalogTestUtil.testDbId1);
        Assert.assertNotNull(sizes);
        Assert.assertTrue(sizes.first >= 0);
        Assert.assertTrue(sizes.second >= 0);
    }

    @Test(expected = DdlException.class)
    public void testRecoverNonExistentDatabase() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        // try to recover a non-existent database
        recycleBin.recoverDatabase("non_existent_db", -1);
    }

    @Test(expected = DdlException.class)
    public void testRecoverNonExistentTable() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
        );
        // try to recover a non-existent table
        recycleBin.recoverTable(db, "non_existent_table", -1, null);
    }

    @Test(expected = DdlException.class)
    public void testRecoverNonExistentPartition() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        // try to recover a non-existent partition
        recycleBin.recoverPartition(CatalogTestUtil.testDbId1, olapTable, "non_existent_partition", -1, null);
    }

    @Test
    public void testAddTabletToInvertedIndex() {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        Database db = CatalogTestUtil.createSimpleDb(
                CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartitionId1,
                CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion
            );

        Optional<Table> table = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table.isPresent());
        Assert.assertTrue(table.get() instanceof OlapTable);

        OlapTable olapTable = (OlapTable) table.get();
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable, false, false, 0);

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        invertedIndex.clear();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(CatalogTestUtil.testTabletId1);
        Assert.assertNull(tabletMeta);

        recycleBin.addTabletToInvertedIndex();

        // verify tablets are added to inverted index
        tabletMeta = invertedIndex.getTabletMeta(CatalogTestUtil.testTabletId1);
        Assert.assertNotNull(tabletMeta);
    }

    public void recycleAllTables(Database db, CatalogRecycleBin recycleBin) {
        Optional<Table> table1 = db.getTable(CatalogTestUtil.testTableId1);
        Assert.assertTrue(table1.isPresent());
        Assert.assertTrue(table1.get() instanceof OlapTable);
        OlapTable olapTable1 = (OlapTable) table1.get();

        Optional<Table> table2 = db.getTable(CatalogTestUtil.testTableId2);
        Assert.assertTrue(table2.isPresent());
        Assert.assertTrue(table2.get() instanceof OlapTable);
        OlapTable olapTable2 = (OlapTable) table2.get();

        Optional<Table> table3 = db.getTable(CatalogTestUtil.testEsTableId1);
        Assert.assertTrue(table3.isPresent());
        Assert.assertTrue(table3.get() instanceof EsTable);
        EsTable esTable = (EsTable) table3.get();

        db.unregisterTable(CatalogTestUtil.testTableId1);
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable1, false, false, 0);

        db.unregisterTable(CatalogTestUtil.testTableId2);
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable2, false, false, 0);

        db.unregisterTable(CatalogTestUtil.testEsTableId1);
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, esTable, false, false, 0);
    }
}

