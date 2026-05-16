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

import org.apache.doris.catalog.Function.BinaryType;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.URI;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
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
    public void testReadRecycleBinDatabaseDoesNotRegisterNereidsFunctions() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        String dbName = "recycle_db_with_python_udf";
        String functionName = "recycled_py_udf";
        Database db = new Database(10001, dbName);
        ScalarFunction function = ScalarFunction.createUdf(
                BinaryType.PYTHON_UDF,
                new FunctionName(dbName, functionName),
                new Type[] {Type.INT},
                Type.INT,
                false,
                URI.create("file:///tmp/recycled_py_udf.py"),
                "evaluate",
                null,
                null
        );
        function.setRuntimeVersion("3.8");
        function.setFunctionCode("def evaluate(x):\n    return x + 1\n");
        function.setNullableMode(NullableMode.ALWAYS_NULLABLE);
        function.setId(10002);
        db.replayAddFunction(function);
        Assert.assertTrue(hasUdfBuilder(dbName, functionName));

        Assert.assertTrue(recycleBin.recycleDatabase(db, Sets.newHashSet(), Sets.newHashSet(), false, false, 0));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        recycleBin.write(new DataOutputStream(outputStream));

        Env.getCurrentEnv().getFunctionRegistry().dropUdfByDb(dbName);
        Assert.assertFalse(hasUdfBuilder(dbName, functionName));

        CatalogRecycleBin.read(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
        Assert.assertFalse(hasUdfBuilder(dbName, functionName));
    }

    private boolean hasUdfBuilder(String dbName, String functionName) {
        Map<String, List<FunctionBuilder>> buildersByName =
                Env.getCurrentEnv().getFunctionRegistry().getName2UdfBuilders().get(dbName);
        return buildersByName != null && buildersByName.containsKey(functionName);
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

        recycleBin.replayErasePartition(CatalogTestUtil.testPartitionId1);

        // verify objects are no longer in recycle bin
        Assert.assertFalse(recycleBin.isRecycleDatabase(CatalogTestUtil.testDbId1));
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1));
        Assert.assertFalse(recycleBin.isRecycleTable(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId2));

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

        db.unregisterTable(CatalogTestUtil.testTableId1);
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable1, false, false, 0);

        db.unregisterTable(CatalogTestUtil.testTableId2);
        recycleBin.recycleTable(CatalogTestUtil.testDbId1, olapTable2, false, false, 0);
    }

    @Test
    public void testConcurrentReadsDoNotBlock() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        // Recycle several partitions
        for (int i = 1; i <= 50; i++) {
            MaterializedIndex index = new MaterializedIndex(2000 + i, IndexState.NORMAL);
            RandomDistributionInfo dist = new RandomDistributionInfo(1);
            Partition partition = new Partition(3000 + i, "part_" + i, index, dist);
            recycleBin.recyclePartition(
                    CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                    CatalogTestUtil.testTable1, partition, null, null,
                    new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3),
                    false, false);
        }

        // Multiple reader threads should run concurrently without blocking each other
        int numReaders = 10;
        CyclicBarrier barrier = new CyclicBarrier(numReaders);
        ExecutorService executor = Executors.newFixedThreadPool(numReaders);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < numReaders; i++) {
            futures.add(executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                // Perform various read operations concurrently
                for (int j = 1; j <= 50; j++) {
                    recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1,
                            CatalogTestUtil.testTableId1, 3000 + j);
                    recycleBin.getRecycleTimeById(3000 + j);
                }
                Set<Long> dbIds = Sets.newHashSet();
                Set<Long> tableIds = Sets.newHashSet();
                Set<Long> partIds = Sets.newHashSet();
                recycleBin.getRecycleIds(dbIds, tableIds, partIds);
                return true;
            }));
        }

        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        for (Future<Boolean> f : futures) {
            Assert.assertTrue(f.get());
        }
    }

    @Test
    public void testConcurrentRecycleAndRead() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        AtomicBoolean readerError = new AtomicBoolean(false);
        AtomicBoolean writerDone = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);

        // Writer thread: continuously recycles partitions
        Thread writer = new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 1; i <= 100; i++) {
                    MaterializedIndex index = new MaterializedIndex(4000 + i, IndexState.NORMAL);
                    RandomDistributionInfo dist = new RandomDistributionInfo(1);
                    Partition partition = new Partition(5000 + i, "cpart_" + i, index, dist);
                    recycleBin.recyclePartition(
                            CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                            CatalogTestUtil.testTable1, partition, null, null,
                            new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3),
                            false, false);
                }
            } catch (Exception e) {
                readerError.set(true);
            } finally {
                writerDone.set(true);
            }
        });

        // Reader threads: continuously read while writer is active
        List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < 5; r++) {
            Thread reader = new Thread(() -> {
                try {
                    startLatch.await();
                    while (!writerDone.get()) {
                        // These should never throw ConcurrentModificationException
                        Set<Long> dbIds = Sets.newHashSet();
                        Set<Long> tableIds = Sets.newHashSet();
                        Set<Long> partIds = Sets.newHashSet();
                        recycleBin.getRecycleIds(dbIds, tableIds, partIds);
                        recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1,
                                CatalogTestUtil.testTableId1, 5001);
                    }
                } catch (Exception e) {
                    readerError.set(true);
                }
            });
            readers.add(reader);
        }

        writer.start();
        readers.forEach(Thread::start);
        startLatch.countDown();

        writer.join(30_000);
        for (Thread reader : readers) {
            reader.join(30_000);
        }

        Assert.assertFalse("Reader or writer thread encountered an error", readerError.get());
        // Verify all 100 partitions were recycled
        for (int i = 1; i <= 100; i++) {
            Assert.assertTrue(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1,
                    CatalogTestUtil.testTableId1, 5000 + i));
        }
    }

    @Test
    public void testMicrobatchEraseReleasesLockBetweenItems() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();

        // Recycle many partitions
        int numPartitions = 50;
        for (int i = 1; i <= numPartitions; i++) {
            MaterializedIndex index = new MaterializedIndex(6000 + i, IndexState.NORMAL);
            RandomDistributionInfo dist = new RandomDistributionInfo(1);
            Partition partition = new Partition(7000 + i, "epart_" + i, index, dist);
            recycleBin.recyclePartition(
                    CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                    CatalogTestUtil.testTable1, partition, null, null,
                    new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3),
                    false, false);
        }

        // Verify all were recycled
        Set<Long> dbIds = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();
        recycleBin.getRecycleIds(dbIds, tableIds, partitionIds);
        Assert.assertEquals(numPartitions, partitionIds.size());

        // Now run erase daemon which should process items one at a time
        // While erase is running, a concurrent recyclePartition should be able to
        // proceed between items (not blocked for the entire erase duration)
        AtomicBoolean recycleCompleted = new AtomicBoolean(false);
        AtomicBoolean eraseStarted = new AtomicBoolean(false);

        Thread eraseThread = new Thread(() -> {
            eraseStarted.set(true);
            recycleBin.runAfterCatalogReady();
        });

        eraseThread.start();

        // Wait briefly for erase to start, then try to recycle a new partition
        Thread.sleep(50);
        if (eraseStarted.get()) {
            MaterializedIndex newIndex = new MaterializedIndex(8000, IndexState.NORMAL);
            RandomDistributionInfo newDist = new RandomDistributionInfo(1);
            Partition newPartition = new Partition(9000, "new_part", newIndex, newDist);
            recycleBin.recyclePartition(
                    CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                    CatalogTestUtil.testTable1, newPartition, null, null,
                    new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3),
                    false, false);
            recycleCompleted.set(true);
        }

        eraseThread.join(60_000);
        Assert.assertFalse("Erase thread should have finished", eraseThread.isAlive());

        // The new partition should have been recycled successfully
        if (eraseStarted.get()) {
            Assert.assertTrue("recyclePartition should succeed during erase",
                    recycleCompleted.get());
            Assert.assertTrue(recycleBin.isRecyclePartition(CatalogTestUtil.testDbId1,
                    CatalogTestUtil.testTableId1, 9000));
        }
    }

    private static class ControllableClock implements LongSupplier {
        private long currentTime;
        private final long initialTime;

        public ControllableClock(long initialTime) {
            this.currentTime = initialTime;
            this.initialTime = initialTime;
        }

        @Override
        public long getAsLong() {
            return currentTime;
        }

        public void advance(long millis) {
            this.currentTime += millis;
        }

        public void backoff(long millis) {
            this.currentTime -= millis;
        }

        public void reset() {
            this.currentTime = this.initialTime;
        }
    }

    /**
     * Test old code bug: Partition timeout causes table/db cleanup delay.
     * Partition: expired (recycled long ago)
     * Table/Db: NOT expired at startTime, but become expired after erasePartition takes time
     * Old code uses stale startTime, so table/db are NOT cleaned.
     */
    @Test
    public void testOldCodePartitionTimeoutCausesTableDbDelay() {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId = baseId + 2;
            final long testPartitionId = baseId + 3;
            final long testIndexId = baseId + 4;
            final long testTabletId = baseId + 5;

            long startTime = System.currentTimeMillis();
            long expireMs = Config.catalog_trash_expire_second * 1000L;  // 1000ms
            long slowOperationMs = expireMs + 100;  // 1100ms

            ControllableClock clock = new ControllableClock(startTime);

            class BuggyRecycleBin extends CatalogRecycleBin {
                BuggyRecycleBin() {
                    setClock(clock);
                }

                @Override
                public void runAfterCatalogReady() {
                    long sharedTime = clock.getAsLong();
                    int keepNum = Config.max_same_name_catalog_trash_num;
                    erasePartition(sharedTime, keepNum);
                    eraseTable(sharedTime, keepNum);
                    eraseDatabase(sharedTime, keepNum);
                }

                @Override
                protected void erasePartition(long currentTimeMs, int keepNum) {
                    clock.advance(slowOperationMs);
                    super.erasePartition(currentTimeMs, keepNum);
                }
            }

            BuggyRecycleBin recycleBin = new BuggyRecycleBin();

            Database db = createSimpleTestDatabase(
                    testDbId, testTableId, testPartitionId,
                    testIndexId, testTabletId,
                    CatalogTestUtil.testStartVersion);

            OlapTable table = (OlapTable) db.getTable(testTableId).get();
            Partition partition = table.getPartition(testPartitionId);

            Set<String> tableNames = Sets.newHashSet();
            Set<Long> tableIds = Sets.newHashSet();
            for (Table tbl : db.getTables()) {
                tableNames.add(tbl.getName());
                tableIds.add(tbl.getId());
            }

            // Partition: recycleTime = startTime - 1100 (expired at startTime)
            clock.backoff(expireMs + 100);
            recycleBin.recyclePartition(testDbId, testTableId, table.getName(), partition, null, null,
                new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3), false, false);
            clock.reset();

            // Table: recycleTime = startTime - 200 (NOT expired at startTime)
            clock.backoff(200);
            for (Table tbl : db.getTables()) {
                db.unregisterTable(tbl.getId());
                recycleBin.recycleTable(testDbId, tbl, false, false, 0);
            }
            clock.reset();

            // Db: recycleTime = startTime - 100 (NOT expired at startTime)
            clock.backoff(100);
            recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);
            clock.reset();

            // Run cleanup
            // Timeline:
            // 1. erasePartition starts: sharedTime = startTime
            //    Partition: expired (latency = 1100 > 1000) -> cleaned
            //    During erasePartition, clock advances to startTime+1100
            // 2. eraseTable uses sharedTime = startTime (stale!)
            //    Table recycleTime = startTime-200, latency = 200 < 1000 -> NOT expired (bug!)
            // 3. eraseDatabase similarly NOT cleaned
            recycleBin.runAfterCatalogReady();

            // Verify bug: only partition cleaned, table and db remain
            Assert.assertNull(recycleBin.getRecycleTimeById(testPartitionId));
            Assert.assertNotNull(recycleBin.getRecycleTimeById(testTableId));
            Assert.assertNotNull(recycleBin.getRecycleTimeById(testDbId));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
        }
    }

    /**
     * Test old code bug: Partition and table timeout cause db cleanup delay.
     * Partition/Table: expired at startTime
     * Db: NOT expired at startTime, but become expired after erasePartition+eraseTable take time
     * Old code uses stale startTime, so db is NOT cleaned.
     */
    @Test
    public void testOldCodePartitionAndTableTimeoutCauseDbDelay() {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId = baseId + 2;
            final long testPartitionId = baseId + 3;
            final long testIndexId = baseId + 4;
            final long testTabletId = baseId + 5;

            long startTime = System.currentTimeMillis();
            long expireMs = Config.catalog_trash_expire_second * 1000L;  // 1000ms
            long slowOperationMs = expireMs + 100;  // 1100ms

            ControllableClock clock = new ControllableClock(startTime);

            class BuggyRecycleBin extends CatalogRecycleBin {
                BuggyRecycleBin() {
                    setClock(clock);
                }

                @Override
                public void runAfterCatalogReady() {
                    long sharedTime = clock.getAsLong();
                    int keepNum = Config.max_same_name_catalog_trash_num;
                    erasePartition(sharedTime, keepNum);
                    eraseTable(sharedTime, keepNum);
                    eraseDatabase(sharedTime, keepNum);
                }

                @Override
                protected void erasePartition(long currentTimeMs, int keepNum) {
                    clock.advance(slowOperationMs / 2);
                    super.erasePartition(currentTimeMs, keepNum);
                }

                @Override
                protected void eraseTable(long currentTimeMs, int keepNum) {
                    clock.advance(slowOperationMs / 2);
                    super.eraseTable(currentTimeMs, keepNum);
                }
            }

            BuggyRecycleBin recycleBin = new BuggyRecycleBin();

            Database db = createSimpleTestDatabase(
                    testDbId, testTableId, testPartitionId,
                    testIndexId, testTabletId,
                    CatalogTestUtil.testStartVersion);

            OlapTable table = (OlapTable) db.getTable(testTableId).get();
            Partition partition = table.getPartition(testPartitionId);

            Set<String> tableNames = Sets.newHashSet();
            Set<Long> tableIds = Sets.newHashSet();
            for (Table tbl : db.getTables()) {
                tableNames.add(tbl.getName());
                tableIds.add(tbl.getId());
            }

            // Partition: recycleTime = startTime - 1100 (expired at startTime)
            clock.backoff(expireMs + 100);
            recycleBin.recyclePartition(testDbId, testTableId, table.getName(), partition, null, null,
                new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3), false, false);
            clock.reset();

            // Table: recycleTime = startTime - 1050 (expired at startTime)
            clock.backoff(expireMs + 50);
            for (Table tbl : db.getTables()) {
                db.unregisterTable(tbl.getId());
                recycleBin.recycleTable(testDbId, tbl, false, false, 0);
            }
            clock.reset();

            // Db: recycleTime = startTime - 200 (NOT expired at startTime)
            clock.backoff(200);
            recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);
            clock.reset();

            // Run cleanup
            // Timeline:
            // 1. erasePartition: sharedTime=startTime, clock advances to startTime+550
            // 2. eraseTable: sharedTime=startTime (stale!), clock advances to startTime+1100
            // 3. eraseDatabase: sharedTime=startTime (stale!)
            //    Db recycleTime=startTime-200, latency=200<1000 -> NOT cleaned (bug!)
            recycleBin.runAfterCatalogReady();

            // Verify bug: partition and table cleaned, db remains
            Assert.assertNull(recycleBin.getRecycleTimeById(testPartitionId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testTableId));
            Assert.assertNotNull(recycleBin.getRecycleTimeById(testDbId));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
        }
    }

    /**
     * Test new code fix: All objects (partition, table, db) are expired.
     * Even with erasePartition taking long time, new code correctly cleans all expired objects.
     * Expected: All objects are cleaned up (null).
     */
    @Test
    public void testNewCodePartitionTimeoutTableDbWork() {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId = baseId + 2;
            final long testPartitionId = baseId + 3;
            final long testIndexId = baseId + 4;
            final long testTabletId = baseId + 5;

            long currentTimeMillis = System.currentTimeMillis();
            long expireMs = Config.catalog_trash_expire_second * 1000L;
            long offset = expireMs + 100;
            long advanceOffset = expireMs + 100;

            ControllableClock clock = new ControllableClock(currentTimeMillis);

            // Fixed: independent timestamps, partition advances to simulate processing delay
            class FixedRecycleBin extends CatalogRecycleBin {
                FixedRecycleBin() {
                    setClock(clock);
                }

                @Override
                protected void erasePartition(long currentTimeMs, int keepNum) {
                    clock.advance(advanceOffset);
                    super.erasePartition(currentTimeMs, keepNum);
                }
            }

            FixedRecycleBin recycleBin = new FixedRecycleBin();

            Database db = createSimpleTestDatabase(
                    testDbId, testTableId, testPartitionId,
                    testIndexId, testTabletId,
                    CatalogTestUtil.testStartVersion);

            OlapTable table = (OlapTable) db.getTable(testTableId).get();
            Partition partition = table.getPartition(testPartitionId);

            Set<String> tableNames = Sets.newHashSet();
            Set<Long> tableIds = Sets.newHashSet();
            for (Table tbl : db.getTables()) {
                tableNames.add(tbl.getName());
                tableIds.add(tbl.getId());
            }

            // Partition dropped first: recycleTime = currentTimeMillis - offset
            clock.backoff(offset);
            recycleBin.recyclePartition(testDbId, testTableId, table.getName(), partition, null, null,
                new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3), false, false);

            // Table and db dropped later: recycleTime = currentTimeMillis
            clock.advance(offset);
            for (Table tbl : db.getTables()) {
                db.unregisterTable(tbl.getId());
                recycleBin.recycleTable(testDbId, tbl, false, false, 0);
            }
            recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);

            // erasePartition gets currentTimeMillis, then advances clock
            // eraseTable gets currentTimeMillis + advanceOffset
            // eraseDatabase gets currentTimeMillis + advanceOffset
            recycleBin.runAfterCatalogReady();

            Assert.assertNull(recycleBin.getRecycleTimeById(testPartitionId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testTableId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testDbId));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
        }
    }

    /**
     * Test new code fix: All objects (partition, table, db) are expired.
     * Even with erasePartition and eraseTable taking long time, new code correctly cleans all expired objects.
     * Expected: All objects are cleaned up (null).
     */
    @Test
    public void testNewCodePartitionAndTableTimeoutDbWork() {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId = baseId + 2;
            final long testPartitionId = baseId + 3;
            final long testIndexId = baseId + 4;
            final long testTabletId = baseId + 5;

            long currentTimeMillis = System.currentTimeMillis();
            long expireMs = Config.catalog_trash_expire_second * 1000L;
            long offset = expireMs + 100;
            long advanceOffset = expireMs + 100;

            ControllableClock clock = new ControllableClock(currentTimeMillis);

            // Fixed: independent timestamps, both partition and table advance
            class FixedRecycleBin extends CatalogRecycleBin {
                FixedRecycleBin() {
                    setClock(clock);
                }

                @Override
                protected void erasePartition(long currentTimeMs, int keepNum) {
                    clock.advance(advanceOffset / 2);
                    super.erasePartition(currentTimeMs, keepNum);
                }

                @Override
                protected void eraseTable(long currentTimeMs, int keepNum) {
                    clock.advance(advanceOffset / 2);
                    super.eraseTable(currentTimeMs, keepNum);
                }
            }

            FixedRecycleBin recycleBin = new FixedRecycleBin();

            Database db = createSimpleTestDatabase(
                    testDbId, testTableId, testPartitionId,
                    testIndexId, testTabletId,
                    CatalogTestUtil.testStartVersion);

            OlapTable table = (OlapTable) db.getTable(testTableId).get();
            Partition partition = table.getPartition(testPartitionId);

            Set<String> tableNames = Sets.newHashSet();
            Set<Long> tableIds = Sets.newHashSet();
            for (Table tbl : db.getTables()) {
                tableNames.add(tbl.getName());
                tableIds.add(tbl.getId());
            }

            // Partition and table dropped first: recycleTime = currentTimeMillis - offset
            clock.backoff(offset);
            recycleBin.recyclePartition(testDbId, testTableId, table.getName(), partition, null, null,
                new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3), false, false);
            for (Table tbl : db.getTables()) {
                db.unregisterTable(tbl.getId());
                recycleBin.recycleTable(testDbId, tbl, false, false, 0);
            }

            // Db dropped later: recycleTime = currentTimeMillis
            clock.advance(offset);
            recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);

            recycleBin.runAfterCatalogReady();

            Assert.assertNull(recycleBin.getRecycleTimeById(testPartitionId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testTableId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testDbId));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
        }
    }

    /**
     * Test orphan partition cleanup in eraseTable.
     *
     * Scenario:
     * - Partition is NOT expired when erasePartition collects expired IDs (latency = 200ms < 1000ms)
     * - Table is recycled after Partition (T_partition = startTime - 200, T_table = startTime - 100)
     * - erasePartition takes 1500ms (time advances after super call)
     * - When eraseTable runs, Table IS expired (latency = 1600ms > 1000ms)
     * - Partition is NOT in erasePartition's expiredIds, so it would be orphaned
     * - eraseTable should detect and clean the orphan Partition before cleaning Table
     *
     * Expected: Partition cleaned (as orphan), Table cleaned, Database cleaned
     */
    @Test
    public void testOrphanPartitionCleanup() {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId = baseId + 2;
            final long testPartitionId = baseId + 3;
            final long testIndexId = baseId + 4;
            final long testTabletId = baseId + 5;

            long startTime = System.currentTimeMillis();
            long expireMs = Config.catalog_trash_expire_second * 1000L;
            long slowOperationMs = expireMs + 500;

            ControllableClock clock = new ControllableClock(startTime);

            class FixedRecycleBin extends CatalogRecycleBin {
                FixedRecycleBin() {
                    setClock(clock);
                }

                @Override
                protected void erasePartition(long currentTimeMs, int keepNum) {
                    super.erasePartition(currentTimeMs, keepNum);
                    clock.advance(slowOperationMs);
                }
            }

            FixedRecycleBin recycleBin = new FixedRecycleBin();

            Database db = createSimpleTestDatabase(
                    testDbId, testTableId, testPartitionId,
                    testIndexId, testTabletId,
                    CatalogTestUtil.testStartVersion);

            OlapTable table = (OlapTable) db.getTable(testTableId).get();
            Partition partition = table.getPartition(testPartitionId);

            Set<String> tableNames = Sets.newHashSet();
            Set<Long> tableIds = Sets.newHashSet();
            for (Table tbl : db.getTables()) {
                tableNames.add(tbl.getName());
                tableIds.add(tbl.getId());
            }

            // Partition: recycleTime = startTime - 200 (NOT expired at startTime: 200 < 1000)
            clock.backoff(200);
            recycleBin.recyclePartition(testDbId, testTableId, table.getName(), partition, null, null,
                new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3), false, false);
            clock.reset();

            // Table: recycleTime = startTime - 100 (NOT expired at startTime: 100 < 1000)
            clock.backoff(100);
            for (Table tbl : db.getTables()) {
                db.unregisterTable(tbl.getId());
                recycleBin.recycleTable(testDbId, tbl, false, false, 0);
            }
            clock.reset();

            // Database: recycleTime = startTime (NOT expired at startTime)
            recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, 0);
            clock.reset();

            recycleBin.runAfterCatalogReady();

            Assert.assertNull(recycleBin.getRecycleTimeById(testPartitionId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testTableId));
            Assert.assertNull(recycleBin.getRecycleTimeById(testDbId));

            Assert.assertFalse(recycleBin.isRecyclePartition(testDbId, testTableId, testPartitionId));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
        }
    }

    /**
     * Test that eraseAllTables cleans orphan partitions before erasing tables.
     *
     * Scenario:
     * - A database has a table and a partition
     * - The partition is NOT expired (still in idToPartition)
     * - eraseAllTables is called to erase the table
     * - Without fix: partition becomes orphan (still in idToPartition)
     * - With fix: partition is cleaned by cleanOrphanPartitions
     *
     * Expected: Both table and partition are cleaned
     */
    @Test
    public void testEraseAllTablesCleansOrphanPartitions() throws Exception {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId = baseId + 2;
            final long testPartitionId = baseId + 3;
            final long testIndexId = baseId + 4;
            final long testTabletId = baseId + 5;

            long startTime = System.currentTimeMillis();

            CatalogRecycleBin recycleBin = new CatalogRecycleBin();

            Database db = createSimpleTestDatabase(
                    testDbId, testTableId, testPartitionId,
                    testIndexId, testTabletId,
                    CatalogTestUtil.testStartVersion);

            OlapTable table = (OlapTable) db.getTable(testTableId).get();
            Partition partition = table.getPartition(testPartitionId);

            Set<String> tableNames = Sets.newHashSet();
            Set<Long> tableIds = Sets.newHashSet();
            for (Table tbl : db.getTables()) {
                tableNames.add(tbl.getName());
                tableIds.add(tbl.getId());
            }

            // Manually put partition into idToPartition (simulating it's in recycle bin)
            // Use reflection to access private field
            Field idToPartitionField = CatalogRecycleBin.class.getDeclaredField("idToPartition");
            idToPartitionField.setAccessible(true);
            ConcurrentHashMap<Long, CatalogRecycleBin.RecyclePartitionInfo> idToPartition =
                    (ConcurrentHashMap<Long, CatalogRecycleBin.RecyclePartitionInfo>) idToPartitionField.get(recycleBin);

            Field idToRecycleTimeField = CatalogRecycleBin.class.getDeclaredField("idToRecycleTime");
            idToRecycleTimeField.setAccessible(true);
            ConcurrentHashMap<Long, Long> idToRecycleTime =
                    (ConcurrentHashMap<Long, Long>) idToRecycleTimeField.get(recycleBin);

            // Partition: recycleTime = startTime - 200 (NOT expired)
            idToRecycleTime.put(testPartitionId, startTime - 200);
            CatalogRecycleBin.RecyclePartitionInfo pInfo = new CatalogRecycleBin().new RecyclePartitionInfo(
                    testDbId, testTableId, partition, null, null,
                    new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3),
                    false, false);
            idToPartition.put(testPartitionId, pInfo);

            // Put table into idToTable
            Field idToTableField = CatalogRecycleBin.class.getDeclaredField("idToTable");
            idToTableField.setAccessible(true);
            ConcurrentHashMap<Long, CatalogRecycleBin.RecycleTableInfo> idToTable =
                    (ConcurrentHashMap<Long, CatalogRecycleBin.RecycleTableInfo>) idToTableField.get(recycleBin);

            idToRecycleTime.put(testTableId, startTime - 100);
            CatalogRecycleBin.RecycleTableInfo tInfo =
                    new CatalogRecycleBin().new RecycleTableInfo(testDbId, table);
            idToTable.put(testTableId, tInfo);

            // Create RecycleDatabaseInfo
            CatalogRecycleBin.RecycleDatabaseInfo dbInfo =
                    new CatalogRecycleBin().new RecycleDatabaseInfo(db, tableNames, tableIds);

            // Call eraseAllTables via reflection
            Method eraseAllTablesMethod = CatalogRecycleBin.class.getDeclaredMethod("eraseAllTables",
                    CatalogRecycleBin.RecycleDatabaseInfo.class);
            eraseAllTablesMethod.setAccessible(true);
            eraseAllTablesMethod.invoke(recycleBin, dbInfo);

            // Verify: Table AND Partition are both cleaned
            // Partition should be cleaned as orphan
            Assert.assertNull(idToRecycleTime.get(testPartitionId));
            Assert.assertFalse(idToPartition.containsKey(testPartitionId));
            Assert.assertFalse(idToTable.containsKey(testTableId));
            Assert.assertNull(idToRecycleTime.get(testTableId));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
        }
    }

    /**
     * Test that eraseTableWithSameName cleans orphan partitions before erasing table.
     *
     * Scenario:
     * - Drop partition p from table t (p enters recycle bin)
     * - Drop table t multiple times to trigger same-name cleanup
     * - Directly call eraseTableWithSameName to verify orphan partition cleanup
     *
     * Expected: p is cleaned before t is erased
     */
    @Test
    public void testEraseTableWithSameNameCleansOrphanPartitions() throws Exception {
        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        int origMaxSameName = Config.max_same_name_catalog_trash_num;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;
            Config.max_same_name_catalog_trash_num = 1;

            final long baseId = System.nanoTime();
            final long testDbId = baseId + 1;
            final long testTableId1 = baseId + 2;
            final long testTableId2 = baseId + 3;
            final long testPartitionId = baseId + 4;
            final long testIndexId = baseId + 5;
            final long testTabletId = baseId + 6;

            long startTime = System.currentTimeMillis();

            CatalogRecycleBin recycleBin = new CatalogRecycleBin();

            // Create database with two tables (same name)
            String dbName = "test_db";
            String tableName = "test_table";

            Database db = new Database(testDbId, dbName);

            // Create table 1 with partition
            OlapTable table1 = createSimpleOlapTable(testTableId1, tableName, testPartitionId,
                    testIndexId, testTabletId, CatalogTestUtil.testStartVersion);
            db.registerTable(table1);

            // Create table 2 with partition (same name, different id)
            OlapTable table2 = createSimpleOlapTable(testTableId2, tableName, testPartitionId + 100,
                    testIndexId + 100, testTabletId + 100, CatalogTestUtil.testStartVersion);
            db.registerTable(table2);

            // Directly put partition into idToPartition (simulate it's in recycle bin)
            Field idToPartitionField = CatalogRecycleBin.class.getDeclaredField("idToPartition");
            idToPartitionField.setAccessible(true);
            ConcurrentHashMap<Long, CatalogRecycleBin.RecyclePartitionInfo> idToPartition =
                    (ConcurrentHashMap<Long, CatalogRecycleBin.RecyclePartitionInfo>) idToPartitionField.get(recycleBin);

            Field idToRecycleTimeField = CatalogRecycleBin.class.getDeclaredField("idToRecycleTime");
            idToRecycleTimeField.setAccessible(true);
            ConcurrentHashMap<Long, Long> idToRecycleTime =
                    (ConcurrentHashMap<Long, Long>) idToRecycleTimeField.get(recycleBin);

            // Partition: recycleTime = startTime - 200 (NOT expired, so erasePartition won't clean it)
            // But in direct test, we bypass erasePartition, so this doesn't matter
            idToRecycleTime.put(testPartitionId, startTime - 200);
            CatalogRecycleBin.RecyclePartitionInfo pInfo = new CatalogRecycleBin().new RecyclePartitionInfo(
                    testDbId, testTableId1, table1.getPartition(testPartitionId), null, null,
                    new DataProperty(TStorageMedium.HDD), new ReplicaAllocation((short) 3),
                    false, false);
            idToPartition.put(testPartitionId, pInfo);

            // Put tables into idToTable
            Field idToTableField = CatalogRecycleBin.class.getDeclaredField("idToTable");
            idToTableField.setAccessible(true);
            ConcurrentHashMap<Long, CatalogRecycleBin.RecycleTableInfo> idToTable =
                    (ConcurrentHashMap<Long, CatalogRecycleBin.RecycleTableInfo>) idToTableField.get(recycleBin);

            idToRecycleTime.put(testTableId1, startTime - 100);
            CatalogRecycleBin.RecycleTableInfo tInfo1 =
                    new CatalogRecycleBin().new RecycleTableInfo(testDbId, table1);
            idToTable.put(testTableId1, tInfo1);

            idToRecycleTime.put(testTableId2, startTime - 50);
            CatalogRecycleBin.RecycleTableInfo tInfo2 =
                    new CatalogRecycleBin().new RecycleTableInfo(testDbId, table2);
            idToTable.put(testTableId2, tInfo2);

            // Build dbIdTableNameToIds cache for same-name cleanup
            Field dbIdTableNameToIdsField = CatalogRecycleBin.class.getDeclaredField("dbIdTableNameToIds");
            dbIdTableNameToIdsField.setAccessible(true);
            Map<Pair<Long, String>, Set<Long>> dbIdTableNameToIds =
                    (Map<Pair<Long, String>, Set<Long>>) dbIdTableNameToIdsField.get(recycleBin);
            Set<Long> tableIdSet = Sets.newHashSet(testTableId1, testTableId2);
            dbIdTableNameToIds.put(Pair.of(testDbId, tableName), tableIdSet);

            // Directly call eraseTableWithSameName via reflection
            Method eraseTableWithSameNameMethod = CatalogRecycleBin.class.getDeclaredMethod(
                    "eraseTableWithSameName",
                    long.class, String.class, long.class, int.class, List.class);
            eraseTableWithSameNameMethod.setAccessible(true);

            List<Long> tableIdList = Lists.newArrayList(testTableId1, testTableId2);
            // Sort by recycle time desc: testTableId2 (startTime-50) is newer, testTableId1 (startTime-100) is older
            // maxSameNameTrashNum=1, so testTableId1 will be erased
            eraseTableWithSameNameMethod.invoke(recycleBin,
                    testDbId, tableName, startTime, 1, tableIdList);

            // Verify: Partition of erased table is cleaned, the other table's partition remains
            // testTableId1 is the older one, should be erased
            Assert.assertFalse(recycleBin.isRecyclePartition(testDbId, testTableId1, testPartitionId));
            // testTableId2 is kept, its partition should still exist
            Assert.assertTrue(recycleBin.isRecyclePartition(testDbId, testTableId2, testPartitionId + 100));

            // Verify testTableId1 is erased, testTableId2 is kept
            Assert.assertFalse(recycleBin.isRecycleTable(testDbId, testTableId1));
            Assert.assertTrue(recycleBin.isRecycleTable(testDbId, testTableId2));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
            Config.max_same_name_catalog_trash_num = origMaxSameName;
        }
    }

    @Test
    public void testDropTableThenDropDatabaseOrphanTable() throws Exception {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        recycleBin.clearAll();

        long origExpireSecond = Config.catalog_trash_expire_second;
        boolean origIgnoreMinErase = Config.catalog_trash_ignore_min_erase_latency;
        try {
            Config.catalog_trash_expire_second = 1;
            Config.catalog_trash_ignore_min_erase_latency = true;

            final long baseId = System.nanoTime();
            long dbId = baseId + 1;
            long tableId1 = baseId + 2;
            long tableId2 = baseId + 3;
            long partitionId1 = baseId + 4;
            long partitionId2 = baseId + 5;
            long indexId1 = baseId + 6;
            long indexId2 = baseId + 7;
            long tabletId1 = baseId + 8;
            long tabletId2 = baseId + 9;
            String tableName2 = "test_table2";

            Database db = createSimpleTestDatabase(
                    dbId, tableId1, partitionId1, indexId1, tabletId1, 1L);

            OlapTable table2 = createSimpleOlapTable(
                    tableId2, tableName2, partitionId2, indexId2, tabletId2, 1L);
            db.registerTable(table2);

            Assert.assertEquals(2, db.getTables().size());

            // Step 1: Drop table1 first
            OlapTable table1 = (OlapTable) db.getTable(tableId1).get();
            db.unregisterTable(tableId1);
            recycleBin.recycleTable(dbId, table1, false, false, 0);
            Assert.assertTrue(recycleBin.isRecycleTable(dbId, tableId1));
            Assert.assertEquals(1, db.getTables().size());

            // Step 2: Drop table2 to make database empty
            db.unregisterTable(tableId2);
            recycleBin.recycleTable(dbId, table2, false, false, 0);
            Assert.assertTrue(recycleBin.isRecycleTable(dbId, tableId2));
            Assert.assertEquals(0, db.getTables().size());

            // Step 3: Drop database (now empty)
            Set<String> tableNames = Sets.newHashSet(db.getTableNames()); // empty
            Set<Long> tableIds = Sets.newHashSet(db.getTableIds()); // empty

            Assert.assertTrue(tableNames.isEmpty());
            Assert.assertTrue(tableIds.isEmpty());

            long oldTime = System.currentTimeMillis() - 5000L;
            recycleBin.recycleDatabase(db, tableNames, tableIds, false, false, oldTime);

            // Manually set recycle times to expired
            Field idToRecycleTimeField = CatalogRecycleBin.class.getDeclaredField("idToRecycleTime");
            idToRecycleTimeField.setAccessible(true);
            ConcurrentHashMap<Long, Long> idToRecycleTime =
                    (ConcurrentHashMap<Long, Long>) idToRecycleTimeField.get(recycleBin);
            idToRecycleTime.put(dbId, oldTime);
            idToRecycleTime.put(tableId1, oldTime);
            idToRecycleTime.put(tableId2, oldTime);

            Assert.assertTrue(recycleBin.isRecycleDatabase(dbId));

            // Step 4: Run cleanup
            recycleBin.runAfterCatalogReady();

            // Step 5: Verify using reflection to check idToTable and idToRecycleTime
            Field idToTableField = CatalogRecycleBin.class.getDeclaredField("idToTable");
            idToTableField.setAccessible(true);
            ConcurrentHashMap<Long, CatalogRecycleBin.RecycleTableInfo> idToTable =
                    (ConcurrentHashMap<Long, CatalogRecycleBin.RecycleTableInfo>) idToTableField.get(recycleBin);

            Field idToDatabaseField = CatalogRecycleBin.class.getDeclaredField("idToDatabase");
            idToDatabaseField.setAccessible(true);
            ConcurrentHashMap<Long, CatalogRecycleBin.RecycleDatabaseInfo> idToDatabase =
                    (ConcurrentHashMap<Long, CatalogRecycleBin.RecycleDatabaseInfo>) idToDatabaseField.get(recycleBin);

            // Verify database has been cleaned
            Assert.assertFalse(idToDatabase.containsKey(dbId));

            // Verify table1 and table2 have been cleaned
            Assert.assertFalse(idToTable.containsKey(tableId1));
            Assert.assertFalse(idToTable.containsKey(tableId2));

            // Verify recycle times have been cleaned
            Assert.assertNull(recycleBin.getRecycleTimeById(dbId));
            Assert.assertNull(recycleBin.getRecycleTimeById(tableId1));
            Assert.assertNull(recycleBin.getRecycleTimeById(tableId2));

        } finally {
            Config.catalog_trash_expire_second = origExpireSecond;
            Config.catalog_trash_ignore_min_erase_latency = origIgnoreMinErase;
            recycleBin.clearAll();
        }
    }

    private Database createSimpleTestDatabase(long dbId, long tableId, long partitionId,
                                              long indexId, long tabletId, long startVersion) {
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", PrimitiveType.INT);
        k1.setIsKey(true);
        columns.add(k1);
        Column k2 = new Column("k2", PrimitiveType.INT);
        k2.setIsKey(true);
        columns.add(k2);
        Column v = new Column("v", ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.SUM, "0", "");
        columns.add(v);

        List<Column> keysColumn = new ArrayList<>();
        keysColumn.add(new Column("k1", PrimitiveType.INT));
        keysColumn.add(new Column("k2", PrimitiveType.INT));
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, keysColumn);

        Tablet tablet = new LocalTablet(tabletId);
        for (int i = 0; i < 3; i++) {
            long replicaId = tabletId * 100 + i;
            Replica replica = new LocalReplica(replicaId, 100 + i, startVersion, 0, 0L, 0L, 0L,
                    Replica.ReplicaState.NORMAL, -1, 0);
            tablet.addReplica(replica, true);
        }

        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, "p_" + partitionId, index, distributionInfo);
        partition.updateVisibleVersion(startVersion);
        partition.setNextVersion(startVersion + 1);

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        partitionInfo.setReplicaAllocation(partitionId, new ReplicaAllocation((short) 3));

        OlapTable table = new OlapTable(tableId, "t_" + tableId, columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "idx_" + indexId, columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setBaseIndexId(indexId);

        Database db = new Database(dbId, "db_" + dbId);
        db.registerTable(table);

        return db;
    }

    private OlapTable createSimpleOlapTable(long tableId, String tableName, long partitionId,
                                            long indexId, long tabletId, long startVersion) {
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", PrimitiveType.INT);
        k1.setIsKey(true);
        columns.add(k1);
        Column k2 = new Column("k2", PrimitiveType.INT);
        k2.setIsKey(true);
        columns.add(k2);
        Column v = new Column("v", ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.SUM, "0", "");
        columns.add(v);

        List<Column> keysColumn = new ArrayList<>();
        keysColumn.add(new Column("k1", PrimitiveType.INT));
        keysColumn.add(new Column("k2", PrimitiveType.INT));
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, keysColumn);

        Tablet tablet = new LocalTablet(tabletId);
        for (int i = 0; i < 3; i++) {
            long replicaId = tabletId * 100 + i;
            Replica replica = new LocalReplica(replicaId, 100 + i, startVersion, 0, 0L, 0L, 0L,
                    Replica.ReplicaState.NORMAL, -1, 0);
            tablet.addReplica(replica, true);
        }

        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(tableId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, "p_" + partitionId, index, distributionInfo);
        partition.updateVisibleVersion(startVersion);
        partition.setNextVersion(startVersion + 1);

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        partitionInfo.setReplicaAllocation(partitionId, new ReplicaAllocation((short) 3));

        OlapTable table = new OlapTable(tableId, tableName, columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "idx_" + indexId, columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setBaseIndexId(indexId);

        return table;
    }
}
