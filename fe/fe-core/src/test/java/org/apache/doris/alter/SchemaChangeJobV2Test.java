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

package org.apache.doris.alter;

import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.AddColumnClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.SchemaVersionAndHash;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaChangeJobV2Test {

    private static String fileName = "./SchemaChangeV2Test";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Env masterEnv;
    private static Env slaveEnv;

    private static Analyzer analyzer;
    private static ColumnDef newCol = new ColumnDef("add_v", new TypeDef(ScalarType.createType(PrimitiveType.INT)),
            false, AggregateType.MAX, false, new DefaultValue(true, "1"), "");
    private static AddColumnClause addColumnClause = new AddColumnClause(newCol, new ColumnPosition("v"), null, null);

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, AnalysisException, DdlException {
        FakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterEnv = CatalogTestUtil.createTestCatalog();
        slaveEnv = CatalogTestUtil.createTestCatalog();
        masterTransMgr = masterEnv.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterEnv.getEditLog());
        slaveTransMgr = slaveEnv.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveEnv.getEditLog());
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        addColumnClause.analyze(analyzer);

        FeConstants.runningUnitTest = true;
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testAddSchemaChange() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(addColumnClause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        schemaChangeHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();

        // add a schema change job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(addColumnClause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        schemaChangeHandler.process(alterClauses,  db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        Assert.assertEquals(IndexState.NORMAL, baseIndex.getState());
        Assert.assertEquals(PartitionState.NORMAL, testPartition.getState());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica1 = replicas.get(0);
        Replica replica2 = replicas.get(1);
        Replica replica3 = replicas.get(2);

        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        Assert.assertEquals(-1, replica1.getLastFailedVersion());
        Assert.assertEquals(-1, replica2.getLastFailedVersion());
        Assert.assertEquals(-1, replica3.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getLastSuccessVersion());

        // runPendingJob
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        // runWaitingTxnJob, task not finished
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        // runRunningJob
        schemaChangeHandler.runAfterCatalogReady();
        // task not finished, still running
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        // finish alter tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersionInfo(testPartition.getVisibleVersion(), shadowReplica.getDataSize(),
                        shadowReplica.getRemoteDataSize(), shadowReplica.getRowCount());
            }
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, schemaChangeJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();

        // add a schema change job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(addColumnClause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        schemaChangeHandler.process(alterClauses,  db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        SchemaChangeJobV2 schemaChangeJob = (SchemaChangeJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        Assert.assertEquals(IndexState.NORMAL, baseIndex.getState());
        Assert.assertEquals(PartitionState.NORMAL, testPartition.getState());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());

        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica1 = replicas.get(0);
        Replica replica2 = replicas.get(1);
        Replica replica3 = replicas.get(2);

        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        Assert.assertEquals(-1, replica1.getLastFailedVersion());
        Assert.assertEquals(-1, replica2.getLastFailedVersion());
        Assert.assertEquals(-1, replica3.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.PENDING, schemaChangeJob.getJobState());

        // table is stable runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        // runWaitingTxnJob, task not finished
        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        // runRunningJob
        schemaChangeHandler.runAfterCatalogReady();
        // task not finished, still running
        Assert.assertEquals(JobState.RUNNING, schemaChangeJob.getJobState());

        // finish alter tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersionInfo(testPartition.getVisibleVersion(), shadowReplica.getDataSize(),
                        shadowReplica.getRemoteDataSize(), shadowReplica.getRowCount());
            }
        }

        schemaChangeHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, schemaChangeJob.getJobState());
    }

    @Test
    public void testModifyDynamicPartitionNormal() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");
        alterClauses.add(new ModifyTablePropertiesClause(properties));
        Database db = CatalogMocker.mockDb();
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogMocker.TEST_TBL2_ID);
        schemaChangeHandler.process(alterClauses,  db, olapTable);
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isExist());
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().getEnable());
        Assert.assertEquals("day", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        Assert.assertEquals("p", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        Assert.assertEquals(30, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());


        // set dynamic_partition.enable = false
        ArrayList<AlterClause> tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.ENABLE, "false");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses,  db, olapTable);
        Assert.assertFalse(olapTable.getTableProperty().getDynamicPartitionProperty().getEnable());
        // set dynamic_partition.time_unit = week
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "week");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses,  db, olapTable);
        Assert.assertEquals("week", olapTable.getTableProperty().getDynamicPartitionProperty().getTimeUnit());
        // set dynamic_partition.end = 10
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.END, "10");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses,  db, olapTable);
        Assert.assertEquals(10, olapTable.getTableProperty().getDynamicPartitionProperty().getEnd());
        // set dynamic_partition.prefix = p1
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.PREFIX, "p1");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses,  db, olapTable);
        Assert.assertEquals("p1", olapTable.getTableProperty().getDynamicPartitionProperty().getPrefix());
        // set dynamic_partition.buckets = 3
        tmpAlterClauses = new ArrayList<>();
        properties.put(DynamicPartitionProperty.BUCKETS, "3");
        tmpAlterClauses.add(new ModifyTablePropertiesClause(properties));
        schemaChangeHandler.process(tmpAlterClauses,  db, olapTable);
        Assert.assertEquals(3, olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets());
    }

    public void modifyDynamicPartitionWithoutTableProperty(String propertyKey, String propertyValue)
            throws UserException {
        fakeEnv = new FakeEnv();
        FakeEnv.setEnv(masterEnv);
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyKey, propertyValue);
        alterClauses.add(new ModifyTablePropertiesClause(properties));

        Database db = CatalogMocker.mockDb();
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogMocker.TEST_TBL2_ID);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2,"
                + " detailMessage = Table test_db.test_tbl2 is not a dynamic partition table. "
                + "Use command `HELP ALTER TABLE` to see how to change a normal table to a dynamic partition table.");
        schemaChangeHandler.process(alterClauses,  db, olapTable);
    }

    @Test
    public void testModifyDynamicPartitionWithoutTableProperty() throws UserException {
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.ENABLE, "false");
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.TIME_UNIT, "day");
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.END, "3");
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.PREFIX, "p");
        modifyDynamicPartitionWithoutTableProperty(DynamicPartitionProperty.BUCKETS, "30");
    }

    @Test
    public void testSerializeOfSchemaChangeJob() throws IOException {
        // prepare file
        File file = new File(fileName);
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        SchemaChangeJobV2 schemaChangeJobV2 = new SchemaChangeJobV2("", 1, 1, 1, "test", 600000);
        schemaChangeJobV2.setStorageFormat(TStorageFormat.V2);
        Deencapsulation.setField(schemaChangeJobV2, "jobState", AlterJobV2.JobState.FINISHED);
        Map<Long, SchemaVersionAndHash> indexSchemaVersionAndHashMap = Maps.newHashMap();
        indexSchemaVersionAndHashMap.put(Long.valueOf(1000), new SchemaVersionAndHash(10, 20));
        Deencapsulation.setField(schemaChangeJobV2, "indexSchemaVersionAndHashMap", indexSchemaVersionAndHashMap);

        // write schema change job
        schemaChangeJobV2.write(out);
        out.flush();
        out.close();

        // read objects from file
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        SchemaChangeJobV2 result = (SchemaChangeJobV2) AlterJobV2.read(in);
        Assert.assertEquals(1, result.getJobId());
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, result.getJobState());
        Assert.assertEquals(TStorageFormat.V2, Deencapsulation.getField(result, "storageFormat"));

        Assert.assertNotNull(Deencapsulation.getField(result, "partitionIndexMap"));
        Assert.assertNotNull(Deencapsulation.getField(result, "partitionIndexTabletMap"));

        Map<Long, SchemaVersionAndHash> map = Deencapsulation.getField(result, "indexSchemaVersionAndHashMap");
        Assert.assertEquals(10, map.get(1000L).schemaVersion);
        Assert.assertEquals(20, map.get(1000L).schemaHash);
    }

    @Test
    public void testModifyTableDistributionType() throws DdlException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        Database db = masterEnv.getInternalCatalog().getDb(CatalogTestUtil.testDbId1).get();
        OlapTable olapTable = (OlapTable) db.getTable(CatalogTestUtil.testTableId1).get();
        Env.getCurrentEnv().convertDistributionType(db, olapTable);
        Assert.assertTrue(olapTable.getDefaultDistributionInfo().getType() == DistributionInfo.DistributionInfoType.RANDOM);
        Partition partition1 = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        Assert.assertTrue(partition1.getDistributionInfo().getType() == DistributionInfo.DistributionInfoType.RANDOM);
    }

    @Test
    public void testAbnormalModifyTableDistributionType1(@Injectable OlapTable table) throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        Database db = masterEnv.getInternalCatalog().getDb(CatalogTestUtil.testDbId1).get();
        new Expectations() {
            {
                table.isColocateTable();
                result = true;
            }
        };
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Cannot change distribution type of colocate table.");
        Env.getCurrentEnv().convertDistributionType(db, table);
    }

    @Test
    public void testAbnormalModifyTableDistributionType2(@Injectable OlapTable table) throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        Database db = masterEnv.getInternalCatalog().getDb(CatalogTestUtil.testDbId1).get();
        new Expectations() {
            {
                table.isColocateTable();
                result = false;
                table.getKeysType();
                result = KeysType.UNIQUE_KEYS;
            }
        };
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Cannot change distribution type of unique keys table.");
        Env.getCurrentEnv().convertDistributionType(db, table);
    }

    @Test
    public void testAbnormalModifyTableDistributionType3(@Injectable OlapTable table) throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        Database db = masterEnv.getInternalCatalog().getDb(CatalogTestUtil.testDbId1).get();
        new Expectations() {
            {
                table.isColocateTable();
                result = false;
                table.getKeysType();
                result = KeysType.AGG_KEYS;
                table.getBaseSchema();
                result = Lists.newArrayList(
                    new Column("k1", Type.INT, true, null, "0", ""),
                    new Column("v1", Type.INT, false, AggregateType.REPLACE, "0", ""));
            }
        };
        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("errCode = 2, detailMessage = Cannot change "
                + "distribution type of aggregate keys table which has value columns with REPLACE type.");
        Env.getCurrentEnv().convertDistributionType(db, table);
    }
}
