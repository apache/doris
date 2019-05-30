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

import com.google.common.collect.Lists;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ColocateTableTest {
    private TableName dbTableName1;
    private TableName dbTableName2;
    private TableName dbTableName3;
    private String dbName = "default:testDb";
    private String tableName1 = "t1";
    private String tableName2 = "t2";
    private String tableName3 = "t3";
    private String clusterName = "default";
    private List<Long> beIds = Lists.newArrayList();
    private List<String> columnNames = Lists.newArrayList();
    private List<ColumnDef> columnDefs = Lists.newArrayList();
    private Map<String, String> properties = new HashMap<String, String>();

    private Catalog catalog;
    private Database db;
    private Analyzer analyzer;

    @Injectable
    private ConnectContext connectContext;
    @Injectable
    private SystemInfoService systemInfoService;
    @Injectable
    private PaloAuth paloAuth;
    @Injectable
    private EditLog editLog;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        dbTableName1 = new TableName(dbName, tableName1);
        dbTableName2 = new TableName(dbName, tableName2);
        dbTableName3 = new TableName(dbName, tableName3);

        beIds.add(1L);
        beIds.add(2L);
        beIds.add(3L);

        columnNames.add("key1");
        columnNames.add("key2");

        columnDefs.add(new ColumnDef("key1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("key2", new TypeDef(ScalarType.createVarchar(10))));

        catalog = Catalog.getInstance();
        analyzer = new Analyzer(catalog, connectContext);

        new Expectations(analyzer) {
            {
                analyzer.getClusterName();
                result = clusterName;
            }
        };

        dbTableName1.analyze(analyzer);
        dbTableName2.analyze(analyzer);
        dbTableName3.analyze(analyzer);

        Config.disable_colocate_join = false;

        Catalog.getInstance().getColocateTableIndex().clear();

        new Expectations(catalog) {
            {
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                result = beIds;

                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.DROP);
                result = true; minTimes = 0; maxTimes = 1;
            }
        };

        new Expectations() {
            {
                Deencapsulation.setField(catalog, "editLog", editLog);
            }
        };

        InitDataBase();
        db = catalog.getDb(dbName);

        new MockUp<AgentBatchTask>() {
            @Mock
            void run() {
                return;
            }
        };

        new MockUp<CountDownLatch>() {
            @Mock
            boolean await(long timeout, TimeUnit unit) {
                return true;
            }
        };
    }

    private void InitDataBase() throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(true, dbName);
        new Expectations(dbStmt) {
            {
                dbStmt.getClusterName();
                result = clusterName;
            }
        };

        ConcurrentHashMap<String, Cluster> nameToCluster =  new ConcurrentHashMap<>();
        nameToCluster.put(clusterName, new Cluster(clusterName, 1));
        new Expectations() {
            {
                Deencapsulation.setField(catalog, "nameToCluster", nameToCluster);
            }
        };

        catalog.createDb(dbStmt);
    }

    @After
    public void tearDown() throws Exception {
        catalog.clear();
    }

    private void CreateParentTable(int numBecket, Map<String, String> properties) throws Exception {
        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName1, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(numBecket, Lists.newArrayList("key1")), properties, null);
        stmt.analyze(analyzer);
        catalog.createTable(stmt);
    }

    @Test
    public void testCreateAndDropParentTable() throws Exception {
        int numBecket = 1;

        CreateParentTable(numBecket, properties);

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        long tableId = db.getTable(tableName1).getId();

        Assert.assertEquals(1, index.getGroup2DB().size());
        Assert.assertEquals(1, index.getGroup2Tables().size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, index.getTable2Group().size());
        Assert.assertEquals(1, index.getGroup2BackendsPerBucketSeq().size());
        Assert.assertEquals(0, index.getBalancingGroupIds().size());

        Assert.assertTrue(index.isColocateTable(tableId));
        Assert.assertTrue(index.isColocateParentTable(tableId));
        Assert.assertFalse(index.isColocateChildTable(tableId));

        Assert.assertEquals(tableId, index.getGroup(tableId));

        Long dbId = db.getId();
        Assert.assertEquals(index.getDB(tableId), dbId);

        List<Long> backendIds = index.getBackendsPerBucketSeq(tableId).get(0);
        Assert.assertEquals(beIds, backendIds);

        DropTableStmt dropTableStmt = new DropTableStmt(false, dbTableName1);
        dropTableStmt.analyze(analyzer);
        catalog.dropTable(dropTableStmt);

        Assert.assertEquals(0, index.getGroup2DB().size());
        Assert.assertEquals(0, index.getGroup2Tables().size());
        Assert.assertEquals(0, index.getAllGroupIds().size());
        Assert.assertEquals(0, index.getTable2Group().size());
        Assert.assertEquals(0, index.getGroup2BackendsPerBucketSeq().size());
        Assert.assertEquals(0, index.getBalancingGroupIds().size());
    }

    @Test
    public void testCreateAndDropParentTableWithOneChild() throws Exception {
        int numBecket = 1;

        CreateParentTable(numBecket, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(numBecket, Lists.newArrayList("key1")), properties, null);
        childStmt.analyze(analyzer);
        catalog.createTable(childStmt);

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        long parentId = db.getTable(tableName1).getId();
        long childId = db.getTable(tableName2).getId();

        Assert.assertEquals(1, index.getGroup2DB().size());
        Assert.assertEquals(2, index.getGroup2Tables().size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(2, index.getTable2Group().size());
        Assert.assertEquals(1, index.getGroup2BackendsPerBucketSeq().size());
        Assert.assertEquals(0, index.getBalancingGroupIds().size());

        Assert.assertTrue(index.isColocateTable(parentId));
        Assert.assertTrue(index.isColocateParentTable(parentId));
        Assert.assertFalse(index.isColocateChildTable(parentId));

        Assert.assertTrue(index.isColocateTable(childId));
        Assert.assertFalse(index.isColocateParentTable(childId));
        Assert.assertTrue(index.isColocateChildTable(childId));

        Assert.assertEquals(parentId, index.getGroup(parentId));
        Assert.assertEquals(parentId, index.getGroup(childId));

        Assert.assertTrue(index.isSameGroup(parentId, childId));

        DropTableStmt dropTableStmt = new DropTableStmt(false, dbTableName2);
        dropTableStmt.analyze(analyzer);
        catalog.dropTable(dropTableStmt);

        Assert.assertEquals(1, index.getGroup2DB().size());
        Assert.assertEquals(1, index.getGroup2Tables().size());
        Assert.assertEquals(1, index.getTable2Group().size());
        Assert.assertEquals(1, index.getGroup2BackendsPerBucketSeq().size());
        Assert.assertEquals(0, index.getBalancingGroupIds().size());

        Assert.assertTrue(index.isColocateTable(parentId));
        Assert.assertTrue(index.isColocateParentTable(parentId));
        Assert.assertFalse(index.isColocateChildTable(parentId));

        Assert.assertFalse(index.isColocateTable(childId));
        Assert.assertFalse(index.isColocateParentTable(childId));
        Assert.assertFalse(index.isColocateChildTable(childId));

        Assert.assertFalse(index.isSameGroup(parentId, childId));
    }

    @Test
    // C -> B, B -> A
    public void testCreateAndDropMultilevelColocateTable() throws Exception {
        int numBecket = 1;

        CreateParentTable(numBecket, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(numBecket, Lists.newArrayList("key1")), properties, null);
        childStmt.analyze(analyzer);
        catalog.createTable(childStmt);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName2);
        CreateTableStmt grandchildStmt = new CreateTableStmt(false, false, dbTableName3, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(numBecket, Lists.newArrayList("key1")), properties, null);
        grandchildStmt.analyze(analyzer);
        catalog.createTable(grandchildStmt);

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();

        long parentId = db.getTable(tableName1).getId();
        long childId = db.getTable(tableName2).getId();
        long grandchildId = db.getTable(tableName3).getId();

        Assert.assertEquals(1, index.getGroup2DB().size());
        Assert.assertEquals(3, index.getGroup2Tables().size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(3, index.getTable2Group().size());
        Assert.assertEquals(1, index.getGroup2BackendsPerBucketSeq().size());
        Assert.assertEquals(0, index.getBalancingGroupIds().size());

        Assert.assertTrue(index.isColocateTable(parentId));
        Assert.assertTrue(index.isColocateParentTable(parentId));
        Assert.assertFalse(index.isColocateChildTable(parentId));

        Assert.assertTrue(index.isColocateTable(childId));
        Assert.assertFalse(index.isColocateParentTable(childId));
        Assert.assertTrue(index.isColocateChildTable(childId));

        Assert.assertTrue(index.isColocateTable(grandchildId));
        Assert.assertFalse(index.isColocateParentTable(grandchildId));
        Assert.assertTrue(index.isColocateChildTable(grandchildId));

        Assert.assertEquals(parentId, index.getGroup(parentId));
        Assert.assertEquals(parentId, index.getGroup(childId));
        Assert.assertEquals(parentId, index.getGroup(grandchildId));

        Assert.assertTrue(index.isSameGroup(parentId, childId));
        Assert.assertTrue(index.isSameGroup(parentId, grandchildId));
        Assert.assertTrue(index.isSameGroup(childId, grandchildId));

        DropTableStmt dropTableStmt = new DropTableStmt(false, dbTableName2);
        dropTableStmt.analyze(analyzer);
        catalog.dropTable(dropTableStmt);

        Assert.assertEquals(1, index.getGroup2DB().size());
        Assert.assertEquals(2, index.getGroup2Tables().size());
        Assert.assertEquals(2, index.getTable2Group().size());
        Assert.assertEquals(1, index.getGroup2BackendsPerBucketSeq().size());
        Assert.assertEquals(0, index.getBalancingGroupIds().size());

        Assert.assertTrue(index.isColocateTable(parentId));
        Assert.assertTrue(index.isColocateParentTable(parentId));
        Assert.assertFalse(index.isColocateChildTable(parentId));

        Assert.assertFalse(index.isColocateTable(childId));
        Assert.assertFalse(index.isColocateParentTable(childId));
        Assert.assertFalse(index.isColocateChildTable(childId));

        Assert.assertTrue(index.isColocateTable(grandchildId));
        Assert.assertFalse(index.isColocateParentTable(grandchildId));
        Assert.assertTrue(index.isColocateChildTable(grandchildId));

        Assert.assertEquals(parentId, index.getGroup(parentId));
        expectedEx.expect(IllegalStateException.class);
        index.getGroup(childId);
        Assert.assertEquals(parentId, index.getGroup(grandchildId));

        Assert.assertFalse(index.isSameGroup(parentId, childId));
        Assert.assertTrue(index.isSameGroup(parentId, grandchildId));
        Assert.assertFalse(index.isSameGroup(childId, grandchildId));
    }

    @Test
    public void testDropDbWithColocateTable() throws Exception {
        int numBecket = 1;

        CreateParentTable(numBecket, properties);

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        long tableId = db.getTable(tableName1).getId();

        Assert.assertEquals(1, index.getGroup2DB().size());
        Assert.assertEquals(1, index.getAllGroupIds().size());

        Long dbId = db.getId();
        Assert.assertEquals(index.getDB(tableId), dbId);

        DropDbStmt stmt = new DropDbStmt(false, dbName);
        catalog.dropDb(stmt);

        Assert.assertEquals(0, index.getGroup2DB().size());
        Assert.assertEquals(0, index.getAllGroupIds().size());
    }

    @Test
    public void testBucketNum() throws Exception {
        int parentBecketNum = 1;

        CreateParentTable(parentBecketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        int childBecketNum = 2;
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(childBecketNum, Lists.newArrayList("key1")), properties, null);
        childStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have the same bucket num: 1");

        catalog.createTable(childStmt);
    }

    @Test
    public void testReplicationNum() throws Exception {
        int bucketNum = 1;

        CreateParentTable(bucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "2");
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key1")), properties, null);
        childStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have the same replication num: 3");

        catalog.createTable(childStmt);
    }

    @Test
    public void testDistributionColumnsSize() throws Exception {
        int bucketNum = 1;

        CreateParentTable(bucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key1", "key2")), properties, null);
        childStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate table distribution columns size must be same : 1");

        catalog.createTable(childStmt);
    }

    @Test
    public void testDistributionColumnsType() throws Exception {
        int bucketNum = 1;

        CreateParentTable(bucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key2")), properties, null);
        childStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate table distribution columns must have the same data type: key2 should be INT");

        catalog.createTable(childStmt);
    }

    @Test
    public void testParentTableNotExist() throws Exception {
        testParentTableNotExist("t8");
    }

    @Test
    public void testParentTableNotExistCaseSensitive() throws Exception {
        testParentTableNotExist(tableName1.toUpperCase());
    }

    private void testParentTableNotExist(String tableName) throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName);

        int bucketNum = 1;
        CreateTableStmt parentStmt = new CreateTableStmt(false, false, dbTableName1, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key1")), properties, null);
        parentStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage(String.format("Colocate table '%s' no exist", tableName));

        catalog.createTable(parentStmt);
    }

    @Test
    public void testParentTableType() throws Exception {
        Table mysqlTable = new MysqlTable();
        String mysqlTableName = "mysqlTable";

        new Expectations(mysqlTable) {
            {
                mysqlTable.getName();
                result = mysqlTableName;
            }
        };

        new Expectations(db) {
            {
                db.getTable(tableName1);
                result = mysqlTable;
            }
        };

        Map<String, String> properties = new HashMap<String, String>();
        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, tableName1);
        CreateTableStmt parentStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null);
        parentStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage(String.format("Colocate tables '%s' must be OLAP table", mysqlTableName));

        catalog.createTable(parentStmt);
    }
}
