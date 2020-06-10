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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

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

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;

public class ColocateTableTest {
    private TableName dbTableName1;
    private TableName dbTableName2;
    private TableName dbTableName3;
    private String dbName = "default:testDb";
    private String groupName1 = "group1";
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

        beIds.clear();
        beIds.add(1L);
        beIds.add(2L);
        beIds.add(3L);

        columnNames.clear();
        columnNames.add("key1");
        columnNames.add("key2");

        columnDefs.clear();
        columnDefs.add(new ColumnDef("key1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("key2", new TypeDef(ScalarType.createVarchar(10))));

        catalog = Deencapsulation.newInstance(Catalog.class);
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

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                Catalog.getCurrentCatalog();
                result = catalog;

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

        initDatabase();
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

    private void initDatabase() throws Exception {
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

    private void createOneTable(int numBucket, Map<String, String> properties) throws Exception {
        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, groupName1);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName1, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(numBucket, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);
        catalog.createTable(stmt);
    }

    @Test
    public void testCreateOneTable() throws Exception {
        int numBucket = 1;

        createOneTable(numBucket, properties);

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        long tableId = db.getTable(tableName1).getId();

        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(tableId));

        Long dbId = db.getId();
        Assert.assertEquals(dbId, index.getGroup(tableId).dbId);

        GroupId groupId = index.getGroup(tableId);
        List<Long> backendIds = index.getBackendsPerBucketSeq(groupId).get(0);
        Assert.assertEquals(beIds, backendIds);

        String fullGroupName = dbId + "_" + groupName1;
        Assert.assertEquals(tableId, index.getTableIdByGroup(fullGroupName));
        ColocateGroupSchema groupSchema = index.getGroupSchema(fullGroupName);
        Assert.assertNotNull(groupSchema);
        Assert.assertEquals(dbId, groupSchema.getGroupId().dbId);
        Assert.assertEquals(numBucket, groupSchema.getBucketsNum());
        Assert.assertEquals(3, groupSchema.getReplicationNum());
    }

    @Test
    public void testCreateTwoTableWithSameGroup() throws Exception {
        int numBucket = 1;

        createOneTable(numBucket, properties);

        // create second table
        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, groupName1);
        CreateTableStmt secondStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(numBucket, Lists.newArrayList("key1")), properties, null, "");
        secondStmt.analyze(analyzer);
        catalog.createTable(secondStmt);

        ColocateTableIndex index = Catalog.getCurrentColocateIndex();
        long firstTblId = db.getTable(tableName1).getId();
        long secondTblId = db.getTable(tableName2).getId();
        
        Assert.assertEquals(2, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(2, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));

        Assert.assertTrue(index.isSameGroup(firstTblId, secondTblId));

        // drop first
        index.removeTable(firstTblId);
        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));
        Assert.assertFalse(index.isSameGroup(firstTblId, secondTblId));

        // drop second
        index.removeTable(secondTblId);
        Assert.assertEquals(0, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(0, index.getAllGroupIds().size());
        Assert.assertEquals(0, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(0,
                Deencapsulation.<Map<GroupId, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertFalse(index.isColocateTable(secondTblId));
    }

    @Test
    public void testBucketNum() throws Exception {
        int firstBucketNum = 1;
        createOneTable(firstBucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, groupName1);
        int secondBucketNum = 2;
        CreateTableStmt secondStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(secondBucketNum, Lists.newArrayList("key1")), properties, null, "");
        secondStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same bucket num: 1");

        catalog.createTable(secondStmt);
    }

    @Test
    public void testReplicationNum() throws Exception {
        int bucketNum = 1;

        createOneTable(bucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, groupName1);
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "2");
        CreateTableStmt secondStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key1")), properties, null, "");
        secondStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same replication num: 3");

        catalog.createTable(secondStmt);
    }

    @Test
    public void testDistributionColumnsSize() throws Exception {
        int bucketNum = 1;
        createOneTable(bucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, groupName1);
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key1", "key2")), properties, null, "");
        childStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns size must be same : 1");

        catalog.createTable(childStmt);
    }

    @Test
    public void testDistributionColumnsType() throws Exception {
        int bucketNum = 1;

        createOneTable(bucketNum, properties);

        properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, groupName1);
        CreateTableStmt childStmt = new CreateTableStmt(false, false, dbTableName2, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(bucketNum, Lists.newArrayList("key2")), properties, null, "");
        childStmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage(
                "Colocate tables distribution columns must have the same data type: key2 should be INT");

        catalog.createTable(childStmt);
    }
}
