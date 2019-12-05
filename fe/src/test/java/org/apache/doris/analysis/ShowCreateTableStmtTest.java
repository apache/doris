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

package org.apache.doris.analysis;

import com.google.common.collect.Lists;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.thrift.TStorageMedium;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Mocked;
import mockit.internal.startup.Startup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.doris.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_COLDOWN_TIME;
import static org.apache.doris.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM;

public class ShowCreateTableStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Injectable
    private SystemInfoService systemInfoService;
    @Injectable
    private PaloAuth paloAuth;
    @Injectable
    private EditLog editLog;
    private Catalog catalog;
    private String dbName = "testDb";
    private String clusterDBName = "testCluster:testDb";
    private String clusterName = "testCluster";

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws AnalysisException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName(dbName, "testTbl"));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW CREATE TABLE testCluster:testDb.testTbl", stmt.toString());
        Assert.assertEquals(clusterDBName, stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTable());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Table", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Create Table", stmt.getMetaData().getColumn(1).getName());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTbl() throws AnalysisException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(null);
        stmt.analyze(analyzer);
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testShowStorageMedium() throws Exception {
        List<Long> beIds = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        List<ColumnDef> columnDefs = new ArrayList<>();
        beIds.clear();
        beIds.add(1L);
        beIds.add(2L);
        beIds.add(3L);

        catalog = Deencapsulation.newInstance(Catalog.class);
        analyzer = new Analyzer(catalog, ctx);

        new Expectations(analyzer) {
            {
                analyzer.getClusterName();
                result = "testCluster";
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                Catalog.getInstance();
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
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.SHOW);
                result = true; minTimes = 0; maxTimes = 4;

                ConnectContext.get().getSessionVariable().getTimeZone();
                result = TimeZone.getDefault().getID();
            }
        };

        new Expectations() {
            {
                Deencapsulation.setField(catalog, "editLog", editLog);
            }
        };

        initDatabase();

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

        // make test table
        TableName partitionSSDTable = new TableName(clusterDBName, "p_ssd_tab");
        TableName partitionHDDTable = new TableName(clusterDBName, "p_hdd_tab");
        TableName unPartitionSSDTable = new TableName(clusterDBName, "up_ssd_tab");
        TableName unPartitionHDDTable = new TableName(clusterDBName, "up_hdd_tab");
        partitionSSDTable.analyze(analyzer);
        partitionHDDTable.analyze(analyzer);
        unPartitionSSDTable.analyze(analyzer);
        unPartitionHDDTable.analyze(analyzer);

        columnNames.add("key1");
        columnNames.add("key2");
        columnDefs.add(new ColumnDef("key1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("key2", new TypeDef(ScalarType.createVarchar(10))));

        // make test properties
        String storageColdownTime = "2020-10-04 22:10:23";
        Map<String, String> ssdProperties = new HashMap<>();
        ssdProperties.put(PROPERTIES_STORAGE_MEDIUM, TStorageMedium.SSD.toString());
        ssdProperties.put(PROPERTIES_STORAGE_COLDOWN_TIME, storageColdownTime);

        Map<String, String> hddProperties = new HashMap<>();
        hddProperties.put(PROPERTIES_STORAGE_MEDIUM, TStorageMedium.HDD.toString());

        HashDistributionDesc hashPartition = new HashDistributionDesc(3, Lists.newArrayList("key2"));

        // test partition ssd table
        createTableWithProperties(partitionSSDTable, columnDefs, columnNames, new HashMap<>(ssdProperties), new RangePartitionDesc(Lists.newArrayList("key1"),
                Lists.newArrayList(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("2001"))), null))), hashPartition);
        String partitionSSDTableString = getShowCreateTableStmtString(partitionSSDTable);

        Assert.assertTrue(partitionSSDTableString.contains(PROPERTIES_STORAGE_MEDIUM));
        Assert.assertTrue(partitionSSDTableString.contains(TStorageMedium.SSD.toString()));
        Assert.assertTrue(partitionSSDTableString.contains(PROPERTIES_STORAGE_COLDOWN_TIME));
        Assert.assertTrue(partitionSSDTableString.contains(storageColdownTime));

        // test partition hdd table
        createTableWithProperties(partitionHDDTable, columnDefs, columnNames, hddProperties, new RangePartitionDesc(Lists.newArrayList("key1"),
                Lists.newArrayList(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("2001"))), null))), hashPartition);
        String partitionHDDTableString = getShowCreateTableStmtString(partitionHDDTable);
        Assert.assertTrue(partitionHDDTableString.contains(PROPERTIES_STORAGE_MEDIUM));
        Assert.assertTrue(partitionHDDTableString.contains(TStorageMedium.HDD.toString()));

        // test unpartition ssd table
        createTableWithProperties(unPartitionSSDTable, columnDefs, columnNames, new HashMap<>(ssdProperties), null, new HashDistributionDesc(3, Lists.newArrayList("key2")));
        String unPartitionSSDTableString = getShowCreateTableStmtString(unPartitionSSDTable);

        Assert.assertTrue(unPartitionSSDTableString.contains(PROPERTIES_STORAGE_MEDIUM));
        Assert.assertTrue(unPartitionSSDTableString.contains(TStorageMedium.SSD.toString()));
        Assert.assertTrue(unPartitionSSDTableString.contains(PROPERTIES_STORAGE_COLDOWN_TIME));
        Assert.assertTrue(unPartitionSSDTableString.contains(storageColdownTime));


        // test unpartition hdd table
        createTableWithProperties(unPartitionHDDTable, columnDefs, columnNames, hddProperties, null, hashPartition);
        String unPartitionHDDTableString = getShowCreateTableStmtString(unPartitionHDDTable);
        Assert.assertTrue(unPartitionHDDTableString.contains(PROPERTIES_STORAGE_MEDIUM));
        Assert.assertTrue(unPartitionHDDTableString.contains(TStorageMedium.HDD.toString()));
    }

    private String getShowCreateTableStmtString(TableName dbTableName) throws Exception {
        ShowCreateTableStmt showCreateTableStmt = new ShowCreateTableStmt(dbTableName);
        showCreateTableStmt.analyze(analyzer);
        ShowExecutor executor = new ShowExecutor(ctx, showCreateTableStmt);
        ShowResultSet resultSet = executor.execute();
        return resultSet.getResultRows().get(0).get(1);
    }

    private void createTableWithProperties(TableName tableName, List<ColumnDef> columnDefs, List<String> columnNames, Map<String, String> properties,
                                           PartitionDesc partitionDesc, DistributionDesc distributionDesc) throws Exception {
        CreateTableStmt stmt = new CreateTableStmt(false, false, tableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), partitionDesc, distributionDesc, properties, null, "");
        stmt.analyze(analyzer);
        catalog.createTable(stmt);
    }

    private void initDatabase() throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(true, clusterDBName);
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

}