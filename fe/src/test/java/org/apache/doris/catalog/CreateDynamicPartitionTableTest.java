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
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CreateDynamicPartitionTableTest {
    private TableName dbTableName;
    private String dbName = "default:testDb";
    private String tableName = "testDynamicPartitionTable";
    private String clusterName = "default";
    private List<Long> beIds = Lists.newArrayList();
    private List<String> columnNames = Lists.newArrayList();
    private List<ColumnDef> columnDefs = Lists.newArrayList();

    private Catalog catalog = Catalog.getInstance();
    private Database db = new Database();
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
        dbTableName = new TableName(dbName, tableName);

        beIds.add(1L);
        beIds.add(2L);
        beIds.add(3L);

        columnNames.add("key1");
        columnNames.add("key2");

        columnDefs.add(new ColumnDef("key1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("key2", new TypeDef(ScalarType.createVarchar(10))));

        analyzer = new Analyzer(catalog, connectContext);

        new Expectations(analyzer) {
            {
                analyzer.getClusterName();
                result = clusterName;
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

    @Test
    public void testCreateNormalDynamicPartitionTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.TIME_UNIT.getDesc(), "day");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.END.getDesc(), "3");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.PREFIX.getDesc(), "p");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.BUCKETS.getDesc(), "30");

        SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("20191202"))), null);
        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(Collections.singletonList("key1"),
                Collections.singletonList(singleRangePartitionDesc));
        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), rangePartitionDesc,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        catalog.createTable(stmt);
    }

    @Test
    public void testCreateNoRangePartitionTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.TIME_UNIT.getDesc(), "day");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.END.getDesc(), "3");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.PREFIX.getDesc(), "p");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.BUCKETS.getDesc(), "30");
        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Only support dynamic partition properties on range partition table");

        catalog.createTable(stmt);
    }

    @Test
    public void testCreateMissTimeUnit() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.END.getDesc(), "3");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.PREFIX.getDesc(), "p");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.BUCKETS.getDesc(), "30");

        SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("20191202"))), null);
        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(Collections.singletonList("key1"),
                Collections.singletonList(singleRangePartitionDesc));
        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), rangePartitionDesc,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.time_unit properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testCreateMissEnd() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.TIME_UNIT.getDesc(), "day");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.PREFIX.getDesc(), "p");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.BUCKETS.getDesc(), "30");

        SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("20191202"))), null);
        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(Collections.singletonList("key1"),
                Collections.singletonList(singleRangePartitionDesc));
        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), rangePartitionDesc,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.end properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testCreateMissPrefix() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.TIME_UNIT.getDesc(), "day");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.END.getDesc(), "3");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.BUCKETS.getDesc(), "30");

        SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("20191202"))), null);
        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(Collections.singletonList("key1"),
                Collections.singletonList(singleRangePartitionDesc));
        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), rangePartitionDesc,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.prefix properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testCreateMissBuckets() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.TIME_UNIT.getDesc(), "day");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.END.getDesc(), "3");
        properties.put(DynamicPartitionUtils.DynamicPartitionProperties.PREFIX.getDesc(), "p");

        SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("20191202"))), null);
        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(Collections.singletonList("key1"),
                Collections.singletonList(singleRangePartitionDesc));
        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), rangePartitionDesc,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.buckets properties");

        catalog.createTable(stmt);
    }
}
