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
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;

public class DynamicPartitionTableTest {
    private TableName dbTableName;
    private String dbName = "testDb";
    private String tableName = "testTable";
    private String clusterName = "default";
    private List<Long> beIds = Lists.newArrayList();
    private List<String> columnNames = Lists.newArrayList();
    private List<ColumnDef> columnDefs = Lists.newArrayList();

    private Catalog catalog = Catalog.getInstance();
    private Database db = new Database();
    private Analyzer analyzer;

    private Map<String, String> properties;
    private List<SingleRangePartitionDesc> singleRangePartitionDescs;

    @Injectable
    ConnectContext connectContext;

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
        columnNames.add("key3");

        columnDefs.add(new ColumnDef("key1", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("key2", new TypeDef(ScalarType.createType(PrimitiveType.INT))));
        columnDefs.add(new ColumnDef("key3", new TypeDef(ScalarType.createVarchar(10))));

        analyzer = new Analyzer(catalog, connectContext);

        properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.START, "-3");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");

        singleRangePartitionDescs = new LinkedList<>();
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-128"))), null));

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

        new Expectations(analyzer, catalog) {{
            analyzer.getClusterName();
            minTimes = 0;
            result = clusterName;
        }};

        dbTableName.analyze(analyzer);
    }

    @Test
    public void testNormal(@Injectable SystemInfoService systemInfoService,
                           @Injectable PaloAuth paloAuth,
                           @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        catalog.createTable(stmt);
    }

    @Test
    public void testMissPrefix(@Injectable SystemInfoService systemInfoService,
                               @Injectable PaloAuth paloAuth,
                               @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        properties.remove(DynamicPartitionProperty.PREFIX);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.prefix properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testMissTimeUnit(@Injectable SystemInfoService systemInfoService,
                                 @Injectable PaloAuth paloAuth,
                                 @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        properties.remove(DynamicPartitionProperty.TIME_UNIT);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.time_unit properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testMissSTART(@Injectable SystemInfoService systemInfoService,
                              @Injectable PaloAuth paloAuth,
                              @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        properties.remove(DynamicPartitionProperty.START);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        catalog.createTable(stmt);
    }

    @Test
    public void testMissEnd(@Injectable SystemInfoService systemInfoService,
                            @Injectable PaloAuth paloAuth,
                            @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        properties.remove(DynamicPartitionProperty.END);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.end properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testMissBuckets(@Injectable SystemInfoService systemInfoService,
                                @Injectable PaloAuth paloAuth,
                                @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        properties.remove(DynamicPartitionProperty.BUCKETS);

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Must assign dynamic_partition.buckets properties");

        catalog.createTable(stmt);
    }

    @Test
    public void testNotAllowed(@Injectable SystemInfoService systemInfoService,
                               @Injectable PaloAuth paloAuth,
                               @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Only support dynamic partition properties on range partition table");

        catalog.createTable(stmt);
    }

    @Test
    public void testNotAllowedInMultiPartitions(@Injectable SystemInfoService systemInfoService,
                                                @Injectable PaloAuth paloAuth,
                                                @Injectable EditLog editLog) throws UserException {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                minTimes = 0;
                result = db;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                minTimes = 0;
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                minTimes = 0;
                result = beIds;

                catalog.getAuth();
                minTimes = 0;
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        List<SingleRangePartitionDesc> rangePartitionDescs = new LinkedList<>();
        rangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-128"), new PartitionValue("100"))), null));

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames),
                new RangePartitionDesc(Lists.newArrayList("key1", "key2"), singleRangePartitionDescs),
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null, "");
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Dynamic partition only support single-column range partition");

        catalog.createTable(stmt);
    }
}
