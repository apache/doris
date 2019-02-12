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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CreateTableTest {

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

    @Injectable
    ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws AnalysisException {
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

        dbTableName.analyze(analyzer);
    }

    @Test
    public void testNormalOlap(@Injectable SystemInfoService systemInfoService, @Injectable PaloAuth paloAuth,
            @Injectable EditLog editLog) throws Exception {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                result = db;

                Catalog.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                result = beIds;

                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;

                catalog.getEditLog();
                result = editLog;
            }
        };

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

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), null, null);
        stmt.analyze(analyzer);

        catalog.createTable(stmt);
    }

    @Test
    public void testUnknownDatabase(@Injectable PaloAuth paloAuth) throws Exception {
        new Expectations(catalog) {
            {
                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
            }
        };

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), null, null);

        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Unknown database 'default:testDb'");

        catalog.createTable(stmt);
    }

    @Test
    public void testShortKeyTooLarge(@Injectable SystemInfoService systemInfoService, @Injectable PaloAuth paloAuth)
            throws Exception {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                result = db;

                Catalog.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);

                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
            }
        };

        Map<String, String> properties = new HashMap<String, String>();
        //larger then indexColumns size
        properties.put(PropertyAnalyzer.PROPERTIES_SHORT_KEY, "3");

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null);
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Short key is too large. should less than: 2");

        catalog.createTable(stmt);
    }

    @Test
    public void testShortKeyVarcharMiddle(@Injectable SystemInfoService systemInfoService,
            @Injectable PaloAuth paloAuth) throws Exception {
        columnDefs.clear();
        columnDefs.add(new ColumnDef("key1", new TypeDef(ScalarType.createVarchar(10))));
        columnDefs.add(new ColumnDef("key2", new TypeDef(ScalarType.createType(PrimitiveType.INT))));

        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                result = db;

                Catalog.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);

                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
            }
        };

        Map<String, String> properties = new HashMap<String, String>();
        properties.put(PropertyAnalyzer.PROPERTIES_SHORT_KEY, "2");

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), properties, null);
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Varchar should not in the middle of short keys.");

        catalog.createTable(stmt);
    }

    @Test
    public void testNotEnoughBackend(@Injectable SystemInfoService systemInfoService, @Injectable PaloAuth paloAuth)
            throws Exception {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                result = db;

                Catalog.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                result = null;

                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
            }
        };

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), null, null);
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Failed to find enough host in all backends. need: 3");

        catalog.createTable(stmt);
    }

    @Test
    public void testOlapTableExists(@Injectable SystemInfoService systemInfoService, @Injectable PaloAuth paloAuth)
            throws Exception {
        Table olapTable = new OlapTable();
        new Expectations(db) {
            {
                db.getTable(tableName);
                result = olapTable;
            }
        };

        new Expectations(catalog) {
            {

                catalog.getDb(dbTableName.getDb());
                result = db;
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.checkClusterCapacity(anyString);
                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
            }
        };

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), null, null);
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Table 'testTable' already exists");

        catalog.createTable(stmt);
    }

    @Test
    public void testOlapTimeOut(@Injectable SystemInfoService systemInfoService, @Injectable PaloAuth paloAuth)
            throws Exception {
        new Expectations(catalog) {
            {
                catalog.getDb(dbTableName.getDb());
                result = db;

                Catalog.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.checkClusterCapacity(anyString);
                systemInfoService.seqChooseBackendIds(anyInt, true, true, anyString);
                result = beIds;

                catalog.getAuth();
                result = paloAuth;
                paloAuth.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.CREATE);
                result = true;
            }
        };

        CreateTableStmt stmt = new CreateTableStmt(false, false, dbTableName, columnDefs, "olap",
                new KeysDesc(KeysType.AGG_KEYS, columnNames), null,
                new HashDistributionDesc(1, Lists.newArrayList("key1")), null, null);
        stmt.analyze(analyzer);

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Failed to create partition[testTable]. Timeout");

        catalog.createTable(stmt);
    }
}
