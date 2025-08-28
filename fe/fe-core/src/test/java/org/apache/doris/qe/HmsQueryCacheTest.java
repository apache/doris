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

package org.apache.doris.qe;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.HiveDlaTable;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.datasource.systable.SupportedSysTables;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.SqlCache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class HmsQueryCacheTest extends AnalyzeCheckTestBase {
    private static final String HMS_CATALOG = "hms_ctl";
    private static final long NOW = System.currentTimeMillis();
    private Env env;
    private CatalogMgr mgr;
    private OlapScanNode olapScanNode;

    private HMSExternalTable tbl;
    private HMSExternalTable tbl2;
    private HMSExternalTable view1;
    private HMSExternalTable view2;
    private HiveScanNode hiveScanNode1;
    private HiveScanNode hiveScanNode2;
    private HiveScanNode hiveScanNode3;
    private HiveScanNode hiveScanNode4;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_query_hive_views = true;
        Config.cache_enable_sql_mode = true;
        connectContext.getSessionVariable().setEnableSqlCache(true);

        env = Env.getCurrentEnv();
        connectContext.setEnv(env);
        mgr = env.getCatalogMgr();

        // create hms catalog
        String createStmt = "create catalog hms_ctl "
                + "properties("
                + "'type' = 'hms', "
                + "'hive.metastore.uris' = 'thrift://192.168.0.1:9083');";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(connectContext, null);
        }

        // create inner db and tbl for test
        mgr.getInternalCatalog().createDb("test", false, Maps.newHashMap());
        createTable("create table test.tbl1(\n"
                + "k1 int comment 'test column k1', "
                + "k2 int comment 'test column k2')  comment 'test table1' "
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
    }

    private void setField(Object target, String fieldName, Object value) {
        // try {
        //     Field field = target.getClass().getDeclaredField(fieldName);
        //     field.setAccessible(true);
        //     field.set(target, value);
        // } catch (Exception e) {
        //     throw new RuntimeException(e);
        // }

        Deencapsulation.setField(target, fieldName, value);
    }

    private void init(HMSExternalCatalog hmsCatalog) {
        // Create mock objects
        tbl = Mockito.mock(HMSExternalTable.class);
        tbl2 = Mockito.mock(HMSExternalTable.class);
        view1 = Mockito.mock(HMSExternalTable.class);
        view2 = Mockito.mock(HMSExternalTable.class);
        hiveScanNode1 = Mockito.mock(HiveScanNode.class);
        hiveScanNode2 = Mockito.mock(HiveScanNode.class);
        hiveScanNode3 = Mockito.mock(HiveScanNode.class);
        hiveScanNode4 = Mockito.mock(HiveScanNode.class);

        setField(hmsCatalog, "initialized", true);
        setField(hmsCatalog, "objectCreated", true);
        setField(hmsCatalog, "useMetaCache", Optional.of(false));

        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", PrimitiveType.INT));

        HMSExternalDatabase db = new HMSExternalDatabase(hmsCatalog, 10000, "hms_db", "hms_db");
        setField(db, "initialized", true);

        setField(tbl, "objectCreated", true);
        setField(tbl, "schemaUpdateTime", NOW);
        setField(tbl, "eventUpdateTime", 0);
        setField(tbl, "catalog", hmsCatalog);
        setField(tbl, "dbName", "hms_db");
        setField(tbl, "name", "hms_tbl");
        setField(tbl, "dlaTable", new HiveDlaTable(tbl));
        setField(tbl, "dlaType", DLAType.HIVE);

        Mockito.when(tbl.getId()).thenReturn(10001L);
        Mockito.when(tbl.getName()).thenReturn("hms_tbl");
        Mockito.when(tbl.getDbName()).thenReturn("hms_db");
        Mockito.when(tbl.getFullSchema()).thenReturn(schema);
        Mockito.when(tbl.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(tbl.isView()).thenReturn(false);
        Mockito.when(tbl.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(tbl.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(tbl.getDatabase()).thenReturn(db);
        Mockito.when(tbl.getUpdateTime()).thenReturn(NOW);
        // mock initSchemaAndUpdateTime and do nothing
        Mockito.when(tbl.initSchema(Mockito.any(ExternalSchemaCache.SchemaCacheKey.class)))
                .thenReturn(Optional.empty());

        setField(tbl2, "objectCreated", true);
        setField(tbl2, "schemaUpdateTime", NOW);
        setField(tbl2, "eventUpdateTime", 0);
        setField(tbl2, "catalog", hmsCatalog);
        setField(tbl2, "dbName", "hms_db");
        setField(tbl2, "name", "hms_tbl2");
        setField(tbl2, "dlaTable", new HiveDlaTable(tbl2));
        setField(tbl2, "dlaType", DLAType.HIVE);

        Mockito.when(tbl2.getId()).thenReturn(10004L);
        Mockito.when(tbl2.getName()).thenReturn("hms_tbl2");
        Mockito.when(tbl2.getDbName()).thenReturn("hms_db");
        Mockito.when(tbl2.getFullSchema()).thenReturn(schema);
        Mockito.when(tbl2.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(tbl2.isView()).thenReturn(false);
        Mockito.when(tbl2.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(tbl2.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(tbl2.getDatabase()).thenReturn(db);
        Mockito.when(tbl2.getSupportedSysTables()).thenReturn(SupportedSysTables.HIVE_SUPPORTED_SYS_TABLES);
        Mockito.when(tbl2.getUpdateTime()).thenReturn(NOW);
        Mockito.when(tbl2.getSchemaUpdateTime()).thenReturn(NOW);
        // mock initSchemaAndUpdateTime and do nothing
        Mockito.when(tbl2.initSchemaAndUpdateTime(Mockito.any(ExternalSchemaCache.SchemaCacheKey.class)))
                .thenReturn(Optional.empty());
        Mockito.doNothing().when(tbl2).setEventUpdateTime(Mockito.anyLong());

        setField(view1, "objectCreated", true);

        Mockito.when(view1.getId()).thenReturn(10002L);
        Mockito.when(view1.getName()).thenReturn("hms_view1");
        Mockito.when(view1.getDbName()).thenReturn("hms_db");
        Mockito.when(view1.isView()).thenReturn(true);
        Mockito.when(view1.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(view1.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(view1.getFullSchema()).thenReturn(schema);
        Mockito.when(view1.getViewText()).thenReturn("SELECT * FROM hms_db.hms_tbl");
        Mockito.when(view1.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(view1.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(view1.getUpdateTime()).thenReturn(NOW);
        Mockito.when(view1.getDatabase()).thenReturn(db);
        Mockito.when(view1.getSupportedSysTables()).thenReturn(SupportedSysTables.HIVE_SUPPORTED_SYS_TABLES);

        setField(view2, "objectCreated", true);

        Mockito.when(view2.getId()).thenReturn(10003L);
        Mockito.when(view2.getName()).thenReturn("hms_view2");
        Mockito.when(view2.getDbName()).thenReturn("hms_db");
        Mockito.when(view2.isView()).thenReturn(true);
        Mockito.when(view2.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(view2.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(view2.getFullSchema()).thenReturn(schema);
        Mockito.when(view2.getViewText()).thenReturn("SELECT * FROM hms_db.hms_view1");
        Mockito.when(view2.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(view2.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(view2.getUpdateTime()).thenReturn(NOW);
        Mockito.when(view2.getDatabase()).thenReturn(db);
        Mockito.when(view2.getSupportedSysTables()).thenReturn(SupportedSysTables.HIVE_SUPPORTED_SYS_TABLES);

        db.addTableForTest(tbl);
        db.addTableForTest(tbl2);
        db.addTableForTest(view1);
        db.addTableForTest(view2);
        hmsCatalog.addDatabaseForTest(db);

        Mockito.when(hiveScanNode1.getTargetTable()).thenReturn(tbl);
        Mockito.when(hiveScanNode2.getTargetTable()).thenReturn(view1);
        Mockito.when(hiveScanNode3.getTargetTable()).thenReturn(view2);
        Mockito.when(hiveScanNode4.getTargetTable()).thenReturn(tbl2);

        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        desc.setTable(mgr.getInternalCatalog().getDbNullable("test").getTableNullable("tbl1"));
        olapScanNode = new OlapScanNode(new PlanNodeId(1), desc, "tb1ScanNode");
    }

    @Test
    public void testHitSqlCacheByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(CacheAnalyzer.CacheMode.Sql, ca.getCacheMode());
        SqlCache sqlCache = (SqlCache) ca.getCache();
        Assert.assertEquals(NOW, sqlCache.getLatestTime());
    }

    @Test
    public void testHitSqlCacheByNereidsAfterPartitionUpdateTimeChanged() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl2", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode4);

        // invoke initSchemaAndUpdateTime first and init schemaUpdateTime
        tbl2.initSchemaAndUpdateTime(new ExternalSchemaCache.SchemaCacheKey(tbl2.getOrBuildNameMapping()));

        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(CacheAnalyzer.CacheMode.Sql, ca.getCacheMode());
        SqlCache sqlCache1 = (SqlCache) ca.getCache();

        // latestTime is equals to the schema update time if not set partition update time
        Assert.assertEquals(tbl2.getSchemaUpdateTime(), sqlCache1.getLatestTime());

        // wait a second and set partition update time
        try {
            Thread.sleep(1000);
        } catch (Throwable throwable) {
            // do nothing
        }
        long later = System.currentTimeMillis();
        Mockito.when(tbl2.getUpdateTime()).thenReturn(later);

        // check cache mode again
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        SqlCache sqlCache2 = (SqlCache) ca.getCache();
        Assert.assertEquals(CacheAnalyzer.CacheMode.Sql, ca.getCacheMode());

        // the latest time will be changed and is equals to the partition update time
        Assert.assertEquals(later, sqlCache2.getLatestTime());
        Assert.assertTrue(sqlCache2.getLatestTime() > sqlCache1.getLatestTime());
    }

    @Test
    public void testHitSqlCacheWithHiveViewByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_view1", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode2);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(CacheAnalyzer.CacheMode.Sql, ca.getCacheMode());
        SqlCache sqlCache = (SqlCache) ca.getCache();
        Assert.assertEquals(NOW, sqlCache.getLatestTime());
    }

    @Test
    public void testHitSqlCacheWithNestedHiveViewByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_view2", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode3);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(CacheAnalyzer.CacheMode.Sql, ca.getCacheMode());
        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals("select * from hms_ctl.hms_db.hms_view2"
                    + "|SELECT * FROM hms_db.hms_tbl|SELECT * FROM hms_db.hms_view1", cacheKey);
        Assert.assertEquals(NOW, sqlCache.getLatestTime());
    }

    @Test
    public void testNotHitSqlCacheByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1);

        CacheAnalyzer ca2 = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca2.checkCacheModeForNereids(0);
        long latestPartitionTime = ca2.getLatestTable().latestPartitionTime;

        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(latestPartitionTime);
        Assert.assertEquals(CacheAnalyzer.CacheMode.None, ca.getCacheMode());
    }

    @Test
    public void testNotHitSqlCacheWithFederatedQueryByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        // cache mode is None if this query is a federated query
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl "
                + "inner join internal.test.tbl1", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1, olapScanNode);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(CacheAnalyzer.CacheMode.None, ca.getCacheMode());
    }
}
