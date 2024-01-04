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

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable.DLAType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.SqlCache;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class HmsQueryCacheTest extends AnalyzeCheckTestBase {
    private static final String HMS_CATALOG = "hms_ctl";
    private static final long NOW = System.currentTimeMillis();
    private Env env;
    private CatalogMgr mgr;
    private OlapScanNode olapScanNode;

    @Mocked
    private HMSExternalTable tbl;
    @Mocked
    private HMSExternalTable tbl2;
    @Mocked
    private HMSExternalTable view1;
    @Mocked
    private HMSExternalTable view2;
    @Mocked
    private HiveScanNode hiveScanNode1;
    @Mocked
    private HiveScanNode hiveScanNode2;
    @Mocked
    private HiveScanNode hiveScanNode3;
    @Mocked
    private HiveScanNode hiveScanNode4;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_query_hive_views = true;
        Config.cache_enable_sql_mode = true;
        Config.cache_enable_partition_mode = true;
        connectContext.getSessionVariable().setEnableSqlCache(true);

        env = Env.getCurrentEnv();
        connectContext.setEnv(env);
        mgr = env.getCatalogMgr();

        // create hms catalog
        CreateCatalogStmt hmsCatalogStmt = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hms_ctl properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                connectContext);
        mgr.createCatalog(hmsCatalogStmt);

        // create inner db and tbl for test
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt("create database test", connectContext);
        mgr.getInternalCatalog().createDb(createDbStmt);

        CreateTableStmt createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt("create table test.tbl1(\n"
                + "k1 int comment 'test column k1', "
                + "k2 int comment 'test column k2')  comment 'test table1' "
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        mgr.getInternalCatalog().createTable(createTableStmt);
    }

    private void init(HMSExternalCatalog hmsCatalog) {
        Deencapsulation.setField(hmsCatalog, "initialized", true);
        Deencapsulation.setField(hmsCatalog, "objectCreated", true);

        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", PrimitiveType.INT));

        HMSExternalDatabase db = new HMSExternalDatabase(hmsCatalog, 10000, "hms_db");
        Deencapsulation.setField(db, "initialized", true);

        Deencapsulation.setField(tbl, "objectCreated", true);
        Deencapsulation.setField(tbl, "rwLock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(tbl, "schemaUpdateTime", NOW);
        Deencapsulation.setField(tbl, "eventUpdateTime", 0);
        new Expectations(tbl) {
            {
                tbl.getId();
                minTimes = 0;
                result = 10001;

                tbl.getName();
                minTimes = 0;
                result = "hms_tbl";

                tbl.getDbName();
                minTimes = 0;
                result = "hms_db";

                tbl.getFullSchema();
                minTimes = 0;
                result = schema;

                tbl.isSupportedHmsTable();
                minTimes = 0;
                result = true;

                tbl.isView();
                minTimes = 0;
                result = false;

                tbl.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                tbl.getDlaType();
                minTimes = 0;
                result = DLAType.HIVE;

                // mock initSchemaAndUpdateTime and do nothing
                tbl.initSchemaAndUpdateTime();
                minTimes = 0;
            }
        };

        Deencapsulation.setField(tbl2, "objectCreated", true);
        Deencapsulation.setField(tbl2, "rwLock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(tbl2, "schemaUpdateTime", NOW);
        Deencapsulation.setField(tbl2, "eventUpdateTime", 0);
        new Expectations(tbl2) {
            {
                tbl2.getId();
                minTimes = 0;
                result = 10004;

                tbl2.getName();
                minTimes = 0;
                result = "hms_tbl2";

                tbl2.getDbName();
                minTimes = 0;
                result = "hms_db";

                tbl2.getFullSchema();
                minTimes = 0;
                result = schema;

                tbl2.isSupportedHmsTable();
                minTimes = 0;
                result = true;

                tbl2.isView();
                minTimes = 0;
                result = false;

                tbl2.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                tbl2.getDlaType();
                minTimes = 0;
                result = DLAType.HIVE;

                // mock initSchemaAndUpdateTime and do nothing
                tbl2.initSchemaAndUpdateTime();
                minTimes = 0;
            }
        };

        Deencapsulation.setField(view1, "objectCreated", true);
        Deencapsulation.setField(view1, "rwLock", new ReentrantReadWriteLock(true));

        new Expectations(view1) {
            {
                view1.getId();
                minTimes = 0;
                result = 10002;

                view1.getName();
                minTimes = 0;
                result = "hms_view1";

                view1.getDbName();
                minTimes = 0;
                result = "hms_db";

                view1.isView();
                minTimes = 0;
                result = true;

                view1.getCatalog();
                minTimes = 0;
                result = hmsCatalog;

                view1.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                view1.getFullSchema();
                minTimes = 0;
                result = schema;

                view1.getViewText();
                minTimes = 0;
                result = "SELECT * FROM hms_db.hms_tbl";

                view1.isSupportedHmsTable();
                minTimes = 0;
                result = true;

                view1.getDlaType();
                minTimes = 0;
                result = DLAType.HIVE;

                view1.getUpdateTime();
                minTimes = 0;
                result = NOW;
            }
        };

        Deencapsulation.setField(view2, "objectCreated", true);
        Deencapsulation.setField(view2, "rwLock", new ReentrantReadWriteLock(true));
        new Expectations(view2) {
            {
                view2.getId();
                minTimes = 0;
                result = 10003;

                view2.getName();
                minTimes = 0;
                result = "hms_view2";

                view2.getDbName();
                minTimes = 0;
                result = "hms_db";

                view2.isView();
                minTimes = 0;
                result = true;

                view2.getCatalog();
                minTimes = 0;
                result = hmsCatalog;

                view2.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                view2.getFullSchema();
                minTimes = 0;
                result = schema;

                view2.getViewText();
                minTimes = 0;
                result = "SELECT * FROM hms_db.hms_view1";

                view2.isSupportedHmsTable();
                minTimes = 0;
                result = true;

                view2.getDlaType();
                minTimes = 0;
                result = DLAType.HIVE;

                view2.getUpdateTime();
                minTimes = 0;
                result = NOW;
            }
        };

        db.addTableForTest(tbl);
        db.addTableForTest(tbl2);
        db.addTableForTest(view1);
        db.addTableForTest(view2);
        hmsCatalog.addDatabaseForTest(db);

        new Expectations(hiveScanNode1) {
            {
                hiveScanNode1.getTargetTable();
                minTimes = 0;
                result = tbl;
            }
        };

        new Expectations(hiveScanNode2) {
            {
                hiveScanNode2.getTargetTable();
                minTimes = 0;
                result = view1;
            }
        };

        new Expectations(hiveScanNode3) {
            {
                hiveScanNode3.getTargetTable();
                minTimes = 0;
                result = view2;
            }
        };

        new Expectations(hiveScanNode4) {
            {
                hiveScanNode4.getTargetTable();
                minTimes = 0;
                result = tbl2;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        desc.setTable(mgr.getInternalCatalog().getDbNullable("test").getTableNullable("tbl1"));
        olapScanNode = new OlapScanNode(new PlanNodeId(1), desc, "tb1ScanNode");
    }

    @Test
    public void testHitSqlCache() throws Exception {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = parseAndAnalyzeStmt("select * from hms_ctl.hms_db.hms_tbl", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheMode(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        Assert.assertEquals(sqlCache.getLatestTime(), NOW);
    }

    @Test
    public void testHitSqlCacheAfterPartitionUpdateTimeChanged() throws Exception {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = parseAndAnalyzeStmt("select * from hms_ctl.hms_db.hms_tbl2", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode4);

        // invoke initSchemaAndUpdateTime first and init schemaUpdateTime
        tbl2.initSchemaAndUpdateTime();

        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheMode(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache1 = (SqlCache) ca.getCache();

        // latestTime is equals to the schema update time if not set partition update time
        Assert.assertEquals(sqlCache1.getLatestTime(), tbl2.getSchemaUpdateTime());

        // wait a second and set partition update time
        try {
            Thread.sleep(1000);
        } catch (Throwable throwable) {
            // do nothing
        }
        long later = System.currentTimeMillis();
        tbl2.setEventUpdateTime(later);

        // check cache mode again
        ca.checkCacheMode(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        SqlCache sqlCache2 = (SqlCache) ca.getCache();
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);

        // the latest time will be changed and is equals to the partition update time
        Assert.assertEquals(later, sqlCache2.getLatestTime());
        Assert.assertTrue(sqlCache2.getLatestTime() > sqlCache1.getLatestTime());
    }

    @Test
    public void testHitSqlCacheByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        Assert.assertEquals(sqlCache.getLatestTime(), NOW);
    }

    @Test
    public void testHitSqlCacheByNereidsAfterPartitionUpdateTimeChanged() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl2", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode4);

        // invoke initSchemaAndUpdateTime first and init schemaUpdateTime
        tbl2.initSchemaAndUpdateTime();

        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache1 = (SqlCache) ca.getCache();

        // latestTime is equals to the schema update time if not set partition update time
        Assert.assertEquals(sqlCache1.getLatestTime(), tbl2.getSchemaUpdateTime());

        // wait a second and set partition update time
        try {
            Thread.sleep(1000);
        } catch (Throwable throwable) {
            // do nothing
        }
        long later = System.currentTimeMillis();
        tbl2.setEventUpdateTime(later);

        // check cache mode again
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        SqlCache sqlCache2 = (SqlCache) ca.getCache();
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);

        // the latest time will be changed and is equals to the partition update time
        Assert.assertEquals(later, sqlCache2.getLatestTime());
        Assert.assertTrue(sqlCache2.getLatestTime() > sqlCache1.getLatestTime());
    }

    @Test
    public void testHitSqlCacheWithHiveView() throws Exception {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = parseAndAnalyzeStmt("select * from hms_ctl.hms_db.hms_view1", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode2);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheMode(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        Assert.assertEquals(sqlCache.getLatestTime(), NOW);
    }

    @Test
    public void testHitSqlCacheWithHiveViewByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_view1", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode2);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        Assert.assertEquals(sqlCache.getLatestTime(), NOW);
    }

    @Test
    public void testHitSqlCacheWithNestedHiveView() throws Exception {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = parseAndAnalyzeStmt("select * from hms_ctl.hms_db.hms_view2", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode3);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheMode(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "SELECT `hms_ctl`.`hms_db`.`hms_view2`.`k1` AS `k1` "
                    + "FROM `hms_ctl`.`hms_db`.`hms_view2`"
                    + "|SELECT * FROM hms_db.hms_tbl|SELECT * FROM hms_db.hms_view1");
        Assert.assertEquals(sqlCache.getLatestTime(), NOW);
    }

    @Test
    public void testHitSqlCacheWithNestedHiveViewByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_view2", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode3);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.Sql);
        SqlCache sqlCache = (SqlCache) ca.getCache();
        String cacheKey = sqlCache.getSqlWithViewStmt();
        Assert.assertEquals(cacheKey, "select * from hms_ctl.hms_db.hms_view2"
                    + "|SELECT * FROM hms_db.hms_tbl|SELECT * FROM hms_db.hms_view1");
        Assert.assertEquals(sqlCache.getLatestTime(), NOW);
    }

    @Test
    public void testNotHitSqlCache() throws Exception {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = parseAndAnalyzeStmt("select * from hms_ctl.hms_db.hms_tbl", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheMode(0);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.None);
    }

    @Test
    public void testNotHitSqlCacheByNereids() {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        StatementBase parseStmt = analyzeAndGetStmtByNereids("select * from hms_ctl.hms_db.hms_tbl", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheModeForNereids(0);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.None);
    }

    @Test
    public void testNotHitSqlCacheWithFederatedQuery() throws Exception {
        init((HMSExternalCatalog) mgr.getCatalog(HMS_CATALOG));
        // cache mode is None if this query is a federated query
        StatementBase parseStmt = parseAndAnalyzeStmt("select * from hms_ctl.hms_db.hms_tbl "
                + "inner join internal.test.tbl1", connectContext);
        List<ScanNode> scanNodes = Arrays.asList(hiveScanNode1, olapScanNode);
        CacheAnalyzer ca = new CacheAnalyzer(connectContext, parseStmt, scanNodes);
        ca.checkCacheMode(System.currentTimeMillis() + Config.cache_last_version_interval_second * 1000L * 2);
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.None);
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
        Assert.assertEquals(ca.getCacheMode(), CacheAnalyzer.CacheMode.None);
    }
}
