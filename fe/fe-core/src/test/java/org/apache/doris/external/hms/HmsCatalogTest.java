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

package org.apache.doris.external.hms;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.HMSSchemaCacheValue;
import org.apache.doris.datasource.hive.HiveDlaTable;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class HmsCatalogTest extends AnalyzeCheckTestBase {
    private static final String HMS_CATALOG = "hms_ctl";
    private static final long NOW = System.currentTimeMillis();
    private Env env;
    private CatalogMgr mgr;

    private HMSExternalTable tbl = Mockito.mock(HMSExternalTable.class);
    private HMSExternalTable view1 = Mockito.mock(HMSExternalTable.class);
    private HMSExternalTable view2 = Mockito.mock(HMSExternalTable.class);
    private HMSExternalTable view3 = Mockito.mock(HMSExternalTable.class);
    private HMSExternalTable view4 = Mockito.mock(HMSExternalTable.class);

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_query_hive_views = true;
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

    private void createDbAndTableForHmsCatalog(HMSExternalCatalog hmsCatalog) {
        Deencapsulation.setField(hmsCatalog, "initialized", true);
        Deencapsulation.setField(hmsCatalog, "objectCreated", true);
        Env.getCurrentEnv().getExtMetaCacheMgr().prepareCatalog(hmsCatalog.getId());

        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", PrimitiveType.INT));
        HMSSchemaCacheValue schemaCacheValue = new HMSSchemaCacheValue(schema, Lists.newArrayList());

        HMSExternalDatabase db = new HMSExternalDatabase(hmsCatalog, 10000, "hms_db", "hms_db");
        Deencapsulation.setField(db, "initialized", true);

        Deencapsulation.setField(tbl, "objectCreated", true);
        Deencapsulation.setField(tbl, "updateTime", NOW);
        Deencapsulation.setField(tbl, "catalog", hmsCatalog);
        Deencapsulation.setField(tbl, "dbName", "hms_db");
        Deencapsulation.setField(tbl, "name", "hms_tbl");
        Deencapsulation.setField(tbl, "dlaTable", new HiveDlaTable(tbl));
        Deencapsulation.setField(tbl, "dlaType", DLAType.HIVE);
        long now = System.currentTimeMillis();
        Mockito.when(tbl.getId()).thenReturn(10001L);
        Mockito.when(tbl.getName()).thenReturn("hms_tbl");
        Mockito.when(tbl.getDbName()).thenReturn("hms_db");
        Mockito.when(tbl.getFullSchema()).thenReturn(schema);
        Mockito.when(tbl.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(tbl.isView()).thenReturn(false);
        Mockito.when(tbl.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(tbl.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(tbl.getSchemaCacheValue()).thenReturn(Optional.of(schemaCacheValue));
        Mockito.when(tbl.initSchemaAndUpdateTime(Mockito.any(SchemaCacheKey.class))).thenReturn(Optional.of(schemaCacheValue));
        Mockito.when(tbl.getDatabase()).thenReturn(db);
        Mockito.when(tbl.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(tbl.getNewestUpdateVersionOrTime()).thenReturn(now);

        Deencapsulation.setField(view1, "objectCreated", true);
        Deencapsulation.setField(view1, "updateTime", NOW);
        Deencapsulation.setField(view1, "catalog", hmsCatalog);
        Deencapsulation.setField(view1, "dbName", "hms_db");
        Deencapsulation.setField(view1, "name", "hms_view1");
        Deencapsulation.setField(view1, "dlaType", DLAType.HIVE);

        Mockito.when(view1.getId()).thenReturn(10002L);
        Mockito.when(view1.getName()).thenReturn("hms_view1");
        Mockito.when(view1.getDbName()).thenReturn("hms_db");
        Mockito.when(view1.isView()).thenReturn(true);
        Mockito.when(view1.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(view1.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(view1.getFullSchema()).thenReturn(schema);
        Mockito.when(view1.getViewText()).thenReturn("SELECT * FROM hms_db.hms_tbl");
        Mockito.when(view1.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(view1.getDatabase()).thenReturn(db);

        Deencapsulation.setField(view2, "objectCreated", true);
        Deencapsulation.setField(view2, "updateTime", NOW);
        Deencapsulation.setField(view2, "catalog", hmsCatalog);
        Deencapsulation.setField(view2, "dbName", "hms_db");
        Deencapsulation.setField(view2, "name", "hms_view2");
        Deencapsulation.setField(view2, "dlaType", DLAType.HIVE);

        Mockito.when(view2.getId()).thenReturn(10003L);
        Mockito.when(view2.getName()).thenReturn("hms_view2");
        Mockito.when(view2.getDbName()).thenReturn("hms_db");
        Mockito.when(view2.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(view2.isView()).thenReturn(true);
        Mockito.when(view2.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(view2.getFullSchema()).thenReturn(schema);
        Mockito.when(view2.getViewText()).thenReturn("SELECT * FROM (SELECT * FROM hms_db.hms_view1) t1");
        Mockito.when(view2.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(view2.getDatabase()).thenReturn(db);

        Deencapsulation.setField(view3, "objectCreated", true);
        Deencapsulation.setField(view3, "updateTime", NOW);
        Deencapsulation.setField(view3, "catalog", hmsCatalog);
        Deencapsulation.setField(view3, "dbName", "hms_db");
        Deencapsulation.setField(view3, "name", "hms_view3");
        Deencapsulation.setField(view3, "dlaType", DLAType.HIVE);

        Mockito.when(view3.getId()).thenReturn(10004L);
        Mockito.when(view3.getName()).thenReturn("hms_view3");
        Mockito.when(view3.getDbName()).thenReturn("hms_db");
        Mockito.when(view3.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(view3.isView()).thenReturn(true);
        Mockito.when(view3.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(view3.getFullSchema()).thenReturn(schema);
        Mockito.when(view3.getViewText()).thenReturn("SELECT * FROM hms_db.hms_view1 UNION ALL SELECT * FROM hms_db.hms_view2");
        Mockito.when(view3.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(view3.getDatabase()).thenReturn(db);

        Deencapsulation.setField(view4, "objectCreated", true);
        Deencapsulation.setField(view4, "updateTime", NOW);
        Deencapsulation.setField(view4, "catalog", hmsCatalog);
        Deencapsulation.setField(view4, "dbName", "hms_db");
        Deencapsulation.setField(view4, "name", "hms_view4");
        Deencapsulation.setField(view4, "dlaType", DLAType.HIVE);

        Mockito.when(view4.getId()).thenReturn(10005L);
        Mockito.when(view4.getName()).thenReturn("hms_view4");
        Mockito.when(view4.getDbName()).thenReturn("hms_db");
        Mockito.when(view4.getCatalog()).thenReturn(hmsCatalog);
        Mockito.when(view4.isView()).thenReturn(true);
        Mockito.when(view4.getType()).thenReturn(TableIf.TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(view4.getFullSchema()).thenReturn(schema);
        Mockito.when(view4.getViewText()).thenReturn("SELECT not_exists_func(k1) FROM hms_db.hms_tbl");
        Mockito.when(view4.isSupportedHmsTable()).thenReturn(true);
        Mockito.when(view4.getDatabase()).thenReturn(db);

        db.addTableForTest(tbl);
        db.addTableForTest(view1);
        db.addTableForTest(view2);
        db.addTableForTest(view3);
        db.addTableForTest(view4);
        hmsCatalog.addDatabaseForTest(db);
    }

    @Test
    public void testQueryView() {
        SessionVariable sv = connectContext.getSessionVariable();
        Assertions.assertNotNull(sv);

        createDbAndTableForHmsCatalog((HMSExternalCatalog) env.getCatalogMgr().getCatalog(HMS_CATALOG));
        // force use nereids planner to query hive views
        queryViews();
    }

    private void testParseAndAnalyze(String sql) {
        try {
            checkAnalyze(sql);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }
    }

    private void testParseAndAnalyzeWithThrows(String sql) {
        try {
            Assert.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () -> checkAnalyze(sql));
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }
    }

    private void queryViews() {
        // test normal table
        testParseAndAnalyze("SELECT * FROM hms_ctl.hms_db.hms_tbl");

        // test simple view
        testParseAndAnalyze("SELECT * FROM hms_ctl.hms_db.hms_view1");

        // test view with subquery
        testParseAndAnalyze("SELECT * FROM hms_ctl.hms_db.hms_view2");

        // test view with union
        testParseAndAnalyze("SELECT * FROM hms_ctl.hms_db.hms_view3");

        // test view with not support func
        testParseAndAnalyzeWithThrows("SELECT * FROM hms_ctl.hms_db.hms_view4");

        // change to hms_ctl
        try {
            env.changeCatalog(connectContext, HMS_CATALOG);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // test in hms_ctl
        testParseAndAnalyze("SELECT * FROM hms_db.hms_view1");

        testParseAndAnalyze("SELECT * FROM hms_db.hms_view2");

        testParseAndAnalyze("SELECT * FROM hms_db.hms_view3");

        testParseAndAnalyzeWithThrows("SELECT * FROM hms_db.hms_view4");

        // test federated query
        testParseAndAnalyze("SELECT * FROM hms_db.hms_view3, internal.test.tbl1");

        // change to internal catalog
        try {
            env.changeCatalog(connectContext, InternalCatalog.INTERNAL_CATALOG_NAME);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        testParseAndAnalyze("SELECT * FROM hms_ctl.hms_db.hms_view3, internal.test.tbl1");

        testParseAndAnalyze("SELECT * FROM hms_ctl.hms_db.hms_view3, test.tbl1");
    }

}
