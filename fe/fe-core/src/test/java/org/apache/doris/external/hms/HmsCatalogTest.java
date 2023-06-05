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

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

public class HmsCatalogTest extends TestWithFeService {
    private static final String HMS_CATALOG = "hms_ctl";
    private Env env;
    private CatalogMgr mgr;
    private ConnectContext rootCtx;

    @Mocked
    private HMSExternalTable tbl;
    @Mocked
    private HMSExternalTable view1;
    @Mocked
    private HMSExternalTable view2;
    @Mocked
    private HMSExternalTable view3;
    @Mocked
    private HMSExternalTable view4;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_query_hive_views = true;
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        rootCtx.setEnv(env);
        mgr = env.getCatalogMgr();

        // create hms catalog
        CreateCatalogStmt hmsCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hms_ctl properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        mgr.createCatalog(hmsCatalog);

        // create inner db and tbl for test
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt("create database test", rootCtx);
        mgr.getInternalCatalog().createDb(createDbStmt);

        CreateTableStmt createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt("create table test.tbl1(\n"
                + "k1 int comment 'test column k1', "
                + "k2 int comment 'test column k2')  comment 'test table1' "
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
        mgr.getInternalCatalog().createTable(createTableStmt);
    }

    private void createDbAndTableForHmsCatalog(HMSExternalCatalog hmsCatalog) {
        Deencapsulation.setField(hmsCatalog, "initialized", true);
        Deencapsulation.setField(hmsCatalog, "objectCreated", true);

        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", PrimitiveType.INT));

        HMSExternalDatabase db = new HMSExternalDatabase(hmsCatalog, 10000, "hms_db");
        Deencapsulation.setField(db, "initialized", true);

        Deencapsulation.setField(tbl, "objectCreated", true);
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
            }
        };

        Deencapsulation.setField(view1, "objectCreated", true);

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
            }
        };

        Deencapsulation.setField(view2, "objectCreated", true);
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

                view2.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                view2.getFullSchema();
                minTimes = 0;
                result = schema;

                view2.getViewText();
                minTimes = 0;
                result = "SELECT * FROM (SELECT * FROM hms_db.hms_view1) t1";

                view2.isSupportedHmsTable();
                minTimes = 0;
                result = true;
            }
        };

        Deencapsulation.setField(view3, "objectCreated", true);
        new Expectations(view3) {
            {

                view3.getId();
                minTimes = 0;
                result = 10004;

                view3.getName();
                minTimes = 0;
                result = "hms_view3";

                view3.getDbName();
                minTimes = 0;
                result = "hms_db";

                view3.isView();
                minTimes = 0;
                result = true;

                view3.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                view3.getFullSchema();
                minTimes = 0;
                result = schema;

                view3.getViewText();
                minTimes = 0;
                result = "SELECT * FROM hms_db.hms_view1 UNION ALL SELECT * FROM hms_db.hms_view2";

                view3.isSupportedHmsTable();
                minTimes = 0;
                result = true;
            }
        };

        Deencapsulation.setField(view4, "objectCreated", true);
        new Expectations(view4) {
            {

                view4.getId();
                minTimes = 0;
                result = 10005;

                view4.getName();
                minTimes = 0;
                result = "hms_view4";

                view4.getDbName();
                minTimes = 0;
                result = "hms_db";

                view4.isView();
                minTimes = 0;
                result = true;

                view4.getType();
                minTimes = 0;
                result = TableIf.TableType.HMS_EXTERNAL_TABLE;

                view4.getFullSchema();
                minTimes = 0;
                result = schema;

                view4.getViewText();
                minTimes = 0;
                result = "SELECT not_exists_func(k1) FROM hms_db.hms_tbl";

                view4.isSupportedHmsTable();
                minTimes = 0;
                result = true;
            }
        };

        db.addTableForTest(tbl);
        db.addTableForTest(view1);
        db.addTableForTest(view2);
        db.addTableForTest(view3);
        db.addTableForTest(view4);
        hmsCatalog.addDatabaseForTest(db);
    }


    @Test
    public void testQueryView() {
        createDbAndTableForHmsCatalog((HMSExternalCatalog) env.getCatalogMgr().getCatalog(HMS_CATALOG));
        // test normal table
        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_tbl", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // test simple view
        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_view1", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // test view with subquery
        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_view2", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // test view with union
        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_view3", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // test view with not support func
        AnalysisException e = Assert.assertThrows(AnalysisException.class,
                () -> parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_view4", rootCtx));
        Assert.assertTrue(e.getMessage().contains("No matching function with signature: not_exists_func"));

        // change to hms_ctl
        try {
            env.changeCatalog(rootCtx, HMS_CATALOG);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // test in hms_ctl
        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_db.hms_view1", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_db.hms_view2", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_db.hms_view3", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        Assert.assertThrows(AnalysisException.class,
                () -> parseAndAnalyzeStmt("SELECT * FROM hms_db.hms_view4", rootCtx));

        // test federated query
        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_db.hms_view3, internal.test.tbl1", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        // change to internal catalog
        try {
            env.changeCatalog(rootCtx, InternalCatalog.INTERNAL_CATALOG_NAME);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_view3, internal.test.tbl1", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

        try {
            parseAndAnalyzeStmt("SELECT * FROM hms_ctl.hms_db.hms_view3, test.tbl1", rootCtx);
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }
    }

}
