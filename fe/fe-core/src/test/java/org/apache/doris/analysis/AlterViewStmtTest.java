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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.View;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class AlterViewStmtTest {
    private Analyzer analyzer;

    private Catalog catalog;

    @Mocked
    EditLog editLog;

    @Mocked
    private ConnectContext connectContext;

    @Mocked
    private PaloAuth auth;

    @Before
    public void setUp() {
        catalog = Deencapsulation.newInstance(Catalog.class);
        analyzer = new Analyzer(catalog, connectContext);


        Database db = new Database(50000L, "testCluster:testDb");

        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);

        List<Column> baseSchema = new LinkedList<Column>();
        baseSchema.add(column1);
        baseSchema.add(column2);

        OlapTable table = new OlapTable(30000, "testTbl",
                baseSchema, KeysType.AGG_KEYS, new SinglePartitionInfo(), null);
        db.createTable(table);


        new Expectations(auth) {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(editLog) {
            {
                editLog.logCreateTable((CreateTableInfo) any);
                minTimes = 0;

                editLog.logModifyViewDef((AlterViewInfo) any);
                minTimes = 0;
            }
        };

        Deencapsulation.setField(catalog, "editLog", editLog);

        new MockUp<Catalog>() {
            @Mock
            Catalog getCurrentCatalog() {
                return catalog;
            }
            @Mock
            PaloAuth getAuth() {
                return auth;
            }
            @Mock
            Database getDbOrDdlException(long dbId) {
                return db;
            }
            @Mock
            Database getDbOrDdlException(String dbName) {
                return db;
            }
            @Mock
            Database getDbOrAnalysisException(long dbId) {
                return db;
            }
            @Mock
            Database getDbOrAnalysisException(String dbName) {
                return db;
            }
        };

        new MockUp<Analyzer>() {
            @Mock
            String getClusterName() {
                return "testCluster";
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        String originStmt = "select col1 as c1, sum(col2) as c2 from testDb.testTbl group by col1";
        View view = new View(30000L, "testView", null);
        view.setInlineViewDefWithSqlMode("select col1 as c1, sum(col2) as c2 from testDb.testTbl group by col1", 0L);
        view.init();

        Database db = analyzer.getCatalog().getDbOrAnalysisException("testDb");
        db.createTable(view);

        Assert.assertEquals(originStmt, view.getInlineViewDef());

        String alterStmt = "with testTbl_cte (w1, w2) as (select col1, col2 from testDb.testTbl) select w1 as c1, sum(w2) as c2 from testTbl_cte where w1 > 10 group by w1 order by w1";
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(alterStmt)));
        QueryStmt alterQueryStmt = (QueryStmt) SqlParserUtils.getFirstStmt(parser);

        ColWithComment col1 = new ColWithComment("h1", null);
        ColWithComment col2 = new ColWithComment("h2", null);

        AlterViewStmt alterViewStmt = new AlterViewStmt(new TableName("testDb", "testView"), Lists.newArrayList(col1, col2), alterQueryStmt);
        alterViewStmt.analyze(analyzer);
        Catalog catalog1 = analyzer.getCatalog();
        if (catalog1 == null) {
            System.out.println("cmy get null");
            return;
        }
        catalog1.alterView(alterViewStmt);

        View newView = (View) db.getTableOrAnalysisException("testView");
        Assert.assertEquals("WITH testTbl_cte(w1, w2) AS (SELECT `col1` AS `col1`, `col2` AS `col2` FROM `testCluster:testDb`.`testTbl`)" +
                        " SELECT `w1` AS `h1`, sum(`w2`) AS `h2` FROM `testTbl_cte` WHERE `w1` > 10 GROUP BY `w1` ORDER BY `w1`",
                newView.getInlineViewDef());
    }
}
