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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class QueryStmtTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_batch_delete_by_default = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createBaseAllStmtStr = "create table db1.baseall(k1 int, k2 varchar(32)) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        String tbl1 = "CREATE TABLE db1.table1 (\n" +
                "  `siteid` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `citycode` smallint(6) NULL COMMENT \"\",\n" +
                "  `username` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`siteid`, `citycode`, `username`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`siteid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(createTblStmtStr)
                .withTable(createBaseAllStmtStr)
                .withTable(tbl1);
    }

    @Test
    public void testCollectExprs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "SELECT CASE\n" +
                "        WHEN (\n" +
                "            SELECT COUNT(*) / 2\n" +
                "            FROM db1.tbl1\n" +
                "        ) > k4 THEN (\n" +
                "            SELECT AVG(k4)\n" +
                "            FROM db1.tbl1\n" +
                "        )\n" +
                "        ELSE (\n" +
                "            SELECT SUM(k4)\n" +
                "            FROM db1.tbl1\n" +
                "        )\n" +
                "    END AS kk4\n" +
                "FROM db1.tbl1;";
        QueryStmt stmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Map<String, Expr> exprsMap = new HashMap<>();
        stmt.collectExprs(exprsMap);
        Assert.assertEquals(4, exprsMap.size());

        sql = "SELECT username\n" +
                "FROM db1.table1\n" +
                "WHERE siteid in\n" +
                "    (SELECT abs(5+abs(0))+1)\n" +
                "UNION\n" +
                "SELECT CASE\n" +
                "           WHEN\n" +
                "                  (SELECT count(*)+abs(8)\n" +
                "                   FROM db1.table1\n" +
                "                   WHERE username='helen')>1 THEN 888\n" +
                "           ELSE 999\n" +
                "       END AS ccc\n" +
                "FROM\n" +
                "  (SELECT curdate()) a;";
        stmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        exprsMap.clear();
        stmt.collectExprs(exprsMap);
        Assert.assertEquals(6, exprsMap.size());

        sql = "select\n" +
                "   avg(t1.k4)\n" +
                "from\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2,\n" +
                "   db1.tbl1 t3,\n" +
                "   db1.tbl1 t4,\n" +
                "   db1.tbl1 t5,\n" +
                "   db1.tbl1 t6\n" +
                "where\n" +
                "   t2.k1 = t1.k1\n" +
                "   and t1.k2 = t6.k2\n" +
                "   and t6.k4 = 2001\n" +
                "   and(\n" +
                "      (\n" +
                "         t1.k2 = t4.k2\n" +
                "         and t3.k3 = t1.k3\n" +
                "         and t3.k1 = 'D'\n" +
                "         and t4.k3 = '2 yr Degree'\n" +
                "         and t1.k4 between 100.00\n" +
                "         and 150.00\n" +
                "         and t4.k4 = 3\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k2 = t4.k2\n" +
                "         and t3.k3 = t1.k3\n" +
                "         and t3.k1 = 'S'\n" +
                "         and t4.k3 = 'Secondary'\n" +
                "         and t1.k4 between 50.00\n" +
                "         and 100.00\n" +
                "         and t4.k4 = 1\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k2 = t4.k2\n" +
                "         and t3.k3 = t1.k3\n" +
                "         and t3.k1 = 'W'\n" +
                "         and t4.k3 = 'Advanced Degree'\n" +
                "         and t1.k4 between 150.00\n" +
                "         and 200.00\n" +
                "         and t4.k4  = 1\n" +
                "      )\n" +
                "   )\n" +
                "   and(\n" +
                "      (\n" +
                "         t1.k1 = t5.k1\n" +
                "         and t5.k2 = 'United States'\n" +
                "         and t5.k3  in ('CO', 'IL', 'MN')\n" +
                "         and t1.k4 between 100\n" +
                "         and 200\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k1 = t5.k1\n" +
                "         and t5.k2 = 'United States'\n" +
                "         and t5.k3 in ('OH', 'MT', 'NM')\n" +
                "         and t1.k4 between 150\n" +
                "         and 300\n" +
                "      )\n" +
                "      or (\n" +
                "         t1.k1 = t5.k1\n" +
                "         and t5.k2 = 'United States'\n" +
                "         and t5.k3 in ('TX', 'MO', 'MI')\n" +
                "         and t1.k4 between 50 and 250\n" +
                "      )\n" +
                "   );";
        stmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        exprsMap.clear();
        stmt.collectExprs(exprsMap);
        Assert.assertEquals(2, exprsMap.size());

        sql = "SELECT k1 FROM db1.baseall GROUP BY k1 HAVING EXISTS(SELECT k4 FROM db1.tbl1 GROUP BY k4 " +
                "HAVING SUM(k4) = k4);";
        stmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        exprsMap.clear();
        stmt.collectExprs(exprsMap);
        Assert.assertEquals(4, exprsMap.size());
    }

    @Test
    public void testPutBackExprs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "SELECT username, @@license, @@time_zone\n" +
                "FROM db1.table1\n" +
                "WHERE siteid in\n" +
                "    (SELECT abs(5+abs(0))+1)\n" +
                "UNION\n" +
                "SELECT CASE\n" +
                "           WHEN\n" +
                "                  (SELECT count(*)+abs(8)\n" +
                "                   FROM db1.table1\n" +
                "                   WHERE username='helen')>1 THEN 888\n" +
                "           ELSE 999\n" +
                "       END AS ccc, @@language, @@storage_engine\n" +
                "FROM\n" +
                "  (SELECT curdate()) a;";
        StatementBase stmt = UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        stmt.foldConstant(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());

        // reAnalyze
        reAnalyze(stmt, ctx);
        Assert.assertTrue(stmt.toSql().contains("Apache License, Version 2.0"));
        Assert.assertTrue(stmt.toSql().contains("/palo/share/english/"));

        // test sysVariableDescs
        sql = "SELECT\n" +
                "   avg(t1.k4)\n" +
                "FROM\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "WHERE\n" +
                "(\n" +
                "   t2.k2 = 'United States'\n" +
                "   AND t2.k3  in (@@license, @@version)\n" +
                ")\n" +
                "OR (\n" +
                "   t2.k2 = @@language\n" +
                ")";
        stmt = UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        stmt.foldConstant(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        // reAnalyze
        reAnalyze(stmt, ctx);
        Assert.assertTrue(stmt.toSql().contains("Apache License, Version 2.0"));
        Assert.assertTrue(stmt.toSql().contains("/palo/share/english/"));

        // test informationFunctions
        sql = "SELECT\n" +
                "   avg(t1.k4)\n" +
                "FROM\n" +
                "   db1.tbl1 t1,\n" +
                "   db1.tbl1 t2\n" +
                "WHERE\n" +
                "(\n" +
                "   t2.k2 = 'United States'\n" +
                "   AND t2.k1  in (USER(), CURRENT_USER(), SCHEMA())\n" +
                ")\n" +
                "OR (\n" +
                "   t2.k2 = CONNECTION_ID()\n" +
                ")";
        stmt = UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        stmt.foldConstant(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        // reAnalyze
        reAnalyze(stmt, ctx);
        Assert.assertTrue(stmt.toSql().contains("root''@''%"));
        Assert.assertTrue(stmt.toSql().contains("root''@''127.0.0.1"));

    }

    private void reAnalyze(StatementBase stmt, ConnectContext ctx) throws UserException {
        // reAnalyze
        List<Type> origResultTypes = Lists.newArrayList();
        for (Expr e: stmt.getResultExprs()) {
            origResultTypes.add(e.getType());
        }
        List<String> origColLabels =
                Lists.newArrayList(stmt.getColLabels());

        // query re-analyze
        stmt.reset();
        // Re-analyze the stmt with a new analyzer.
        stmt.analyze(new Analyzer(ctx.getCatalog(), ctx));

        // Restore the original result types and column labels.
        stmt.castResultExprs(origResultTypes);
        stmt.setColLabels(origColLabels);
    }
}
