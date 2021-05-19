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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mockit.Expectations;
import mockit.Injectable;

public class InsertStmtTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = '1');";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db").useDatabase("db");
        dorisAssert.withTable(createTblStmtStr);

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
    }

    List<Column> getBaseSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column k2 = new Column("k2", ScalarType.createVarchar(25));
        k2.setIsKey(true);
        k2.setIsAllowNull(true);
        columns.add(k2);

        Column v1 = new Column("v1", PrimitiveType.BIGINT);
        v1.setIsKey(false);
        v1.setIsAllowNull(true);
        v1.setAggregationType(AggregateType.SUM, false);

        columns.add(v1);

        Column v2 = new Column("v2", ScalarType.createVarchar(25));
        v2.setIsKey(false);
        v2.setAggregationType(AggregateType.REPLACE, false);
        v2.setIsAllowNull(false);
        columns.add(v2);

        return columns;
    }

    List<Column> getFullSchema() throws Exception {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column k2 = new Column("k2", ScalarType.createVarchar(25));
        k2.setIsKey(true);
        k2.setIsAllowNull(true);
        columns.add(k2);

        Column v1 = new Column("v1", PrimitiveType.BIGINT);
        v1.setIsKey(false);
        v1.setIsAllowNull(true);
        v1.setAggregationType(AggregateType.SUM, false);

        columns.add(v1);

        Column v2 = new Column("v2", ScalarType.createVarchar(25));
        v2.setIsKey(false);
        v2.setAggregationType(AggregateType.REPLACE, false);
        v2.setIsAllowNull(false);
        columns.add(v2);

        Column v3 = new Column(CreateMaterializedViewStmt.mvColumnBuilder("bitmap_union", "k1"),
                PrimitiveType.BITMAP);
        v3.setIsKey(false);
        v3.setAggregationType(AggregateType.BITMAP_UNION, false);
        v3.setIsAllowNull(false);
        ArrayList<Expr> params = new ArrayList<>();

        SlotRef slotRef = new SlotRef(null , "k1");
        slotRef.setType(Type.BIGINT);
        params.add(slotRef.uncheckedCastTo(Type.VARCHAR));

        Expr defineExpr = new FunctionCallExpr("to_bitmap", params);
        v3.setDefineExpr(defineExpr);
        columns.add(v3);

        Column v4 = new Column(CreateMaterializedViewStmt.mvColumnBuilder("hll_union", "k2"), PrimitiveType.HLL);
        v4.setIsKey(false);
        v4.setAggregationType(AggregateType.HLL_UNION, false);
        v4.setIsAllowNull(false);
        params = new ArrayList<>();
        params.add(new SlotRef(null, "k2"));
        defineExpr = new FunctionCallExpr("hll_hash", params);
        v4.setDefineExpr(defineExpr);
        columns.add(v4);

        return columns;
    }


    @Injectable InsertTarget target;
    @Injectable InsertSource source;
    @Injectable Table targetTable;

    @Test
    public void testNormal() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "values(1,'a',2,'b')";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(sql);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        statementBase.analyze(analyzer);

        QueryStmt queryStmt = (QueryStmt) statementBase;

        new Expectations() {{
            targetTable.getBaseSchema(); result = getBaseSchema();
            targetTable.getBaseSchema(anyBoolean); result = getBaseSchema();
            targetTable.getFullSchema(); result = getFullSchema();
        }};


        InsertStmt stmt = new InsertStmt(target, "label", null, source, new ArrayList<>());
        stmt.setTargetTable(targetTable);
        stmt.setQueryStmt(queryStmt);

        Deencapsulation.invoke(stmt, "analyzeSubquery", analyzer);
        System.out.println(stmt.getQueryStmt());

        QueryStmt queryStmtSubstitute = stmt.getQueryStmt();
        Assert.assertEquals(6, queryStmtSubstitute.getResultExprs().size());

        Assert.assertTrue(queryStmtSubstitute.getResultExprs().get(4) instanceof FunctionCallExpr);
        FunctionCallExpr expr4 = (FunctionCallExpr) queryStmtSubstitute.getResultExprs().get(4);
        Assert.assertTrue(expr4.getFnName().getFunction().equals("to_bitmap"));
        List<Expr> slots = Lists.newArrayList();
        expr4.collect(IntLiteral.class, slots);
        Assert.assertEquals(1, slots.size());
        Assert.assertEquals(queryStmtSubstitute.getResultExprs().get(0), slots.get(0));

        Assert.assertTrue(queryStmtSubstitute.getResultExprs().get(5) instanceof FunctionCallExpr);
        FunctionCallExpr expr5 = (FunctionCallExpr) queryStmtSubstitute.getResultExprs().get(5);
        Assert.assertTrue(expr5.getFnName().getFunction().equals("hll_hash"));
        slots = Lists.newArrayList();
        expr5.collect(StringLiteral.class, slots);
        Assert.assertEquals(1, slots.size());
        Assert.assertEquals(queryStmtSubstitute.getResultExprs().get(1), slots.get(0));
    }

    @Test
    public void testInsertSelect() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "select kk1, kk2, kk3, kk4 from db.tbl";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(sql);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        statementBase.analyze(analyzer);

        QueryStmt queryStmt = (QueryStmt) statementBase;

        new Expectations() {{
            targetTable.getBaseSchema(); result = getBaseSchema();
            targetTable.getBaseSchema(anyBoolean); result = getBaseSchema();
            targetTable.getFullSchema(); result = getFullSchema();
        }};


        InsertStmt stmt = new InsertStmt(target, "label", null, source, new ArrayList<>());
        stmt.setTargetTable(targetTable);
        stmt.setQueryStmt(queryStmt);

        Deencapsulation.invoke(stmt, "analyzeSubquery", analyzer);
        System.out.println(stmt.getQueryStmt());

        QueryStmt queryStmtSubstitue = stmt.getQueryStmt();
        Assert.assertEquals(6, queryStmtSubstitue.getResultExprs().size());

        Assert.assertTrue(queryStmtSubstitue.getResultExprs().get(4) instanceof FunctionCallExpr);
        FunctionCallExpr expr4 = (FunctionCallExpr) queryStmtSubstitue.getResultExprs().get(4);
        Assert.assertTrue(expr4.getFnName().getFunction().equals("to_bitmap"));
        List<Expr> slots = Lists.newArrayList();
        expr4.collect(SlotRef.class, slots);
        Assert.assertEquals(1, slots.size());
        Assert.assertEquals(queryStmtSubstitue.getResultExprs().get(0), slots.get(0));

        Assert.assertTrue(queryStmtSubstitue.getResultExprs().get(5) instanceof FunctionCallExpr);
        FunctionCallExpr expr5 = (FunctionCallExpr) queryStmtSubstitue.getResultExprs().get(5);
        Assert.assertTrue(expr5.getFnName().getFunction().equals("hll_hash"));
        slots = Lists.newArrayList();
        expr5.collect(SlotRef.class, slots);
        Assert.assertEquals(1, slots.size());
        Assert.assertEquals(queryStmtSubstitue.getResultExprs().get(1), slots.get(0));
    }
}
