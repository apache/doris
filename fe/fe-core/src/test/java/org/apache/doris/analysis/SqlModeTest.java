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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.rewrite.ExprRewriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class SqlModeTest {

    private Analyzer analyzer;

    @Test
    public void testScannerConstructor() {
        String stmt = new String("SELECT * FROM db1.tbl1 WHERE name = 'BILL GATES'");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        SelectStmt selectStmt = null;
        try {
            selectStmt = (SelectStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals("SELECT * FROM `db1`.`tbl1` WHERE (`name` = 'BILL GATES')", selectStmt.toSql());

        parser = new SqlParser(new SqlScanner(new StringReader(stmt), SqlModeHelper.MODE_DEFAULT));
        try {
            selectStmt = (SelectStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals("SELECT * FROM `db1`.`tbl1` WHERE (`name` = 'BILL GATES')", selectStmt.toSql());
    }

    @Test
    public void testPipesAsConcatMode() {
        // Mode Active
        String stmt = new String("SELECT 'a' || 'b' || 'c'");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt), SqlModeHelper.MODE_PIPES_AS_CONCAT));
        SelectStmt selectStmt = null;
        try {
            selectStmt = (SelectStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Expr expr = selectStmt.getSelectList().getItems().get(0).getExpr();
        if (!(expr instanceof FunctionCallExpr)) {
            Assert.fail("Mode not working");
        }
        Assert.assertEquals("concat('a', 'b', 'c')", expr.toSql());

        // Mode DeActive
        parser = new SqlParser(new SqlScanner(new StringReader(stmt), SqlModeHelper.MODE_DEFAULT));
        try {
            selectStmt = (SelectStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        expr = selectStmt.getSelectList().getItems().get(0).getExpr();
        if (!(expr instanceof CompoundPredicate)) {
            Assert.fail();
        }
        Assert.assertEquals("(('a' OR 'b') OR 'c')", expr.toSql());
    }

    @Test
    public void testPipesAsConcatModeNull() {
        // Mode Active
        String stmt = new String("SELECT ('10' || 'xy' > 1) + 2");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt), SqlModeHelper.MODE_PIPES_AS_CONCAT));
        SelectStmt parsedStmt = null;
        try {
            parsedStmt = (SelectStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Expr expr = parsedStmt.getSelectList().getItems().get(0).getExpr();
        if (!(expr.contains(FunctionCallExpr.class))) {
            Assert.fail("Mode not working");
        }

        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        analyzer.getContext().getSessionVariable().setEnableFoldConstantByBe(false);
        try {
            parsedStmt.analyze(analyzer);
            ExprRewriter rewriter = analyzer.getExprRewriter();
            rewriter.reset();
            parsedStmt.rewriteExprs(rewriter);

            Expr result = parsedStmt.getSelectList().getItems().get(0).getExpr();
            Assert.assertEquals(Expr.IS_NULL_LITERAL.apply(result), true);
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }
}
