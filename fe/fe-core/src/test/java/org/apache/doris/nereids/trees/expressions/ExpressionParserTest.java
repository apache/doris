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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.TreeNode;

import org.junit.Test;

public class ExpressionParserTest {
    private static final NereidsParser PARSER = new NereidsParser();

    private void assertSql(String sql) throws Exception {
        TreeNode treeNode = PARSER.parseSingle(sql);
        System.out.println(treeNode.toString());
    }

    private void assertExpr(String expr) {
        Expression expression = PARSER.parseExpression(expr);
        System.out.println(expression.toSql());
    }

    @Test
    public void testSqlBetweenPredicate() throws Exception {
        String sql = "select * from test1 where d1 between 1 and 2";
        assertSql(sql);
    }

    @Test
    public void testExprBetweenPredicate() {
        String sql = "c BETWEEN a AND b";
        assertExpr(sql);
    }

    @Test
    public void testSqlAnd() throws Exception {
        String sql = "select * from test1 where a > 1 and b > 1";
        TreeNode treeNode = PARSER.parseSingle(sql);
        System.out.println(treeNode);
    }

    @Test
    public void testExprAnd() {
        String expr = "a AND b";
        assertExpr(expr);
    }

    @Test
    public void testExprMultiAnd() {
        String expr = "a AND b AND c AND d";
        assertExpr(expr);
    }

    @Test
    public void testExprOr() {
        String expr = "a OR b";
        assertExpr(expr);
    }

    @Test
    public void testExprMultiOr() {
        String expr = "a OR b OR c OR d";
        assertExpr(expr);
    }

    @Test
    public void testExprArithmetic() {
        String multiply = "1 * 2";
        assertExpr(multiply);

        String divide = "3 / 2";
        assertExpr(divide);

        String mod = "5 % 3";
        assertExpr(mod);

        String add = "3 + 3";
        assertExpr(add);

        String subtract = "3 - 2";
        assertExpr(subtract);
    }

    @Test
    public void testSqlFunction() throws Exception {
        String sum = "select sum(a) from test1";
        assertSql(sum);

        String sumWithAs = "select sum(a) as b from test1";
        assertSql(sumWithAs);

        String sumAndAvg = "select sum(a),avg(b) from test1";
        assertSql(sumAndAvg);
    }

    @Test
    public void testGroupByClause() throws Exception {

        String groupBy = "select a from test group by a";
        assertSql(groupBy);

        String groupByWithFun1 = "select sum(a), b from test1 group by b";
        assertSql(groupByWithFun1);


        String groupByWithFun2 = "select sum(a), b, c+1 from test1 group by b, c";
        assertSql(groupByWithFun2);

        String groupBySum = "select k1+k2 from test group by k1+k2";
        assertSql(groupBySum);
    }

    @Test
    public void testSortClause() throws Exception {

        String sort = "select a from test order by c, d";
        assertSql(sort);

        String sort1 = "select a from test order by 1";
        assertSql(sort1);
    }
}
