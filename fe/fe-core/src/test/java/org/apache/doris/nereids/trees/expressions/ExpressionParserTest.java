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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Test;

public class ExpressionParserTest extends ParserTestBase {
    private static final NereidsParser PARSER = new NereidsParser();

    /**
     * This method is deprecated.
     * <p>
     * Please use utility functions `parsePlan `in {@link ParserTestBase}
     * to get {@link org.apache.doris.nereids.util.PlanParseChecker}.
     */
    @Deprecated
    private void assertSql(String sql) {
        PARSER.parseSingle(sql);
    }

    /**
     * This method is deprecated.
     * <p>
     * Please use utility functions `parseExpression` in {@link ParserTestBase}
     * to get {@link org.apache.doris.nereids.util.PlanParseChecker}.
     */
    @Deprecated
    private void assertExpr(String expr) {
        Expression expression = PARSER.parseExpression(expr);
        System.out.println(expression.toSql());
    }

    @Test
    void testNoBackslashEscapes() {
        parseExpression("'\\b'")
                .assertEquals(new StringLiteral("\b"));
        parseExpression("'\\n'")
                .assertEquals(new StringLiteral("\n"));
        parseExpression("'\\t'")
                .assertEquals(new StringLiteral("\t"));
        parseExpression("'\\0'")
                .assertEquals(new StringLiteral("\0"));
        ConnectContext.get().getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
        parseExpression("'\\b'")
                .assertEquals(new StringLiteral("\\b"));
        parseExpression("'\\n'")
                .assertEquals(new StringLiteral("\\n"));
        parseExpression("'\\t'")
                .assertEquals(new StringLiteral("\\t"));
        parseExpression("'\\0'")
                .assertEquals(new StringLiteral("\\0"));
    }

    @Test
    public void testSqlBetweenPredicate() {
        String sql = "select * from test1 where d1 between 1 and 2";
        assertSql(sql);
    }

    @Test
    public void testExprBetweenPredicate() {
        parseExpression("c BETWEEN a AND b")
                .assertEquals(
                        new And(
                                new GreaterThanEqual(new UnboundSlot("c"), new UnboundSlot("a")),
                                new LessThanEqual(new UnboundSlot("c"), new UnboundSlot("b"))
                        )
                );
    }

    @Test
    public void testInPredicate() {
        String in = "select * from test1 where d1 in (1, 2, 3)";
        assertSql(in);

        String inExpr = "c IN (a, b)";
        assertExpr(inExpr);
    }

    @Test
    public void testSqlAnd() {
        String sql = "select * from test1 where a > 1 and b > 1";
        assertSql(sql);
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

        parseExpression("3 += 2")
                .assertThrowsExactly(ParseException.class)
                .assertMessageContains("extraneous input '=' expecting {'(");

    }

    @Test
    public void testSqlFunction() {
        String sum = "select sum(a) from test1";
        assertSql(sum);

        String sumWithAs = "select sum(a) as b from test1";
        assertSql(sumWithAs);

        String sumAndAvg = "select sum(a),avg(b) from test1";
        assertSql(sumAndAvg);

        String substring = "select substr(a, 1, 2), substring(b ,3 ,4) from test1";
        assertSql(substring);

        String count = "select count(*), count(b) from test1";
        assertSql(count);

        String min = "select min(a), min(b) as m from test1";
        assertSql(min);

        String max = "select max(a), max(b) as m from test1";
        assertSql(max);

        String maxAndMin = "select max(a), min(b) from test1";
        assertSql(maxAndMin);
    }

    @Test
    public void testGroupByClause() {

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
    public void testSortClause() {

        String sort = "select a from test order by c, d";
        assertSql(sort);

        String sort1 = "select a from test order by 1";
        assertSql(sort1);
    }

    @Test
    public void testCaseWhen() {
        String caseWhen = "select case a when 1 then 2 else 3 end from test";
        assertSql(caseWhen);

        String caseWhen2 = "select case when a = 1 then 2 else 3 end from test";
        assertSql(caseWhen2);
    }

    @Test
    public void testInSubquery() {
        String in = "select * from test where a in (select * from test1 where a = 0)";
        assertSql(in);

        String inExpr = "a in (select * from test where b = 1)";
        assertExpr(inExpr);

        String notIn = "select * from test where a not in (select * from test1 where a = 0)";
        assertSql(notIn);

        String notInExpr = "a not in (select * from test where b = 1)";
        assertExpr(notInExpr);
    }

    @Test
    public void testExist() {
        String exist = "select * from test where exists (select * from test where a = 1)";
        assertSql(exist);

        String existExpr = "exists (select * from test where b = 1)";
        assertExpr(existExpr);

        String notExist = "select * from test where not exists (select * from test where a = 1)";
        assertSql(notExist);

        String notExistExpr = "not exists (select * from test where b = 1)";
        assertExpr(notExistExpr);
    }

    @Test
    public void testInterval() {
        String interval = "tt > date '1991-05-01' + interval '1' day";
        assertExpr(interval);

        interval = "tt > '1991-05-01' + interval '1' day";
        assertExpr(interval);

        interval = "tt > '1991-05-01' + interval 1 day";
        assertExpr(interval);

        interval = "tt > '1991-05-01' - interval 1 day";
        assertExpr(interval);

        interval = "tt > date '1991-05-01' - interval '1' day";
        assertExpr(interval);

        interval = "tt > interval '1' day + '1991-05-01'";
        assertExpr(interval);

        interval = "tt > interval '1' day + date '1991-05-01'";
        assertExpr(interval);

        interval = "tt > '1991-05-01'  - interval 2*1 day";
        assertExpr(interval);

        interval = "tt > now() - interval 1+1 day";
        assertExpr(interval);
    }

    @Test
    public void testExtract() {
        String extract = "SELECT EXTRACT(YEAR FROM TIMESTAMP '2022-02-21 00:00:00') AS year FROM TEST;";
        assertSql(extract);

        String extract2 = "SELECT EXTRACT(YEAR FROM DATE '2022-02-21 00:00:00') AS year FROM TEST;";
        assertSql(extract2);

        String extract3 = "SELECT EXTRACT(YEAR FROM '2022-02-21 00:00:00') AS year FROM TEST;";
        assertSql(extract3);
    }

    @Test
    public void testCast() {
        String cast = "SELECT CAST(A AS STRING) FROM TEST;";
        assertSql(cast);

        String cast2 = "SELECT CAST(A AS INT) AS I FROM TEST;";
        assertSql(cast2);
    }

    @Test
    public void testIsNull() {
        String e1 = "a is null";
        assertExpr(e1);

        String e2 = "a is not null";
        assertExpr(e2);
    }

    @Test
    public void testMatch() {
        String sql = "select * from test "
                + "where (a match 'hello' or a match_any 'world') "
                + "and b match_all 'yes ok' or c match_phrase 'nice day';";
        assertSql(sql);
    }
}
