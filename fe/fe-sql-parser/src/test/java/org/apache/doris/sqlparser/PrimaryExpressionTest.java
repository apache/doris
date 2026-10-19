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

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisParser.ExpressionContext;
import org.apache.doris.nereids.exceptions.ParseException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class PrimaryExpressionTest {
    private final DorisSqlParser parser = new DorisSqlParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("primaryExpressionAlternatives")
    void parsesEveryPrimaryExpressionAlternative(String description, String sql) {
        ExpressionContext context = parser.parseExpression(sql);
        Assertions.assertNotNull(context);
    }

    private static Stream<Arguments> primaryExpressionAlternatives() {
        return Stream.of(
                Arguments.of("current date", "CURRENT_DATE"),
                Arguments.of("current time", "CURRENT_TIME"),
                Arguments.of("current timestamp", "CURRENT_TIMESTAMP"),
                Arguments.of("local time", "LOCALTIME"),
                Arguments.of("local timestamp", "LOCALTIMESTAMP"),
                Arguments.of("current user", "CURRENT_USER"),
                Arguments.of("session user", "SESSION_USER"),
                Arguments.of("searched case", "CASE WHEN a > 0 THEN 1 ELSE 2 END"),
                Arguments.of("simple case", "CASE a WHEN 1 THEN 2 WHEN 3 THEN 4 END"),
                Arguments.of("cast", "CAST(a + 1 AS BIGINT)"),
                Arguments.of("try cast", "TRY_CAST(a AS DECIMAL(10, 2))"),
                Arguments.of("default value", "DEFAULT(t.c)"),
                Arguments.of("null literal", "NULL"),
                Arguments.of("typed literal", "DATE '2026-08-19'"),
                Arguments.of("numeric literal", "123.45"),
                Arguments.of("boolean literal", "TRUE"),
                Arguments.of("string literal", "'text'"),
                Arguments.of("varbinary literal", "X'0A0B'"),
                Arguments.of("array literal", "[1, 2, 3]"),
                Arguments.of("map literal", "{'a': 1, 'b': 2}"),
                Arguments.of("struct literal", "{1, 'a'}"),
                Arguments.of("placeholder", "?"),
                Arguments.of("interval", "INTERVAL 1 DAY"),
                Arguments.of("unqualified star", "* EXCEPT(a)"),
                Arguments.of("qualified star", "db.t.* REPLACE(1 AS a)"),
                Arguments.of("char function", "CHAR(65, 66 USING utf8)"),
                Arguments.of("convert charset", "CONVERT(IF(a, b, c) USING utf8)"),
                Arguments.of("convert type", "CONVERT(IF(a, b, c), DECIMAL(10, 2))"),
                Arguments.of("group concat", "GROUP_CONCAT(DISTINCT a ORDER BY b SEPARATOR ',')"),
                Arguments.of("trim", "TRIM(BOTH 'x' FROM a)"),
                Arguments.of("substring", "SUBSTRING(a FROM 2 FOR 3)"),
                Arguments.of("position", "POSITION('x' IN a)"),
                Arguments.of("is null function", "ISNULL(a)"),
                Arguments.of("is not null function", "IS_NOT_NULL_PRED(a)"),
                Arguments.of("function call", "db.fn(a, b + 1)"),
                Arguments.of("element at", "a[1]"),
                Arguments.of("array slice", "a[1:2]"),
                Arguments.of("scalar subquery", "(SELECT 1)"),
                Arguments.of("user variable", "@user_var"),
                Arguments.of("system variable", "@@SESSION.system_var"),
                Arguments.of("binary column reference", "BINARY a"),
                Arguments.of("column reference", "a"),
                Arguments.of("dereference", "a.b.c"),
                Arguments.of("parenthesized expression", "((a + 1) * 2)"),
                Arguments.of("encrypt key", "KEY db.key_name"),
                Arguments.of("extract", "EXTRACT(YEAR FROM DATE '2026-08-19')"),
                Arguments.of("collate", "a COLLATE utf8_general_ci"));
    }

    @ParameterizedTest(name = "ambiguous prefix: {0}")
    @MethodSource("ambiguousPrefixExpressions")
    void preservesIdentifierAndSpecialFormBoundaries(String description, String sql) {
        ExpressionContext context = parser.parseExpression(sql);
        Assertions.assertNotNull(context);
    }

    private static Stream<Arguments> ambiguousPrefixExpressions() {
        return Stream.of(
                Arguments.of("current date special form", "CURRENT_DATE"),
                Arguments.of("current date function", "CURRENT_DATE()"),
                Arguments.of("case keyword as identifier", "CASE"),
                Arguments.of("cast keyword as identifier", "CAST"),
                Arguments.of("convert keyword as identifier", "CONVERT"),
                Arguments.of("convert generic function", "CONVERT(a)"),
                Arguments.of("trim special syntax", "TRIM(a FROM b)"),
                Arguments.of("trim generic function", "TRIM(a)"),
                Arguments.of("substring special syntax", "SUBSTRING(a FROM 1)"),
                Arguments.of("substring generic function", "SUBSTRING(a, 1)"),
                Arguments.of("position special syntax", "POSITION(a IN b)"),
                Arguments.of("position generic function", "POSITION(a)"),
                Arguments.of("date typed literal", "DATE '2026-08-19'"),
                Arguments.of("date generic function", "DATE(a)"),
                Arguments.of("date column", "DATE"),
                Arguments.of("interval literal", "INTERVAL 1 DAY"),
                Arguments.of("interval generic function", "INTERVAL()"),
                Arguments.of("qualified function", "db.fn(a)"),
                Arguments.of("qualified star", "db.t.*"),
                Arguments.of("qualified column", "db.t.c"),
                Arguments.of("binary string literal", "BINARY 'abc'"),
                Arguments.of("binary column", "BINARY a"));
    }

    @ParameterizedTest(name = "invalid: {0}")
    @MethodSource("invalidExpressions")
    void rejectsIncompletePrimaryExpressionsAtTheOriginalPosition(
            String description, String sql, int errorPosition) {
        ParseException exception = Assertions.assertThrows(ParseException.class, () -> parser.parseExpression(sql));
        Assertions.assertTrue(exception.getMessage().contains("line 1, pos " + errorPosition), exception::getMessage);
    }

    private static Stream<Arguments> invalidExpressions() {
        return Stream.of(
                Arguments.of("searched case missing END", "CASE WHEN a THEN b", 5),
                invalidAtEnd("simple case missing result", "CASE a WHEN 1 THEN"),
                Arguments.of("convert charset missing charset", "CONVERT(a USING)", 15),
                Arguments.of("convert type missing type", "CONVERT(a,)", 10),
                invalidAtEnd("subquery missing right parenthesis", "(SELECT 1"),
                invalidAtEnd("expression missing right parenthesis", "(a + 1"),
                invalidAtEnd("array access missing index", "a["),
                invalidAtEnd("array slice missing end bracket", "a[1:"),
                invalidAtEnd("array slice value missing end bracket", "a[1:2"),
                invalidAtEnd("dereference missing field", "a."),
                invalidAtEnd("collate missing collation", "a COLLATE"),
                Arguments.of("cast missing type", "CAST(a AS)", 9),
                Arguments.of("trim missing source", "TRIM(a FROM)", 11),
                Arguments.of("trailing garbage", "a[1] garbage", 5));
    }

    private static Arguments invalidAtEnd(String description, String sql) {
        return Arguments.of(description, sql, sql.length());
    }
}
