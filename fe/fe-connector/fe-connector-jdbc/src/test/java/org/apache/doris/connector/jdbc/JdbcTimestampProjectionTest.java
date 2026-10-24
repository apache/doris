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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class JdbcTimestampProjectionTest {
    @Test
    void trinoSessionClauseRemainsOutsideTimestampProjection() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.TRINO);
        java.util.List<org.apache.doris.connector.spi.handle.ConnectorColumnHandle> columns =
                java.util.Collections.singletonList(new JdbcColumnHandle(
                        "ts", "ts", ConnectorType.of("TIMESTAMPTZ", 6, 0)));
        for (String prefix : new String[] {"WITH SESSION query_max_execution_time='2h' ",
                "/* SELECT */ with -- comment\n session query_max_execution_time=concat('1', 'h'), "
                        + "query_max_run_time='2h' "}) {
            for (String body : new String[] {"SELECT ts FROM t", "WITH q AS (SELECT ts FROM t) SELECT ts FROM q"}) {
                String query = builder.wrapPassthroughQuery(prefix + body, columns);
                Assertions.assertTrue(query.startsWith(prefix + "SELECT "), query);
                Assertions.assertTrue(query.contains("FROM (" + body + "\n) doris_jdbc_query"), query);
            }
        }
    }

    @Test
    void mysqlInstantsUseTextProjectionForBothScanAndPassthrough() {
        for (JdbcDbType dialect : new JdbcDbType[] {JdbcDbType.MYSQL, JdbcDbType.OCEANBASE}) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
            java.util.List<org.apache.doris.connector.spi.handle.ConnectorColumnHandle> columns =
                    java.util.Collections.singletonList(new JdbcColumnHandle(
                            "ts", "ts", ConnectorType.of("TIMESTAMPTZ", 6, 0)));
            for (String query : new String[] {
                    builder.buildQuery("db", "tbl", columns, java.util.Optional.empty(), -1),
                    builder.wrapPassthroughQuery("SELECT ts FROM tbl", columns)}) {
                Assertions.assertTrue(query.contains("CAST(`ts` AS CHAR)"), query);
            }
        }
    }

    @Test
    void wallClockLiteralsKeepInstantPredicatesAndLimitLocal() {
        ConnectorType instant = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        for (String name : new String[] {"DATE", "DATEV2", "DATETIME", "DATETIMEV2"}) {
            ConnectorType local = ConnectorType.of(name, 6, 0);
            org.apache.doris.connector.spi.pushdown.ConnectorExpression filter =
                    new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                            org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.GT,
                            new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("ts", instant),
                            new org.apache.doris.connector.spi.pushdown.ConnectorLiteral(local,
                                    "DATE".equals(name) || "DATEV2".equals(name)
                                            ? java.time.LocalDate.of(2020, 1, 2)
                                            : java.time.LocalDateTime.of(2020, 1, 2, 4, 0)));
            String sql = new JdbcQueryBuilder(JdbcDbType.MYSQL).buildQuery("db", "tbl",
                    java.util.Collections.singletonList(new JdbcColumnHandle("ts", "ts", instant)),
                    java.util.Optional.of(filter), 1);
            // A +08:00 session compares local 08:00 to 04:00; the remote UTC session sees 00:00.
            Assertions.assertFalse(sql.contains("WHERE"), sql);
            Assertions.assertFalse(sql.contains("LIMIT"), sql);
        }
    }

    @Test
    void mysqlMixedTimestampComparisonsRemainLocal() {
        ConnectorType instant = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        ConnectorType local = ConnectorType.of("DATETIMEV2", 6, 0);
        org.apache.doris.connector.spi.pushdown.ConnectorComparison filter =
                new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                        org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.GT,
                        new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("ts", instant),
                        new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("dt", local));
        String sql = new JdbcQueryBuilder(JdbcDbType.MYSQL).buildQuery("db", "tbl",
                java.util.Arrays.asList(new JdbcColumnHandle("ts", "ts", instant),
                        new JdbcColumnHandle("dt", "dt", local)), java.util.Optional.of(filter), 1);
        Assertions.assertFalse(sql.contains("WHERE"), sql);
        Assertions.assertFalse(sql.contains("LIMIT"), sql);
    }

    @Test
    void scalarAndNestedInstantsAreProjectedBeforeDecoding() {
        ConnectorType instant = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        ConnectorType nested = ConnectorType.arrayOf(ConnectorType.arrayOf(instant));
        for (JdbcDbType dialect : new JdbcDbType[] {JdbcDbType.TRINO, JdbcDbType.PRESTO, JdbcDbType.CLICKHOUSE}) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
            java.util.List<org.apache.doris.connector.spi.handle.ConnectorColumnHandle> columns =
                    java.util.Arrays.asList(new JdbcColumnHandle("ts", "ts", instant),
                            new JdbcColumnHandle("events", "events", nested));
            String query = builder.buildQuery("db", "tbl", columns, java.util.Optional.empty(), -1);
            String tvf = builder.wrapPassthroughQuery("SELECT ts, events FROM tbl;", columns);
            for (String sql : new String[] {query, tvf}) {
                if (dialect == JdbcDbType.CLICKHOUSE) {
                    Assertions.assertTrue(sql.contains("toUnixTimestamp64Micro(toDateTime64(\"ts\", 6))"), sql);
                    Assertions.assertTrue(sql.contains("arrayMap(doris_ts_0 -> arrayMap(doris_ts_1"), sql);
                } else {
                    Assertions.assertTrue(sql.contains("(\"ts\" AT TIME ZONE 'UTC')"), sql);
                    Assertions.assertTrue(sql.contains("transform(\"events\", doris_ts_0 -> transform(doris_ts_0"), sql);
                }
            }
            Assertions.assertTrue(tvf.endsWith("FROM (SELECT ts, events FROM tbl\n) doris_jdbc_query"), tvf);
            Assertions.assertEquals("SELECT local_time FROM tbl;", builder.wrapPassthroughQuery(
                    "SELECT local_time FROM tbl;", java.util.Collections.singletonList(
                            new JdbcColumnHandle("local_time", "local_time", ConnectorType.of("DATETIMEV2", 6, 0)))));
        }
    }

    @Test
    void terminalLineCommentsDoNotConsumeTheWrapper() {
        for (JdbcDbType dialect : new JdbcDbType[] {JdbcDbType.TRINO, JdbcDbType.PRESTO, JdbcDbType.CLICKHOUSE}) {
            String sql = new JdbcQueryBuilder(dialect).wrapPassthroughQuery(
                    "SELECT ts FROM tbl -- terminal comment\n",
                    java.util.Collections.singletonList(new JdbcColumnHandle(
                            "ts", "ts", ConnectorType.of("TIMESTAMPTZ", 6, 0))));
            // A line comment must not consume the derived table's closing parenthesis and alias.
            String withoutComments = sql.replaceAll("(?m)--[^\r\n]*", "");
            Assertions.assertTrue(withoutComments.endsWith(") doris_jdbc_query"), sql);
        }
    }

    @Test
    void terminalDelimiterBeforeCommentsIsRemovedWithoutChangingQuotedText() {
        for (JdbcDbType dialect : new JdbcDbType[] {JdbcDbType.MYSQL, JdbcDbType.OCEANBASE,
                JdbcDbType.TRINO, JdbcDbType.PRESTO, JdbcDbType.CLICKHOUSE}) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
            java.util.List<org.apache.doris.connector.spi.handle.ConnectorColumnHandle> columns =
                    java.util.Collections.singletonList(new JdbcColumnHandle(
                            "ts", "ts", ConnectorType.of("TIMESTAMPTZ", 6, 0)));
            String body = "SELECT ts FROM tbl WHERE label = 'a;--b'';/*c*/'";
            for (String suffix : new String[] {" -- label", " /* label; */", " /* first */ -- last",
                    "\n-- label\r\n/* last */"}) {
                String query = builder.wrapPassthroughQuery(body + ";" + suffix, columns);
                Assertions.assertTrue(query.endsWith("FROM (" + body + suffix
                        + "\n) doris_jdbc_query"), query);
            }
            if (dialect == JdbcDbType.MYSQL || dialect == JdbcDbType.OCEANBASE) {
                String bodyWithIdentifier = "SELECT `semi;colon` AS ts FROM tbl";
                String query = builder.wrapPassthroughQuery(bodyWithIdentifier + "; # label", columns);
                Assertions.assertTrue(query.endsWith("FROM (" + bodyWithIdentifier
                        + " # label\n) doris_jdbc_query"), query);
            }
        }
    }

    @Test
    void postgresTimestampNullPredicatesAndLimitStayLocal() {
        ConnectorType type = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        org.apache.doris.connector.spi.pushdown.ConnectorIsNull filter =
                new org.apache.doris.connector.spi.pushdown.ConnectorIsNull(
                        new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("ts", type), false);
        String sql = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL).buildQuery("db", "tbl",
                java.util.Collections.singletonList(new JdbcColumnHandle("ts", "ts", type)),
                java.util.Optional.of(filter), 1);
        // Out-of-range PostgreSQL instants decode to NULL; remote IS NULL has a different result.
        Assertions.assertFalse(sql.contains("WHERE"), sql);
        Assertions.assertFalse(sql.contains("LIMIT"), sql);
    }

    @Test
    void mysqlDoubleMinusArithmeticRetainsTheExpression() {
        for (JdbcDbType dialect : new JdbcDbType[] {JdbcDbType.MYSQL, JdbcDbType.OCEANBASE}) {
            String body = "SELECT ts, balance--1 AS n FROM tbl";
            String query = new JdbcQueryBuilder(dialect).wrapPassthroughQuery(body + ";",
                    java.util.Collections.singletonList(new JdbcColumnHandle(
                            "ts", "ts", ConnectorType.of("TIMESTAMPTZ", 6, 0))));
            // MySQL requires whitespace after the second dash to start a line comment.
            Assertions.assertTrue(query.endsWith("FROM (" + body + "\n) doris_jdbc_query"), query);
        }
    }

    @Test
    void mysqlNoBackslashEscapesDoesNotConsumeTheClosingQuote() {
        java.util.List<org.apache.doris.connector.spi.handle.ConnectorColumnHandle> columns =
                java.util.Collections.singletonList(new JdbcColumnHandle(
                        "ts", "ts", ConnectorType.of("TIMESTAMPTZ", 6, 0)));
        for (JdbcDbType dialect : new JdbcDbType[] {JdbcDbType.MYSQL, JdbcDbType.OCEANBASE}) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
            String body = "SELECT ts FROM tbl WHERE label='a\\'";
            for (String suffix : new String[] {"", " -- trailing", " /* trailing */"}) {
                String sql = builder.wrapPassthroughQuery(body + ";" + suffix, columns, true);
                Assertions.assertTrue(sql.endsWith("FROM (" + body + suffix + "\n) doris_jdbc_query"), sql);
            }
            String escapedBody = "SELECT ts FROM tbl WHERE label='a\\';b'";
            String escaped = builder.wrapPassthroughQuery(escapedBody + ";", columns, false);
            Assertions.assertTrue(escaped.endsWith("FROM (" + escapedBody + "\n) doris_jdbc_query"), escaped);
        }
    }

    private static final ConnectorType INSTANT = ConnectorType.of("TIMESTAMPTZ", 6, 0);
    private static final ConnectorType LOCAL = ConnectorType.of("DATETIMEV2", 6, 0);
    private static final ConnectorType BYTES = ConnectorType.of("VARBINARY", 16, 0);
    private static final List<ConnectorColumnHandle> FILTER_COLUMNS = Arrays.asList(
            new JdbcColumnHandle("ts", "ts", INSTANT), new JdbcColumnHandle("other_ts", "other_ts", INSTANT),
            new JdbcColumnHandle("dt", "dt", LOCAL), new JdbcColumnHandle("payload", "payload", BYTES),
            new JdbcColumnHandle("id", "id", ConnectorType.of("INT")));

    private static boolean pushedDown(JdbcQueryBuilder builder, ConnectorExpression filter) {
        return builder.buildQuery("db", "tbl", FILTER_COLUMNS, Optional.of(filter), -1).contains("WHERE");
    }

    private static ConnectorExpression idIsOne() {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), new ConnectorLiteral(ConnectorType.of("INT"), 1));
    }

    @Test
    void calendarFunctionsOnInstantsStayLocal() {
        JdbcQueryBuilder mysql = new JdbcQueryBuilder(JdbcDbType.MYSQL,
                JdbcFunctionPushdownConfig.create(JdbcDbType.MYSQL, null, true), Collections.emptyMap());
        // year() of an instant depends on the zone; the remote UTC session differs from the Doris session.
        Assertions.assertFalse(pushedDown(mysql, new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorFunctionCall("year", ConnectorType.of("INT"),
                        Collections.singletonList(new ConnectorColumnRef("ts", INSTANT))),
                new ConnectorLiteral(ConnectorType.of("INT"), 2020))));
        Assertions.assertTrue(pushedDown(mysql, new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorFunctionCall("year", ConnectorType.of("INT"),
                        Collections.singletonList(new ConnectorColumnRef("dt", LOCAL))),
                new ConnectorLiteral(ConnectorType.of("INT"), 2020))));
        // A direct null check is zone-independent.
        Assertions.assertTrue(pushedDown(mysql, new ConnectorIsNull(new ConnectorColumnRef("ts", INSTANT), false)));
    }

    @Test
    void instantLiteralsStayLocalInEveryDialect() {
        ConnectorLiteral literal = new ConnectorLiteral(INSTANT, LocalDateTime.of(2020, 1, 2, 4, 1, 0, 111333000));
        for (JdbcDbType dialect : JdbcDbType.values()) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
            Assertions.assertFalse(pushedDown(builder, new ConnectorComparison(ConnectorComparison.Operator.EQ,
                    new ConnectorColumnRef("ts", INSTANT), literal)), dialect.name());
            Assertions.assertFalse(pushedDown(builder, new ConnectorIn(new ConnectorColumnRef("ts", INSTANT),
                    Collections.singletonList(literal), false)), dialect.name());
        }
    }

    @Test
    void postgresInstantPredicatesObserveDecodedNulls() {
        // Out-of-range PostgreSQL instants become NULL only after JDBC decoding.
        JdbcQueryBuilder postgres = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL);
        ConnectorColumnRef ts = new ConnectorColumnRef("ts", INSTANT);
        ConnectorColumnRef other = new ConnectorColumnRef("other_ts", INSTANT);
        for (ConnectorExpression predicate : Arrays.asList(new ConnectorIsNull(ts, false),
                new ConnectorIsNull(ts, true),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, ts, other),
                new ConnectorComparison(ConnectorComparison.Operator.EQ_FOR_NULL, ts, other),
                new ConnectorOr(Arrays.asList(new ConnectorIsNull(ts, false), idIsOne())))) {
            Assertions.assertFalse(pushedDown(postgres, predicate), predicate.toString());
        }
        Assertions.assertTrue(pushedDown(new JdbcQueryBuilder(JdbcDbType.MYSQL), new ConnectorIsNull(ts, false)));
        Assertions.assertTrue(pushedDown(postgres, new ConnectorIsNull(new ConnectorColumnRef("dt", LOCAL), false)));
        Assertions.assertTrue(pushedDown(postgres, idIsOne()));
    }

    @Test
    void binaryLiteralsStayLocalWhereTheDialectReadsThemDifferently() {
        ConnectorColumnRef payload = new ConnectorColumnRef("payload", BYTES);
        for (JdbcDbType dialect : Arrays.asList(JdbcDbType.POSTGRESQL, JdbcDbType.ORACLE, JdbcDbType.SQLSERVER,
                JdbcDbType.DB2, JdbcDbType.OCEANBASE_ORACLE)) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(dialect);
            for (ConnectorComparison.Operator operator : Arrays.asList(ConnectorComparison.Operator.EQ,
                    ConnectorComparison.Operator.NE, ConnectorComparison.Operator.LT)) {
                Assertions.assertFalse(pushedDown(builder, new ConnectorComparison(operator, payload,
                        binaryLiteral((byte) 0, (byte) 0xff))), dialect + " " + operator);
            }
            for (boolean notIn : new boolean[] {false, true}) {
                ConnectorExpression in = new ConnectorIn(payload, Arrays.asList(
                        binaryLiteral((byte) 0xde, (byte) 0, (byte) 0xff), binaryLiteral()), notIn);
                Assertions.assertFalse(pushedDown(builder, in), dialect + " notIn=" + notIn);
                Assertions.assertFalse(pushedDown(builder, new ConnectorOr(Arrays.asList(in, idIsOne()))),
                        dialect + " notIn=" + notIn);
            }
            Assertions.assertTrue(pushedDown(builder, new ConnectorIn(new ConnectorColumnRef("id",
                    ConnectorType.of("INT")), Collections.singletonList(
                            new ConnectorLiteral(ConnectorType.of("INT"), 1)), false)), dialect.name());
        }
        String mysql = new JdbcQueryBuilder(JdbcDbType.MYSQL).buildQuery("db", "tbl", FILTER_COLUMNS, Optional.of(
                new ConnectorIn(payload, Collections.singletonList(binaryLiteral((byte) 0, (byte) 0xff)), false)), -1);
        Assertions.assertTrue(mysql.contains("IN (X'00ff')"), mysql);
    }

    // VARBINARY literals cross the SPI as a lossless Latin-1 string carrier.
    private static ConnectorLiteral binaryLiteral(byte... bytes) {
        return new ConnectorLiteral(BYTES, new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1));
    }

}
