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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
