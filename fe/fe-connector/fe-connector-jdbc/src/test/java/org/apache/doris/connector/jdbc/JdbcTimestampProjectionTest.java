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
