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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Runs the generated SQL through the real SQLite ADBC driver.
 *
 * <p>Asserting the text of a statement only proves it is the text that was intended. Whether a source
 * ACCEPTS it -- the quoting, the literal spellings, the placement of {@code LIMIT} -- is a different
 * question, and one the ANSI dialect answers on behalf of sources nobody has tried. This is the cheapest
 * available source that answers it for real.
 *
 * <p>Skips loudly without the native libraries; a skipped run has verified nothing about real SQL
 * acceptance, only about string building.
 */
class AdbcQueryBuilderNativeTest {

    private static final AdbcDialect ANSI = AdbcDialectRegistry.defaultDialect();
    private static final AdbcTableHandle T1 =
            new AdbcTableHandle(new AdbcNamespace("main", ""), "t1");

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    /**
     * Values chosen so every literal form the dialect renders appears in a predicate below, and so the
     * quoting matters: a column named with a reserved word and a string holding a quote.
     */
    private static void seed(AdbcClient client) {
        client.withConnection(connection -> {
            for (String sql : new String[] {
                    "CREATE TABLE t1 (id INTEGER, \"select\" REAL, name TEXT)",
                    "INSERT INTO t1 VALUES (1, 1.5, 'a')",
                    "INSERT INTO t1 VALUES (2, 2.5, 'O''Brien')",
                    "INSERT INTO t1 VALUES (3, 3.5, NULL)"}) {
                try (AdbcStatement statement = connection.createStatement()) {
                    statement.setSqlQuery(sql);
                    statement.executeUpdate();
                }
            }
            return null;
        });
    }

    private static List<ConnectorColumnHandle> columns(String... names) {
        List<ConnectorColumnHandle> handles = new ArrayList<>(names.length);
        for (String name : names) {
            handles.add(new NamedColumnHandle(name));
        }
        return handles;
    }

    private static ConnectorColumnRef col(String name, String type) {
        return new ConnectorColumnRef(name, ConnectorType.of(type));
    }

    /** Runs the generated statement and returns its column names and row count. */
    private static Result run(AdbcClient client, List<ConnectorColumnHandle> cols,
            ConnectorExpression filter, long limit) {
        String sql = AdbcQueryBuilder.build(ANSI, T1, cols, Optional.ofNullable(filter), limit).getSql();
        return client.withConnection(connection -> {
            try (AdbcStatement statement = connection.createStatement()) {
                statement.setSqlQuery(sql);
                try (AdbcStatement.QueryResult queryResult = statement.executeQuery()) {
                    ArrowReader reader = queryResult.getReader();
                    List<String> names = new ArrayList<>();
                    reader.getVectorSchemaRoot().getSchema().getFields()
                            .forEach(field -> names.add(field.getName()));
                    int rows = 0;
                    while (reader.loadNextBatch()) {
                        rows += reader.getVectorSchemaRoot().getRowCount();
                    }
                    return new Result(sql, names, rows);
                }
            }
        });
    }

    @Test
    void theSourceAcceptsAProjectionOfQuotedIdentifiers(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("scan.db"))) {
            seed(client);
            // "select" is a reserved word; unquoted it is a syntax error, which is what makes this a real
            // test of the quoting rather than of the string building.
            Result result = run(client, columns("id", "select"), null, -1);

            Assertions.assertEquals(List.of("id", "select"), result.columnNames, result.sql);
            Assertions.assertEquals(3, result.rows, result.sql);
        }
    }

    @Test
    void theSourceReturnsOnlyTheRequestedColumns(@TempDir Path tempDir) {
        // BE rejects any column it did not ask for, so this is the property the scan depends on -- not
        // merely that the query runs.
        try (AdbcClient client = sqliteClient(tempDir.resolve("proj.db"))) {
            seed(client);
            Assertions.assertEquals(List.of("id"), run(client, columns("id"), null, -1).columnNames);
        }
    }

    @Test
    void theSourceAcceptsTheNumericAndStringLiteralsTheDialectWrites(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("lit.db"))) {
            seed(client);

            Assertions.assertEquals(2, run(client, columns("id"),
                    new ConnectorComparison(ConnectorComparison.Operator.GT,
                            col("id", "INT"), ConnectorLiteral.ofLong(1)), -1).rows);
            Assertions.assertEquals(1, run(client, columns("id"),
                    new ConnectorComparison(ConnectorComparison.Operator.LT,
                            col("select", "DOUBLE"), ConnectorLiteral.ofDouble(2.0d)), -1).rows);
            // The escaped quote survives the round trip to the source, rather than ending the literal.
            Assertions.assertEquals(1, run(client, columns("id"),
                    new ConnectorComparison(ConnectorComparison.Operator.EQ,
                            col("name", "STRING"), ConnectorLiteral.ofString("O'Brien")), -1).rows);
        }
    }

    @Test
    void theSourceAcceptsNullTestsInListsAndConjunctions(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("pred.db"))) {
            seed(client);

            Assertions.assertEquals(1, run(client, columns("id"),
                    new ConnectorIsNull(col("name", "STRING"), false), -1).rows);
            Assertions.assertEquals(2, run(client, columns("id"),
                    new ConnectorIn(col("id", "INT"),
                            List.of(ConnectorLiteral.ofLong(1), ConnectorLiteral.ofLong(3)), false),
                    -1).rows);
            Assertions.assertEquals(1, run(client, columns("id"), new ConnectorAnd(List.of(
                    new ConnectorComparison(ConnectorComparison.Operator.GE,
                            col("id", "INT"), ConnectorLiteral.ofLong(2)),
                    new ConnectorIsNull(col("name", "STRING"), true))), -1).rows);
        }
    }

    @Test
    void theSourceAcceptsTheLimitClauseWhereTheBuilderPutsIt(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("limit.db"))) {
            seed(client);
            Assertions.assertEquals(2, run(client, columns("id"), null, 2).rows);
            Assertions.assertEquals(1, run(client, columns("id"),
                    new ConnectorComparison(ConnectorComparison.Operator.GT,
                            col("id", "INT"), ConnectorLiteral.ofLong(1)), 1).rows);
        }
    }

    @Test
    void theSourceAcceptsTheCountOnlyProjection(@TempDir Path tempDir) {
        // What a pushed-down COUNT(*) sends: one narrow column per row, no table values.
        try (AdbcClient client = sqliteClient(tempDir.resolve("count.db"))) {
            seed(client);
            Result result = run(client, columns(), null, -1);
            Assertions.assertEquals(3, result.rows, result.sql);
            Assertions.assertEquals(1, result.columnNames.size(), result.sql);
        }
    }

    private static final class Result {

        private final String sql;
        private final List<String> columnNames;
        private final int rows;

        Result(String sql, List<String> columnNames, int rows) {
            this.sql = sql;
            this.columnNames = columnNames;
            this.rows = rows;
        }
    }
}
