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

package org.apache.doris.connector.api;

import java.util.function.Supplier;

/**
 * Statement-scoped resolution helpers over the neutral {@link ConnectorStatementScope} (reached via
 * {@link ConnectorSession#getStatementScope()}). A connector routes its "resolve {@code db.table} once per
 * statement" memo through {@link #resolveInStatement}, so every read / scan / write resolver of one statement
 * shares the single loaded value and the loader runs at most once — while a reused prepared-statement scope,
 * reset per execution, still isolates each execution (its {@code queryId} is part of the key).
 *
 * <p>This standardizes the security-critical key convention once instead of letting each connector re-derive it:
 * dropping {@code queryId} would leak a table across executions of a reused prepared statement; dropping
 * {@code catalogId} would collide across a cross-catalog {@code MERGE}.
 *
 * <p><b>Key-namespace registry.</b> {@code keyNamespace} namespaces the value <em>type</em> stored under the
 * shared {@code (catalogId, db, table, queryId)} coordinate, so a heterogeneous gateway statement touching two
 * connectors cannot collide on {@code (db, table)} and hand one connector another's value (a
 * {@link ClassCastException}). Every consumer MUST use a distinct namespace declared here as a reviewed
 * uniqueness invariant. Declared today: {@link #ICEBERG_TABLE}, {@link #HUDI_LATEST_SCHEMA},
 * {@link #HUDI_LATEST_INSTANT}, {@link #ES_INDEX_MAPPING}, {@link #JDBC_COLUMNS}. (maxcompute memoizes its
 * handle on the per-statement metadata instance itself, so it needs no namespace here.)
 */
public final class ConnectorStatementScopes {

    /** Namespace for iceberg's per-statement RAW {@code Table} memo (see {@code IcebergStatementScope}). */
    public static final String ICEBERG_TABLE = "iceberg.table";

    /** Namespace for hudi's per-statement latest-columns memo (see {@code HudiStatementScope}). */
    public static final String HUDI_LATEST_SCHEMA = "hudi.latest_schema";

    /** Namespace for hudi's per-statement latest-completed-instant memo (see {@code HudiStatementScope}). */
    public static final String HUDI_LATEST_INSTANT = "hudi.latest_instant";

    /**
     * Namespace for es's per-statement raw index-mapping memo, shared by the schema path
     * ({@code EsConnectorMetadata}) and the scan path ({@code EsMetadataFetcher}) so one index's
     * {@code getMapping} fires once per statement (see {@code EsStatementScope}).
     */
    public static final String ES_INDEX_MAPPING = "es.index_mapping";

    /**
     * Namespace for jdbc's per-statement RAW remote-columns memo (a {@code List<JdbcFieldInfo>}), shared by the
     * schema path ({@code JdbcConnectorMetadata#getTableSchema}), the scan column-handle path
     * ({@code JdbcConnectorMetadata#getColumnHandles}) and the write INSERT-SQL shaping
     * ({@code JdbcWritePlanProvider#buildInsertSql}) so one table's remote {@code getJdbcColumnsInfo} fires at
     * most once per statement. The value is the raw remote columns (session-independent); each consumer
     * re-applies its own transform (identifier mapper for handles, type conversion for schema) per call.
     * The shared list is treated read-only EXCEPT that the schema path's {@code jdbcTypeToConnectorType}
     * idempotently sets {@code allowNull = true} on a field in place for some date types — safe because the
     * mutation is idempotent and the only reader of {@code allowNull} is {@code getTableSchema}, which always
     * re-derives it via {@code jdbcTypeToConnectorType} before reading (the handle / write paths read only the
     * column name).
     */
    public static final String JDBC_COLUMNS = "jdbc.columns";

    private ConnectorStatementScopes() {
    }

    /**
     * Resolves {@code db.table} once per statement and shares the single value across every resolver of the
     * statement. The key is {@code keyNamespace + ":" + catalogId + ":" + db + ":" + table + ":" + queryId}: the
     * catalog id isolates a cross-catalog {@code MERGE}, the {@code queryId} isolates each execution of a reused
     * prepared statement, and {@code keyNamespace} isolates value types across a heterogeneous gateway.
     * {@code loader} runs at most once per statement; under a {@code null} session or
     * {@link ConnectorStatementScope#NONE} (offline / no live statement) it runs on every call — byte-identical
     * to loading every time.
     */
    public static <T> T resolveInStatement(ConnectorSession session, String keyNamespace,
                                           String db, String table, Supplier<T> loader) {
        if (session == null) {
            return loader.get();
        }
        String key = keyNamespace + ":" + session.getCatalogId() + ":" + db + ":" + table
                + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, loader);
    }
}
