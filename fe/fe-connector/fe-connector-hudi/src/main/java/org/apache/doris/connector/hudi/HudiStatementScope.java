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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.connector.api.ConnectorStatementScopes;

import java.util.List;
import java.util.function.Supplier;

/**
 * Connector-private helpers over the neutral {@link ConnectorStatementScope} (reached via
 * {@link ConnectorSession#getStatementScope()}), giving hudi one place to key its per-statement metaClient memos.
 *
 * <p>Two INDEPENDENT memos, so a statement builds each latest fact at most once and the two never couple: the
 * latest-schema read ({@code getTableSchema} with no time-travel pin) resolves through {@link #sharedLatestColumns},
 * and the query-snapshot pin ({@code beginQuerySnapshot}) through {@link #sharedLatestInstant}. Each loader is the
 * connector's existing single-fact method, so the memo only collapses REPEATED same-fact resolves within a statement
 * and leaves each fact's value / failure semantics byte-identical. Distinct namespaces
 * ({@link ConnectorStatementScopes#HUDI_LATEST_SCHEMA} vs {@link ConnectorStatementScopes#HUDI_LATEST_INSTANT}) keep
 * the two value types (columns vs instant) from colliding on the shared {@code (catalog, db, table, queryId)}
 * coordinate.</p>
 *
 * <p>Under {@link ConnectorStatementScope#NONE} (offline planning / no live statement) or a {@code null} session each
 * loader runs every time — byte-identical to resolving the fact per call, as before the memo.</p>
 */
final class HudiStatementScope {

    private HudiStatementScope() {}

    /**
     * Resolves the latest columns for {@code db.table} once per statement, sharing the single result across every
     * latest-schema resolver of the statement. {@code loader} (the connector's existing latest-schema read) runs at
     * most once per statement; the key carries the catalog id and queryId (cross-catalog / prepared-execution
     * isolation).
     */
    static List<ConnectorColumn> sharedLatestColumns(ConnectorSession session, String dbName, String tableName,
            Supplier<List<ConnectorColumn>> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, ConnectorStatementScopes.HUDI_LATEST_SCHEMA, dbName, tableName, loader);
    }

    /**
     * Resolves the latest completed instant for {@code db.table} once per statement, sharing the single value across
     * every snapshot-pin resolver of the statement. {@code loader} (the connector's existing instant read) runs at
     * most once per statement.
     */
    static long sharedLatestInstant(ConnectorSession session, String dbName, String tableName,
            Supplier<Long> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, ConnectorStatementScopes.HUDI_LATEST_INSTANT, dbName, tableName, loader);
    }
}
