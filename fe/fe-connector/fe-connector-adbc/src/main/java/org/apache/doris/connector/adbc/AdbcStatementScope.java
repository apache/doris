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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.function.Supplier;

/**
 * Shares one table's Arrow schema across the paths that each need it within a single statement.
 *
 * <p>{@code getTableSchema} and {@code getColumnHandles} are separate SPI calls that derive different
 * products (Doris columns vs. name-to-handle map) from the same remote answer, and the engine may call both
 * for one table in one statement. Routing them through the statement scope collapses that to one remote
 * round trip while each keeps its own derivation.
 *
 * <p>Under a null session or a scope of {@code NONE} (offline, no live statement) the loader runs on every
 * call, which is byte-identical to fetching every time.
 */
final class AdbcStatementScope {

    /**
     * Namespace for the per-statement Arrow-schema memo. Prefixed with the connector's type name so it
     * stays distinct from a sibling connector's memo inside a heterogeneous gateway; guarded by
     * {@code AdbcStatementScopeTest}.
     */
    static final String TABLE_SCHEMA_NAMESPACE = "adbc.table_schema";

    private AdbcStatementScope() {
    }

    static Schema sharedTableSchema(ConnectorSession session, AdbcTableHandle handle,
            Supplier<Schema> loader) {
        // Keyed by the Doris database name rather than the remote parts: it is what the engine addresses
        // the table by, and within one catalog it identifies the namespace uniquely.
        return ConnectorStatementScopes.resolveInStatement(
                session, TABLE_SCHEMA_NAMESPACE, handle.getDorisDbName(), handle.getRemoteTable(), loader);
    }
}
