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

package org.apache.doris.connector.spi;

/**
 * Passing a SQL string through to the remote source untouched, for a connector that fronts a system which
 * speaks SQL itself.
 *
 * <p><b>Optional: implement this interface, or do not.</b> It is not part of {@link ConnectorMetadata}, so a
 * connector that has no remote SQL dialect never sees these methods and owes them nothing. Implementing the
 * interface IS the declaration — there is no accompanying capability flag to keep in step with it (a
 * {@code SUPPORTS_PASSTHROUGH_QUERY} flag existed and was removed: it was a second overridable answer to the
 * question "can this connector run my SQL", and a connector could declare it while implementing nothing).</p>
 *
 * <p>Both methods take a SQL string the user wrote. A connector that implements them owns the consequences:
 * the engine does not parse, rewrite or authorize the statement beyond the catalog-level privilege check on
 * the entry points, so an implementation must send it under the catalog's own credentials and must not widen
 * what those credentials can reach.</p>
 *
 * <p>Minimum implementation set: whichever of the two the connector actually supports. Each defaults to
 * refusing, so implementing the interface for the {@code query()} TVF alone does not silently claim
 * {@code CALL EXECUTE_STMT} as well.</p>
 */
public interface ConnectorPassthroughSqlOps {

    /**
     * Executes a DML statement (INSERT/UPDATE/DELETE) on the remote source verbatim, for
     * {@code CALL EXECUTE_STMT(catalog, stmt)}. Nothing is returned: the statement's effect is remote.
     */
    default void executeStmt(ConnectorSession session, String stmt) {
        throw new DorisConnectorException("executeStmt not supported");
    }

    /**
     * Returns the column metadata of an arbitrary remote query, for the {@code query()} table-valued function
     * (typically via the remote driver's prepared-statement metadata, without running the query).
     */
    default ConnectorTableSchema getColumnsFromQuery(ConnectorSession session, String query) {
        throw new DorisConnectorException("getColumnsFromQuery not supported");
    }
}
