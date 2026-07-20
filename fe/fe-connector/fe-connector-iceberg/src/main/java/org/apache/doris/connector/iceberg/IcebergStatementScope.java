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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Connector-private helpers over the neutral {@link ConnectorStatementScope} (reached via
 * {@link ConnectorSession#getStatementScope()}), giving iceberg one place to key its per-statement state.
 *
 * <p>The scope is the per-statement table-load owner: the read metadata path, scan planning, write shaping
 * and {@code beginWrite} all resolve one table through {@link #sharedTable} so a single statement loads each
 * table once and every resolver shares that one RAW object (snapshot pins and auth wraps are applied per
 * consumer, never frozen into the shared object). It also carries the merge-on-read rewritable-delete supply
 * from the scan seam to the write seam ({@link #rewritableDeleteSupply}), replacing the former per-catalog
 * singleton stash — the scope is per-statement, so a statement's supply is GC'd with it and a reused
 * prepared-statement scope is reset per execution (see {@code ExecuteCommand}).</p>
 *
 * <p>Under {@link ConnectorStatementScope#NONE} (offline planning / no live statement) {@link #sharedTable}
 * loads every time (byte-identical to the pre-scope behavior) and {@link #rewritableDeleteSupply} returns a
 * throwaway map that does NOT bridge scan→write — so a format-version&ge;3 row-level DML under NONE fails
 * loud at the write seam rather than silently resurrecting rows.</p>
 */
final class IcebergStatementScope {

    private IcebergStatementScope() {}

    /**
     * Loads the RAW iceberg {@link Table} for {@code db.tbl} once per statement and shares it across every
     * resolver. The key includes the catalog id (cross-catalog MERGE isolation) and the statement's queryId
     * (a reused prepared context sees each execution's own table). {@code loader} runs at most once per
     * statement — callers pass the raw load (cross-query cache or direct remote) and own the auth scope
     * (the caller wraps this in {@code executeAuthenticated}).
     */
    static Table sharedTable(ConnectorSession session, String dbName, String tableName, Supplier<Table> loader) {
        if (session == null) {
            // No session (offline / direct-construction tests): load every time, like ConnectorStatementScope.NONE.
            return loader.get();
        }
        String key = "iceberg.table:" + session.getCatalogId() + ":" + dbName + ":" + tableName
                + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, loader);
    }

    /**
     * Returns this statement's rewritable-delete supply map (RAW data-file path &rarr; its non-equality delete
     * descs), creating it empty on first use. The scan seam accumulates into it (per touched data file) and the
     * write seam drains it; keyed by catalog id + queryId so a cross-catalog MERGE keeps each table's supply
     * isolated. Under {@link ConnectorStatementScope#NONE} each call returns a fresh throwaway map, so scan and
     * write do NOT share — the write seam guards format-version&ge;3 DML against that (fail loud).
     */
    static Map<String, List<TIcebergDeleteFileDesc>> rewritableDeleteSupply(ConnectorSession session) {
        if (session == null) {
            // No session: a throwaway map that does NOT bridge scan->write (same as NONE).
            return new ConcurrentHashMap<>();
        }
        String key = "iceberg.rewritable-delete-supply:" + session.getCatalogId() + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, ConcurrentHashMap::new);
    }
}
