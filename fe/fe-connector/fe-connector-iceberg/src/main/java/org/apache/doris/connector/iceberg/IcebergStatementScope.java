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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Connector-private helpers over the neutral {@link ConnectorStatementScope} (reached via
 * {@link ConnectorSession#getStatementScope()}), giving iceberg one place to key its per-statement state.
 *
 * <p>The scope is the per-statement table-load owner: read metadata and scan planning resolve one frozen
 * read table through {@link #sharedTable}; write shaping and
 * {@code beginWrite} use {@link #sharedWritableTable}. It also carries the merge-on-read rewritable-delete supply
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

    /**
     * Namespace for iceberg's per-statement RAW {@link Table} memo. Source-prefixed with the connector type
     * ("iceberg") so it stays distinct across a heterogeneous gateway; see {@link ConnectorStatementScopes}.
     */
    static final String TABLE_NAMESPACE = "iceberg.table";

    /** Separate namespace for mutable tables used by write planning and transaction creation. */
    static final String WRITABLE_TABLE_NAMESPACE = "iceberg.writable-table";

    /**
     * Namespace for iceberg's per-statement rewritable-delete supply map (a per-statement singleton keyed by
     * catalog id + queryId, with no db/table — it aggregates across all touched data files). Source-prefixed
     * with the connector type ("iceberg").
     */
    static final String REWRITABLE_DELETE_SUPPLY_NAMESPACE = "iceberg.rewritable-delete-supply";
    static final String WRITE_SCHEMA_NAMESPACE = "iceberg.write-schema";
    static final String ACTIVE_WRITE_SCHEMA_NAMESPACE = "iceberg.active-write-schema";

    private IcebergStatementScope() {}

    /**
     * Loads and freezes the iceberg {@link Table} for {@code db.tbl} once per statement. The frozen operations
     * prevent another statement's write refresh from changing the metadata generation after slots are bound.
     */
    static Table sharedTable(ConnectorSession session, String dbName, String tableName, Supplier<Table> loader) {
        // Delegates to the shared per-statement resolver. The TABLE_NAMESPACE ("iceberg.table") reproduces the
        // historical "iceberg.table:" key prefix byte-for-byte, so the funnel keeps identical hits / misses / NONE
        // fall-through (proved by IcebergStatementScopeTest#sharedTableKeyReproducesLegacyPrefixByteForByte).
        return ConnectorStatementScopes.resolveInStatement(session, TABLE_NAMESPACE, dbName, tableName,
                () -> snapshotReadTable(loader.get()));
    }

    /** Loads the mutable table used only by write planning and transaction creation. */
    static Table sharedWritableTable(
            ConnectorSession session, String dbName, String tableName, Supplier<Table> loader) {
        return ConnectorStatementScopes.resolveInStatement(
                session, WRITABLE_TABLE_NAMESPACE, dbName, tableName, loader);
    }

    private static Table snapshotReadTable(Table table) {
        if (!(table instanceof BaseTable)) {
            return table;
        }
        BaseTable baseTable = (BaseTable) table;
        TableOperations operations = baseTable.operations();
        TableMetadata metadata = operations.current();
        // Keep IO, encryption and location behavior from the raw table while pinning only metadata.
        return new BaseTable(new IcebergSnapshotTableOperations(operations, metadata),
                table.name(), baseTable.reporter());
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
        String key = REWRITABLE_DELETE_SUPPLY_NAMESPACE + ":" + session.getCatalogId() + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, ConcurrentHashMap::new);
    }

    static IcebergWriteSchemaContext writeSchema(ConnectorSession session, String dbName,
            String tableName, Optional<String> branchName, Supplier<IcebergWriteSchemaContext> loader) {
        if (session == null || session.getStatementScope() == ConnectorStatementScope.NONE) {
            return loader.get();
        }
        String key = WRITE_SCHEMA_NAMESPACE + ":" + session.getCatalogId() + ":"
                + session.getQueryId() + ":" + dbName + ":" + tableName + ":"
                + branchName.orElse("");
        IcebergWriteSchemaContext context =
                session.getStatementScope().computeIfAbsent(key, loader);
        activeWriteSchemas(session).put(tableKey(dbName, tableName), context);
        return context;
    }

    static Optional<IcebergWriteSchemaContext> activeWriteSchema(
            ConnectorSession session, String dbName, String tableName) {
        if (session == null || session.getStatementScope() == ConnectorStatementScope.NONE) {
            return Optional.empty();
        }
        return Optional.ofNullable(activeWriteSchemas(session).get(tableKey(dbName, tableName)));
    }

    private static Map<String, IcebergWriteSchemaContext> activeWriteSchemas(ConnectorSession session) {
        String key = ACTIVE_WRITE_SCHEMA_NAMESPACE + ":" + session.getCatalogId() + ":"
                + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, ConcurrentHashMap::new);
    }

    private static String tableKey(String dbName, String tableName) {
        return dbName + "\u0000" + tableName;
    }
}
