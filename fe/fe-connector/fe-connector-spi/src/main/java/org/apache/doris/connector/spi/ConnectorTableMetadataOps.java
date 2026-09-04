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

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolving a table by name, and reading what it looks like.
 *
 * <p><b>This is the one domain no connector can skip.</b> The four methods marked unconditional below are
 * overridden by all eight shipped connectors, and {@link #buildTableDescriptor} by seven of them. Leaving them
 * at their defaults is not an error the user sees as an error: {@link #listTableNames} answers an empty list,
 * so {@code SHOW TABLES} looks like an empty database, and {@link #getTableHandle} answers empty, so a query
 * looks like a mistyped table name. Only {@link #getTableSchema} and {@link #getColumnHandles} fail loud.</p>
 *
 * <p>Minimum implementation set:</p>
 * <ul>
 * <li><b>Always</b>: {@link #getTableHandle}, {@link #listTableNames},
 *     {@link #getTableSchema(ConnectorSession, ConnectorTableHandle)},
 *     {@link #getColumnHandles(ConnectorSession, ConnectorTableHandle)}, {@link #buildTableDescriptor}. The
 *     descriptor may be left to the engine's generic fallback (returning {@code null}) if the connector needs
 *     no typed descriptor for BE — one shipped connector relies on that — but decide it deliberately.</li>
 * <li><b>Time travel / schema evolution</b>: the snapshot-aware
 *     {@link #getTableSchema(ConnectorSession, ConnectorTableHandle, ConnectorMvccSnapshot)}. Implementing
 *     only this one is the common case (three of the four snapshot-capable connectors stop here).
 *     {@link #getColumnHandles(ConnectorSession, ConnectorTableHandle, ConnectorMvccSnapshot)} is a separate,
 *     stronger step: implement it only if handles are keyed by the PINNED schema's names, and then also
 *     declare {@link #supportsColumnHandleSnapshotPin} so the engine turns on its fail-loud check. A connector
 *     that recovers from a pinned-name miss by other means (rebuilding a field-id dictionary, for example)
 *     legitimately leaves both alone.</li>
 * <li><b>System tables</b>: {@link #listSupportedSysTables} plus {@link #getSysTableHandle};
 *     {@link #isPartitionValuesSysTable} only for a system table served by the engine's generic
 *     partition-values function rather than by a native scan.</li>
 * <li><b>Optional</b>: {@link #getTableComment}, {@link #renderShowCreateTableDdl}.</li>
 * </ul>
 *
 * <p>Note that {@link #getTableComment} addresses a table by NAME, not by handle. A heterogeneous gateway
 * connector routes foreign tables to a sibling by the concrete handle type, so it cannot route a name-only
 * method that way; if you are writing a gateway, handle it explicitly.</p>
 */
public interface ConnectorTableMetadataOps {

    /** Retrieves a table handle for the given database and table name. */
    @ConnectorMustImplement
    default Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName,
            String tableName) {
        return Optional.empty();
    }

    /**
     * Lists the system-table names supported for the given base table
     * (e.g. ["snapshots", "schemas", "options", "audit_log", "binlog"]).
     *
     * <p>The names are WITHOUT any "$" prefix; fe-core composes the
     * "{baseTable}${sysName}" reference name. Default: empty (no system
     * tables). Implemented by connectors that expose system tables.</p>
     */
    @ConnectorMustImplement(when = "the connector exposes system tables")
    default List<String> listSupportedSysTables(ConnectorSession session,
            ConnectorTableHandle baseTableHandle) {
        return Collections.emptyList();
    }

    /**
     * Returns a handle for the named system table of the given base table,
     * or empty if this connector does not expose that system table.
     *
     * <p>The returned handle is connector-internal and carries whatever the
     * connector needs (system-table name, scan-routing hints, etc.); it is
     * opaque to fe-core. {@code sysName} is the bare name (no "$").</p>
     */
    @ConnectorMustImplement(when = "the connector exposes system tables")
    default Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        return Optional.empty();
    }

    /**
     * Whether the named system table of {@code baseTableHandle} is served by the generic
     * {@code partition_values} table-valued function (fe-core's {@code PartitionsSysTable}) rather
     * than by a native connector scan. Default {@code false} (native, the {@link #getSysTableHandle}
     * path).
     *
     * <p>A connector whose partitioned tables expose their partition rows through the generic
     * partition-values TVF (e.g. hive) overrides this to return {@code true} for that sys-table name;
     * such a name need NOT return a handle from {@link #getSysTableHandle} (the TVF path never consults
     * it). fe-core needs the kind at discovery time (before any handle is fetched), so it cannot be
     * inferred from an empty {@code getSysTableHandle}. {@code sysName} is the bare name (no
     * {@code "$"}).</p>
     */
    @ConnectorMustImplement(when = "a system table is served by the generic partition-values function")
    default boolean isPartitionValuesSysTable(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        return false;
    }

    /** Returns the schema (columns, format, etc.) for the given table. */
    @ConnectorMustImplement
    default ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "getTableSchema not implemented");
    }

    /**
     * Returns the schema AT {@code snapshot.getSchemaId()} &mdash; the schema as of the
     * pinned snapshot, for time-travel reads under schema evolution.
     *
     * <p>The default ignores the snapshot and returns the latest schema via
     * {@link #getTableSchema(ConnectorSession, ConnectorTableHandle)}. A connector that
     * supports schema-at-snapshot overrides this to resolve the schema version.</p>
     */
    @ConnectorMustImplement(when = "the connector supports time travel or schema evolution")
    default ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        return getTableSchema(session, handle);
    }

    /**
     * Renders the native {@code SHOW CREATE TABLE} DDL for a table, fetching schema FRESH from the underlying
     * metastore at call time (bypassing any connector-side table cache) so the returned statement always
     * reflects the latest remote schema.
     *
     * <p>This is a LAZY, per-call interception point used ONLY by {@code ShowCreateTableCommand}. It intentionally
     * does NOT participate in the {@code SUPPORTS_SHOW_CREATE_DDL} capability (which gates the engine-assembled
     * DDL in {@code Env.getDdlStmt} for every caller, including delegated sibling tables and the HTTP schema
     * endpoint). A connector that does not natively render its own SHOW CREATE returns {@link Optional#empty()},
     * and the command falls through to the generic {@code Env.getDdlStmt} path unchanged.</p>
     *
     * @return the full {@code CREATE TABLE} statement, or {@link Optional#empty()} to defer to the engine
     */
    default Optional<String> renderShowCreateTableDdl(
            ConnectorSession session, ConnectorTableHandle handle) {
        return Optional.empty();
    }

    /** Returns a name-to-handle map for all columns of the table. */
    @ConnectorMustImplement
    default Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "getColumnHandles not implemented");
    }

    /**
     * Returns a name-to-handle map for all columns AT {@code snapshot.getSchemaId()} &mdash; the
     * columns as of the pinned snapshot, for time-travel reads under schema evolution.
     *
     * <p>The default ignores the snapshot and returns the latest columns via
     * {@link #getColumnHandles(ConnectorSession, ConnectorTableHandle)}. WHY this exists: the generic
     * scan node builds column handles BEFORE it pins the snapshot, so without a snapshot-aware overload
     * it can only key handles by the LATEST names. A time-travel query binds its slots to the PINNED
     * (old) names, so after a RENAME the renamed column's slot misses the latest-keyed map and is
     * silently dropped &mdash; crashing BE with a field-id dictionary miss on connectors whose native
     * projection is name/ordinal-driven (paimon). A connector that supports schema-at-snapshot
     * overrides this to key handles by the pinned names (mirrors the
     * {@link #getTableSchema(ConnectorSession, ConnectorTableHandle, ConnectorMvccSnapshot)} split) and
     * declares {@link #supportsColumnHandleSnapshotPin(ConnectorSession)}.</p>
     */
    @ConnectorMustImplement(when = "column handles must be keyed by the pinned schema's names")
    default Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        return getColumnHandles(session, handle);
    }

    /**
     * Whether {@link #getColumnHandles(ConnectorSession, ConnectorTableHandle, ConnectorMvccSnapshot)}
     * resolves handles AT the pinned snapshot's schema (i.e. keys them by the pinned names).
     *
     * <p>Only a connector that returns {@code true} is subject to the generic node's fail-loud check
     * that every bound column present in the pinned schema has a handle: for such a connector a missing
     * pinned column is a genuine bug (the connector promised pinned handles but dropped one) and must
     * fail with a clear error rather than being silently dropped into a BE crash. A connector that
     * returns {@code false} keeps the legacy latest-keyed handles and recovers from the drop by its own
     * means (e.g. iceberg rebuilds its field-id dictionary from the full pinned schema), so it is left
     * on the unchanged silent-skip path.</p>
     */
    @ConnectorMustImplement(when = "column handles are keyed by the pinned schema's names")
    default boolean supportsColumnHandleSnapshotPin(ConnectorSession session) {
        return false;
    }

    /** Lists all table names within the given database. */
    @ConnectorMustImplement
    default List<String> listTableNames(ConnectorSession session,
            String dbName) {
        return Collections.emptyList();
    }

    /** Returns a human-readable comment for the given table. */
    default String getTableComment(ConnectorSession session,
            String dbName, String tableName) {
        return "";
    }

    /**
     * Builds the Thrift {@code TTableDescriptor} that BE needs for query execution.
     *
     * <p>Each connector constructs its own typed descriptor (e.g., {@code TJdbcTable},
     * {@code TEsTable}) and wraps it in a {@code TTableDescriptor}. This keeps
     * connector-specific Thrift logic out of fe-core.</p>
     *
     * <p>The Thrift classes are provided by fe-thrift at compile time and loaded
     * from the parent classloader at runtime.</p>
     *
     * @param session connector session
     * @param tableId Doris internal table ID
     * @param tableName table display name
     * @param dbName database name
     * @param remoteName remote table name in the external data source
     * @param numCols number of columns in the schema
     * @param catalogId Doris internal catalog ID
     * @return the table descriptor, or {@code null} if the connector does not
     *         need a typed descriptor (fe-core will use a generic fallback)
     */
    @ConnectorMustImplement
    default org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        return null;
    }
}
