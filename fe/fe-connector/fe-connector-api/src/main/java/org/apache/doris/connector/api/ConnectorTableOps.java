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

import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorColumnPath;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.ddl.TagChange;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Operations on tables within a connector catalog.
 */
public interface ConnectorTableOps {

    /** Retrieves a table handle for the given database and table name. */
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
    default boolean isPartitionValuesSysTable(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        return false;
    }

    /** Returns the schema (columns, format, etc.) for the given table. */
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
    default Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "getColumnHandles not implemented");
    }

    /** Lists all table names within the given database. */
    default List<String> listTableNames(ConnectorSession session,
            String dbName) {
        return Collections.emptyList();
    }

    /**
     * Returns whether the named view exists in the given database. Connectors that expose views
     * (declaring {@link ConnectorCapability#SUPPORTS_VIEW}) override this; the default {@code false}
     * keeps view-less connectors reporting every object as a non-view.
     */
    default boolean viewExists(ConnectorSession session, String dbName, String viewName) {
        return false;
    }

    /**
     * Lists all view names within the given database. Connectors that subtract views from
     * {@link #listTableNames} (e.g. iceberg) expose them here so the catalog can merge them back into
     * {@code SHOW TABLES}; the default is empty (no view support).
     */
    default List<String> listViewNames(ConnectorSession session, String dbName) {
        return Collections.emptyList();
    }

    /**
     * Loads the {@link ConnectorViewDefinition stored SQL definition + dialect} of the named view. Connectors
     * that expose views (declaring {@link ConnectorCapability#SUPPORTS_VIEW}) override this; callers gate on
     * {@code SUPPORTS_VIEW} and {@code isView()} so the default — for view-less connectors — fails loud.
     *
     * @throws DorisConnectorException if the connector does not support views
     */
    default ConnectorViewDefinition getViewDefinition(ConnectorSession session, String dbName, String viewName) {
        throw new DorisConnectorException("GET VIEW DEFINITION not supported");
    }

    /**
     * Drops the named view. Connectors that expose views (declaring {@link ConnectorCapability#SUPPORTS_VIEW})
     * override this; callers route a DROP through {@link #viewExists} so the default — for view-less
     * connectors — is unreachable and fails loud as a guard.
     *
     * @throws DorisConnectorException if the connector does not support views
     */
    default void dropView(ConnectorSession session, String dbName, String viewName) {
        throw new DorisConnectorException("DROP VIEW not supported");
    }

    /** Creates a new table with the given schema and properties. */
    default void createTable(ConnectorSession session,
            ConnectorTableSchema schema,
            Map<String, String> properties) {
        throw new DorisConnectorException(
                "CREATE TABLE not supported");
    }

    /**
     * Creates a table with full DDL semantics (partition, bucket, external,
     * {@code IF NOT EXISTS}).
     *
     * <p>Connectors should override this when they support advanced
     * {@code CREATE TABLE} options. The default degrades to the legacy
     * {@link #createTable(ConnectorSession, ConnectorTableSchema, Map)},
     * dropping partition / bucket / external / {@code ifNotExists} info.</p>
     *
     * @throws DorisConnectorException if the connector cannot honor the request
     */
    default void createTable(ConnectorSession session,
            ConnectorCreateTableRequest request) {
        ConnectorTableSchema schema = new ConnectorTableSchema(
                request.getTableName(),
                request.getColumns(),
                null,
                request.getProperties());
        createTable(session, schema, request.getProperties());
    }

    /** Drops the specified table. */
    default void dropTable(ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "DROP TABLE not supported");
    }

    /** Renames the table identified by {@code handle} to {@code newName} within the same database. */
    default void renameTable(ConnectorSession session,
            ConnectorTableHandle handle, String newName) {
        throw new DorisConnectorException(
                "RENAME TABLE not supported");
    }

    /**
     * Truncates the table identified by {@code handle}. When {@code partitions} is non-empty only those
     * partitions are truncated; {@code null} / empty truncates the whole table.
     *
     * <p>Connectors that support {@code TRUNCATE TABLE} override this. The default throws, matching the
     * pre-flip behavior of the generic bridge (which had no truncate route for the SPI path).</p>
     *
     * @throws DorisConnectorException if the connector does not support truncate
     */
    default void truncateTable(ConnectorSession session,
            ConnectorTableHandle handle, List<String> partitions) {
        throw new DorisConnectorException(
                "TRUNCATE TABLE not supported");
    }

    /**
     * Adds a column to the table at the given position.
     *
     * @param position where to place the column ({@link ConnectorColumnPosition#FIRST} /
     *        {@link ConnectorColumnPosition#after(String)}); {@code null} appends at the end.
     */
    default void addColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("ADD COLUMN not supported");
    }

    /** Adds multiple columns to the table, appended in order. */
    default void addColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        throw new DorisConnectorException("ADD COLUMNS not supported");
    }

    /** Drops the named column from the table. */
    default void dropColumn(ConnectorSession session, ConnectorTableHandle handle,
            String columnName) {
        throw new DorisConnectorException("DROP COLUMN not supported");
    }

    /** Renames a column. */
    default void renameColumn(ConnectorSession session, ConnectorTableHandle handle,
            String oldName, String newName) {
        throw new DorisConnectorException("RENAME COLUMN not supported");
    }

    /**
     * Modifies a column's type and/or comment, optionally repositioning it.
     *
     * @param position where to move the column; {@code null} keeps its current position.
     */
    default void modifyColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("MODIFY COLUMN not supported");
    }

    /** Reorders the table's columns to match the given full ordered list of column names. */
    default void reorderColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<String> newOrder) {
        throw new DorisConnectorException("REORDER COLUMNS not supported");
    }

    // ---- Nested (dotted-path) column evolution ----
    // #65329 dotted column paths (e.g. `s.b`, `arr.element.c`, `m.value`) are carried neutrally by
    // {@link ConnectorColumnPath}; a single-part path targets a top-level column. Connectors that support
    // nested column schema evolution (iceberg) override these; others inherit the throwing default. The
    // fe-core bridge routes top-level ADD/DROP/RENAME/MODIFY through the flat ops above and reserves these
    // {@code *NestedColumn} ops for the nested case, except {@link #modifyColumnComment}, which is the sole
    // entrypoint for the new `MODIFY COLUMN ... COMMENT` op (flat and nested alike). Distinct names (rather
    // than overloads of the flat String/ConnectorColumn ops) keep {@code Mockito.any()} / null call sites in
    // connector tests unambiguous.

    /**
     * Adds a field at {@code path} (the full path of the new field: its parent struct plus the new leaf name).
     *
     * @param position where to place the new field within its parent struct; {@code null} appends at the end.
     */
    default void addNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("nested ADD COLUMN not supported");
    }

    /** Drops the field at {@code path}. */
    default void dropNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path) {
        throw new DorisConnectorException("nested DROP COLUMN not supported");
    }

    /** Renames the field at {@code path} to {@code newName} (a leaf name, not a path). */
    default void renameNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String newName) {
        throw new DorisConnectorException("nested RENAME COLUMN not supported");
    }

    /**
     * Modifies the field at {@code path} (type / comment / nullability), optionally repositioning it within
     * its parent struct.
     *
     * @param position where to move the field; {@code null} keeps its current position.
     */
    default void modifyNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        throw new DorisConnectorException("nested MODIFY COLUMN not supported");
    }

    /** Sets (or clears, with {@code ""}) the comment/doc of the field at {@code path}. */
    default void modifyColumnComment(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String comment) {
        throw new DorisConnectorException("MODIFY COLUMN COMMENT not supported");
    }

    /** Creates or replaces a named branch (snapshot ref) on the table. */
    default void createOrReplaceBranch(ConnectorSession session, ConnectorTableHandle handle,
            BranchChange branch) {
        throw new DorisConnectorException("CREATE/REPLACE BRANCH not supported");
    }

    /** Creates or replaces a named tag (snapshot ref) on the table. */
    default void createOrReplaceTag(ConnectorSession session, ConnectorTableHandle handle,
            TagChange tag) {
        throw new DorisConnectorException("CREATE/REPLACE TAG not supported");
    }

    /** Drops a named branch (snapshot ref) from the table. */
    default void dropBranch(ConnectorSession session, ConnectorTableHandle handle,
            DropRefChange branch) {
        throw new DorisConnectorException("DROP BRANCH not supported");
    }

    /** Drops a named tag (snapshot ref) from the table. */
    default void dropTag(ConnectorSession session, ConnectorTableHandle handle,
            DropRefChange tag) {
        throw new DorisConnectorException("DROP TAG not supported");
    }

    /** Adds a partition field (column reference + optional transform) to the table's partition spec. */
    default void addPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        throw new DorisConnectorException("ADD PARTITION FIELD not supported");
    }

    /** Drops a partition field from the table's partition spec. */
    default void dropPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        throw new DorisConnectorException("DROP PARTITION FIELD not supported");
    }

    /** Replaces a partition field (removes the old field, adds the new one) in the table's partition spec. */
    default void replacePartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        throw new DorisConnectorException("REPLACE PARTITION FIELD not supported");
    }

    /** Returns the primary key column names for the given table. */
    default List<String> getPrimaryKeys(ConnectorSession session,
            String dbName, String tableName) {
        return Collections.emptyList();
    }

    /** Returns a human-readable comment for the given table. */
    default String getTableComment(ConnectorSession session,
            String dbName, String tableName) {
        return "";
    }

    /**
     * Executes a DML statement (INSERT/UPDATE/DELETE) directly.
     * Used for DML passthrough features like CALL EXECUTE_STMT.
     */
    default void executeStmt(ConnectorSession session, String stmt) {
        throw new DorisConnectorException("executeStmt not supported");
    }

    /**
     * Gets column metadata from a query string via PreparedStatement metadata.
     * Used for table-valued functions like query().
     */
    default ConnectorTableSchema getColumnsFromQuery(ConnectorSession session, String query) {
        throw new DorisConnectorException("getColumnsFromQuery not supported");
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
    default org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        return null;
    }

    /**
     * Lists all partition display names (e.g., {@code "year=2024/month=01"}).
     *
     * <p>Should be cheap and avoid loading per-partition metadata.</p>
     */
    default List<String> listPartitionNames(ConnectorSession session,
            ConnectorTableHandle handle) {
        return Collections.emptyList();
    }

    /**
     * Lists partitions matching the optional filter, with full metadata.
     *
     * <p>Connectors should push the filter into the metastore / catalog when
     * possible. {@code filter} is empty when the caller wants the full list.</p>
     */
    default List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter) {
        return Collections.emptyList();
    }

    /**
     * Lists distinct partition column value combinations for the given columns.
     *
     * <p>Used by the {@code partition_values()} TVF and by column-distinct-value
     * optimizations. Inner list order matches {@code partitionColumns}.</p>
     */
    default List<List<String>> listPartitionValues(ConnectorSession session,
            ConnectorTableHandle handle,
            List<String> partitionColumns) {
        return Collections.emptyList();
    }
}
