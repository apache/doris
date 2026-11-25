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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.search.ConnectorSearch;
import org.apache.doris.connector.spi.search.ConnectorSearchFunction;
import org.apache.doris.connector.spi.search.ConnectorSearchLazyMaterialization;
import org.apache.doris.connector.spi.search.ConnectorSearchOps;
import org.apache.doris.connector.spi.search.ConnectorSearchRequest;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;

/**
 * Serves {@code SHOW DATABASES} / {@code SHOW TABLES} / {@code DESC}, the statement's version pin, time
 * travel, and {@code vector_search} / {@code full_text_search} for a Lance catalog.
 *
 * <p>Created per statement by the engine's metadata funnel; it holds no cross-statement state. Reads of one
 * table version share one metadata read through {@link LanceStatementScope}.
 */
final class LanceConnectorMetadata implements ConnectorMetadata, ConnectorSearchOps {

    /** Informational format name the engine records with the schema. */
    private static final String TABLE_FORMAT_TYPE = "LANCE";
    /**
     * The fe-core session variable that turns the lazy fetch of a search's columns on, byte-for-byte
     * {@code SessionVariable.ENABLE_LANCE_LAZY_MATERIALIZATION}; absent means on, its default.
     */
    static final String SESSION_ENABLE_LAZY_MATERIALIZATION = "enable_lance_lazy_materialization";
    /**
     * The first BE exec version whose TopN lazy materialization reads a row by its Lance dataset row location,
     * byte-for-byte fe-common {@code Config.LANCE_ROW_LOCATION_MIN_BE_EXEC_VERSION}. An older backend would read the
     * row location of a search as another kind of row id and fetch the wrong rows, so below it a search is not
     * lazily materialized.
     */
    static final int LANCE_ROW_LOCATION_MIN_BE_EXEC_VERSION = 20;

    private final LanceCatalogClient client;

    LanceConnectorMetadata(LanceCatalogClient client) {
        this.client = client;
    }

    // ========= ConnectorSchemaOps =========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return client.listDatabaseNames();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return client.listDatabaseNames().contains(dbName);
    }

    // ========= ConnectorTableMetadataOps =========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return client.listTableNames(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        return client.tableExists(dbName, tableName)
                ? Optional.of(new LanceTableHandle(dbName, tableName)) : Optional.empty();
    }

    /** The schema of the handle's pinned version, or of the newest version when it has no pin. */
    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        LanceTableHandle lanceHandle = (LanceTableHandle) handle;
        return toTableSchema(lanceHandle, metadata(session, lanceHandle));
    }

    /** The schema of a time-travel pin: each Lance version carries its own schema. */
    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        LanceTableHandle lanceHandle = pinned((LanceTableHandle) handle, snapshot);
        return toTableSchema(lanceHandle, metadata(session, lanceHandle));
    }

    /** The table's columns; for a search, also the search's result column, which its ranges fill. */
    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (handle instanceof LanceSearchTableHandle) {
            LanceSearchTableHandle search = (LanceSearchTableHandle) handle;
            Map<String, ConnectorColumnHandle> handles = toColumnHandles(metadata(session, search.getTable()));
            String resultColumn = searchFunction(search).getResultColumn();
            handles.put(resultColumn, new NamedColumnHandle(resultColumn));
            return handles;
        }
        return toColumnHandles(metadata(session, (LanceTableHandle) handle));
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        return toColumnHandles(metadata(session, pinned((LanceTableHandle) handle, snapshot)));
    }

    @Override
    public boolean supportsColumnHandleSnapshotPin(ConnectorSession session) {
        return true;
    }

    /**
     * Lance has no table descriptor of its own, so it borrows the Hive one, as 4.x Lance tables did and as
     * the other connectors that read through the generic file-scan path do. Leaving the SPI default (null)
     * is not neutral: fe-core would fall back to {@code SCHEMA_TABLE} and BE would build the wrong
     * descriptor.
     */
    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        THiveTable hiveTable = new THiveTable(dbName, tableName, new HashMap<>());
        TTableDescriptor descriptor = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
        descriptor.setHiveTable(hiveTable);
        return descriptor;
    }

    // ========= ConnectorStatisticsOps =========

    /** The logical row count of the handle's version: the fragments' rows minus their deletions. */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(ConnectorSession session,
            ConnectorTableHandle handle) {
        LanceTableHandle lanceHandle = (LanceTableHandle) handle;
        LanceTableMetadata metadata = metadata(session, lanceHandle);
        return Optional.of(new ConnectorTableStatistics(metadata.getRowCount(), -1));
    }

    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        return getTableStatistics(session, pinned((LanceTableHandle) handle, snapshot));
    }

    // ========= ConnectorPushdownOps =========

    /**
     * A conjunct holding a CAST stays in Doris. The engine would strip the CAST and hand over the bare column
     * comparison, which Lance would then evaluate in the column's own type, not the one Doris compares in.
     */
    @Override
    public boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return false;
    }

    // ========= ConnectorSearchOps =========

    /**
     * Prepares a {@code vector_search} or {@code full_text_search} over {@code table}, pinned to the version the
     * statement reads: the newest one, read through the same statement memo as a plain scan of the table and its
     * query-begin pin, so a statement that also reads the table reads one version. The arguments are
     * {@link LanceSearchArguments}, checked before the table is read as far as its schema does not decide them; a
     * search that may use an index needs the searched column's Lance field id.
     */
    @Override
    public ConnectorSearch prepareSearch(ConnectorSession session, ConnectorTableHandle table,
            ConnectorSearchRequest request) {
        LanceSearchArguments arguments = LanceSearchArguments.parse(request);
        LanceTableHandle lanceTable = (LanceTableHandle) table;
        LanceTableMetadata metadata = metadata(session, lanceTable);
        LanceTableHandle pinned = lanceTable.withVersion(metadata.getVersion());
        LanceSearchArguments.Bound search = arguments.bind(metadata.getSchema());
        int searchFieldId = arguments.searchesIndex()
                ? lanceFieldId(session, pinned, metadata, search.getSearchField(), request.getFunction()) : -1;
        return new ConnectorSearch(new LanceSearchTableHandle(pinned, search.getRequest(), searchFieldId),
                searchColumns(metadata, request.getFunction()), arguments.getTopK(), arguments.getOffset(),
                lazyMaterialization(session));
    }

    /**
     * Records the columns of a search that the engine reads by row id after merging the candidates: the backend
     * that ran the search reads them, so they count toward the scan's backend requirements.
     */
    @Override
    public ConnectorTableHandle applyTopnLazyMaterialization(ConnectorSession session, ConnectorTableHandle handle,
            List<String> lazyColumns) {
        return handle instanceof LanceSearchTableHandle
                ? ((LanceSearchTableHandle) handle).withLazyColumns(lazyColumns) : handle;
    }

    // ========= MVCC =========

    /**
     * Pins the newest version in storage for the statement. The version is also what MTMV freshness
     * compares, and Lance versions only grow on the main chain.
     */
    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(ConnectorSession session,
            ConnectorTableHandle handle) {
        LanceTableMetadata metadata = metadata(session, ((LanceTableHandle) handle).unpinned());
        // Every dataset version owns its complete schema. Retain it before the engine's independent
        // schema-cache loader can observe a newer external commit.
        return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(metadata.getVersion())
                .schemaId(metadata.getVersion()).retainSchema(true).build());
    }

    /**
     * {@code FOR VERSION AS OF <n>} opens version {@code n}; {@code FOR TIME AS OF '<time>'} opens the
     * main-chain version committed last at or before that time in the session time zone. Tags, branches
     * and scan options are not supported yet and fail rather than read the newest version.
     */
    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
        LanceTableHandle lanceHandle = (LanceTableHandle) handle;
        LanceTableMetadata metadata;
        switch (spec.getKind()) {
            case SNAPSHOT_ID:
            case VERSION_REF:
                long version = LanceSnapshotResolver.parseVersion(spec.getStringValue());
                metadata = metadata(session, lanceHandle.withVersion(version));
                break;
            case TIMESTAMP:
                metadata = metadataAtOrBefore(session, lanceHandle, spec.getStringValue());
                break;
            case TAG:
            case BRANCH:
                throw new DorisConnectorException("Lance tables do not support @"
                        + spec.getKind().name().toLowerCase(Locale.ROOT) + " yet; read a version with FOR VERSION AS OF"
                        + " or FOR TIME AS OF instead");
            default:
                throw new DorisConnectorException("Lance tables do not support " + spec.getKind()
                        + " scan parameters");
        }
        return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(metadata.getVersion()).build());
    }

    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        return pinned((LanceTableHandle) handle, snapshot);
    }

    // ========= internals =========

    private LanceTableMetadata metadata(ConnectorSession session, LanceTableHandle handle) {
        // Looked up before the memo's loader runs: a loader may not use the statement scope itself.
        LanceMetadataMetrics metrics = LanceMetadataMetrics.forTable(session, handle.getDbName(),
                handle.getTableName());
        return LanceStatementScope.sharedMetadata(session, handle,
                () -> client.loadTableMetadata(handle.getDbName(), handle.getTableName(), handle.getVersion(),
                        metrics));
    }

    private LanceTableMetadata metadataAtOrBefore(ConnectorSession session, LanceTableHandle handle,
            String requestedText) {
        OptionalLong timestamp = LanceSnapshotResolver.parseTimestamp(requestedText,
                LanceSnapshotResolver.sessionZone(session.getTimeZone()));
        if (!timestamp.isPresent()) {
            throw new DorisConnectorException("Cannot parse Lance FOR TIME AS OF value '" + requestedText
                    + "', expected " + LanceSnapshotResolver.TIMESTAMP_FORMATS);
        }
        LanceTableMetadata metadata = client.loadTableMetadataAtOrBefore(handle.getDbName(),
                handle.getTableName(), timestamp.getAsLong(), requestedText,
                LanceMetadataMetrics.forTable(session, handle.getDbName(), handle.getTableName()));
        // Remembered under the version it resolved to, which is what the pinned reads that follow ask for.
        return LanceStatementScope.sharedMetadata(session, handle.withVersion(metadata.getVersion()),
                () -> metadata);
    }

    /** The handle pinned to {@code snapshot}; an empty pin (no snapshot id) leaves it unpinned. */
    private static LanceTableHandle pinned(LanceTableHandle handle, ConnectorMvccSnapshot snapshot) {
        return snapshot.getSnapshotId() > 0 ? handle.withVersion(snapshot.getSnapshotId()) : handle;
    }

    /**
     * No table properties: SHOW CREATE TABLE renders them unmasked, and nothing reads them for a Lance
     * table, so the catalog's storage credentials are kept out by sending none.
     */
    private static ConnectorTableSchema toTableSchema(LanceTableHandle handle, LanceTableMetadata metadata) {
        return new ConnectorTableSchema(handle.getTableName(), LanceTypeMapping.toColumns(metadata.getSchema()),
                TABLE_FORMAT_TYPE, Collections.emptyMap());
    }

    /** The Lance field id of a search's column, from the index metadata the search's planning reads too. */
    private int lanceFieldId(ConnectorSession session, LanceTableHandle pinned, LanceTableMetadata metadata,
            Field field, ConnectorSearchFunction function) {
        // Looked up before the memo's loader runs: a loader may not use the statement scope itself.
        LanceMetadataMetrics metrics = LanceMetadataMetrics.forTable(session, pinned.getDbName(),
                pinned.getTableName());
        LanceIndexMetadata indexMetadata = LanceStatementScope.sharedIndexMetadata(session, pinned, metadata,
                () -> client.loadIndexMetadata(pinned.getDbName(), pinned.getTableName(), metadata.getVersion(),
                        metrics));
        OptionalInt fieldId = indexMetadata.getLanceFieldId(field.getName());
        if (!fieldId.isPresent()) {
            throw new DorisConnectorException("Lance "
                    + (function == ConnectorSearchFunction.VECTOR_SEARCH ? "vector" : "full-text") + " column '"
                    + field.getName() + "' has no field ID in the Lance schema");
        }
        return fieldId.getAsInt();
    }

    /**
     * The table's columns and then the function's nullable FLOAT result column. A name the engine could not tell
     * apart from another, case-insensitively, is rejected, as is a table column named like the result column.
     */
    private static List<ConnectorColumn> searchColumns(LanceTableMetadata metadata,
            ConnectorSearchFunction function) {
        List<ConnectorColumn> columns = new ArrayList<>(LanceTypeMapping.toColumns(metadata.getSchema()));
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ConnectorColumn column : columns) {
            if (!names.add(column.getName())) {
                throw new DorisConnectorException("Duplicate Lance schema column under case-insensitive matching: '"
                        + column.getName() + "'");
            }
        }
        String resultColumn = function.getResultColumn();
        if (names.contains(resultColumn)) {
            throw new DorisConnectorException("Lance table already contains reserved "
                    + (function == ConnectorSearchFunction.VECTOR_SEARCH ? "vector search" : "full-text search")
                    + " column '" + resultColumn + "'");
        }
        columns.add(new ConnectorColumn(resultColumn, ConnectorType.of("FLOAT"), null, true, null));
        return columns;
    }

    /**
     * Whether the engine may fetch a search's columns by row id after merging the candidates: unless the session
     * turns it off, and only when every backend reads Lance row locations.
     */
    private static ConnectorSearchLazyMaterialization lazyMaterialization(ConnectorSession session) {
        String enabled = session.getSessionProperties().get(SESSION_ENABLE_LAZY_MATERIALIZATION);
        return (enabled == null || Boolean.parseBoolean(enabled))
                && session.getBeExecVersion() >= LANCE_ROW_LOCATION_MIN_BE_EXEC_VERSION
                ? ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS : ConnectorSearchLazyMaterialization.NONE;
    }

    private static ConnectorSearchFunction searchFunction(LanceSearchTableHandle search) {
        return search.isVectorSearch() ? ConnectorSearchFunction.VECTOR_SEARCH
                : ConnectorSearchFunction.FULL_TEXT_SEARCH;
    }

    private static Map<String, ConnectorColumnHandle> toColumnHandles(LanceTableMetadata metadata) {
        List<Field> fields = metadata.getSchema().getFields();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (Field field : fields) {
            handles.put(field.getName(), new NamedColumnHandle(field.getName()));
        }
        return handles;
    }
}
