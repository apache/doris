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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Metadata facade backed by a Delta catalog adapter. */
public final class DeltaConnectorMetadata implements ConnectorMetadata {

    private static final String IN_COMMIT_TIMESTAMPS = "delta.enableInCommitTimestamps";
    // Kernel enables inCommitTimestamp as a dependency of catalogManaged when committing.
    private static final Set<String> SUPPORTED_CATALOG_MANAGED_WRITER_FEATURES =
            Set.of("catalogManaged", "inCommitTimestamp", "vacuumProtocolCheck");

    private final DeltaCatalogAdapter catalogAdapter;
    private final Map<String, String> properties;
    private final DeltaKernelWriter writer;
    private final boolean writeEnabled;
    private final Map<DeltaTableHandle, DeltaTableHandle> historicalHandles = new LinkedHashMap<>();

    public DeltaConnectorMetadata(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties) {
        this(catalogAdapter, properties, null);
    }

    public DeltaConnectorMetadata(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties, DeltaKernelWriter writer) {
        this.catalogAdapter = catalogAdapter;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.writer = writer;
        this.writeEnabled = Boolean.parseBoolean(
                properties.getOrDefault(DeltaConnectorProperties.WRITE_ENABLED, "false"));
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return catalogAdapter.listDatabaseNames();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return catalogAdapter.databaseExists(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return catalogAdapter.listTableNames(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        return DeltaStatementScope.resolve(session, catalogAdapter, dbName, tableName)
                .map(handle -> (ConnectorTableHandle) handle);
    }

    ConnectorTableHandle applyTableSnapshot(ConnectorSession session,
            ConnectorTableHandle handle, DeltaTableSnapshot snapshot) {
        return catalogAdapter.applyTableSnapshot((DeltaTableHandle) handle, snapshot);
    }

    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(ConnectorSession session,
            ConnectorTableHandle handle) {
        return Optional.of(toMvccSnapshot((DeltaTableHandle) handle));
    }

    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
        DeltaTableSnapshot boundary;
        switch (spec.getKind()) {
            case SNAPSHOT_ID:
                boundary = DeltaTableSnapshot.version(Long.parseLong(spec.getStringValue()));
                break;
            case TIMESTAMP:
                boundary = DeltaTableSnapshot.timestampMillis(spec.isDigital()
                        ? Long.parseLong(spec.getStringValue()) : parseTimestampMillis(session, spec.getStringValue()));
                break;
            default:
                throw new UnsupportedOperationException("Native Delta does not support time-travel kind "
                        + spec.getKind());
        }
        DeltaTableHandle requested = catalogAdapter.applyTableSnapshot((DeltaTableHandle) handle, boundary);
        historicalHandles.put(requested, requested);
        return Optional.of(toMvccSnapshot(requested));
    }

    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        DeltaTableHandle current = (DeltaTableHandle) handle;
        String versionProperty = snapshot.getProperties().get("delta.snapshot.version");
        if (versionProperty != null && Long.parseLong(versionProperty) != snapshot.getSnapshotId()) {
            throw new DorisConnectorException("Delta snapshot properties disagree with the pinned version");
        }
        if (current.getSnapshotVersion() == snapshot.getSnapshotId()) {
            return current;
        }
        DeltaTableHandle key = current.withSnapshotVersion(snapshot.getSnapshotId());
        return historicalHandles.computeIfAbsent(key,
                ignored -> catalogAdapter.applyTableSnapshot(current,
                        DeltaTableSnapshot.version(snapshot.getSnapshotId())));
    }

    private static ConnectorMvccSnapshot toMvccSnapshot(DeltaTableHandle handle) {
        return ConnectorMvccSnapshot.builder().snapshotId(handle.getSnapshotVersion())
                .schemaId(handle.getSnapshotVersion())
                .property("delta.snapshot.version", Long.toString(handle.getSnapshotVersion())).build();
    }

    private static long parseTimestampMillis(ConnectorSession session, String value) {
        String normalized = value.replace(' ', 'T');
        try {
            return OffsetDateTime.parse(normalized).toInstant().toEpochMilli();
        } catch (DateTimeParseException noOffset) {
            ZoneId zone = ZoneId.of(session == null ? "UTC" : session.getTimeZone());
            try {
                return normalized.length() == 10
                        ? LocalDate.parse(normalized).atStartOfDay(zone).toInstant().toEpochMilli()
                        : LocalDateTime.parse(normalized).atZone(zone).toInstant().toEpochMilli();
            } catch (DateTimeParseException invalidTimestamp) {
                throw new DorisConnectorException("Invalid Delta time-travel timestamp: " + value, invalidTimestamp);
            }
        }
    }

    @Override
    public void createTable(
            ConnectorSession session, ConnectorCreateTableRequest request) {
        requireWriteEnabled();
        DeltaCreateTableValidator.partitionColumns(request);
        if (!catalogAdapter.supportsCreateTable()) {
            throw new UnsupportedOperationException(
                    "This native Delta catalog adapter does not support CREATE TABLE");
        }
        for (ConnectorColumn column : request.getColumns()) {
            if (column.getDefaultValue() != null) {
                throw new UnsupportedOperationException(
                        "Native Delta CREATE TABLE does not support column defaults");
            }
        }
        catalogAdapter.createTable(request);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
        if (!writeEnabled || !catalogAdapter.supportsDropTable()) {
            throw new UnsupportedOperationException(
                    "Native Delta DROP TABLE requires delta.write.enabled=true, "
                            + "delta.drop.enabled=true, and a Unity catalog");
        }
        catalogAdapter.dropTable((DeltaTableHandle) handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        List<ConnectorColumn> columns = toColumns(snapshot.getSchema());
        Map<String, String> tableProperties = new LinkedHashMap<>();
        tableProperties.put("location", snapshot.getTablePath());
        tableProperties.put("delta.snapshot.version", String.valueOf(snapshot.getVersion()));
        return new ConnectorTableSchema(deltaHandle.getTableName(), columns,
                "DELTA", tableProperties);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        return getTableSchema(session, snapshot == null ? handle : applySnapshot(session, handle, snapshot));
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        StructType schema = catalogAdapter.loadSnapshot(deltaHandle).getSchema();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
        for (StructField field : schema.fields()) {
            handles.put(field.getName(), new NamedColumnHandle(field.getName()));
        }
        return handles;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        return getColumnHandles(session, snapshot == null ? handle : applySnapshot(session, handle, snapshot));
    }

    @Override
    public boolean supportsColumnHandleSnapshotPin(ConnectorSession session) {
        return true;
    }

    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session) {
        requireWriteEnabled();
        return new DeltaConnectorTransaction(session.allocateTransactionId(), this);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle handle, List<String> partitions) {
        if (partitions != null && !partitions.isEmpty()) {
            throw new UnsupportedOperationException("Native Delta TRUNCATE PARTITION is not supported");
        }
        truncateTable(session, handle);
    }

    @Override
    public void close() {
        historicalHandles.clear();
    }

    private boolean supportsInsert() {
        return writeEnabled && (writer != null || catalogAdapter.supportsInsert());
    }

    DeltaWriteConfig getWriteConfig(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        requireExternalWriteCapability(deltaHandle);
        if (!deltaHandle.isExternalTable() && !deltaHandle.isCatalogManaged()) {
            throw new UnsupportedOperationException(
                    "Ordinary Unity managed Delta writes are not supported; "
                            + "the table must enable the catalogManaged feature");
        }
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        validateWriteProtocol(deltaHandle, snapshot);
        List<ConnectorColumn> tableColumns = toColumns(snapshot.getSchema());
        if (!hasSameWriteSchema(tableColumns, columns)) {
            throw new UnsupportedOperationException(
                    "The initial native Delta writer requires every table column in schema order; "
                            + "expected " + tableColumns + " but received " + columns);
        }
        Map<String, String> storageProperties = getBackendStoragePropertiesForWrite(deltaHandle);
        DeltaVendedCredentialLifetime.validateWrite(session, storageProperties,
                DeltaConnectorProperties.positiveLongProperty(properties,
                        DeltaConnectorProperties.UNITY_CREDENTIAL_MIN_LIFETIME_MS,
                        DeltaConnectorProperties.DEFAULT_UNITY_CREDENTIAL_MIN_LIFETIME_MS));
        return new DeltaWriteConfig(deltaHandle.getTablePath(), snapshot.getPartitionColumnNames(), storageProperties);
    }

    private static void validateWriteProtocol(DeltaTableHandle handle, DeltaKernelSnapshot snapshot) {
        if (handle.isCatalogManaged()) {
            if (!Boolean.parseBoolean(snapshot.getTableProperties().get(IN_COMMIT_TIMESTAMPS))) {
                throw new UnsupportedOperationException(
                        "Catalog-managed Delta writes require " + IN_COMMIT_TIMESTAMPS + "=true");
            }
            if (snapshot.getMinWriterVersion() != 7
                    || !SUPPORTED_CATALOG_MANAGED_WRITER_FEATURES.containsAll(snapshot.getWriterFeatures())) {
                throw new UnsupportedOperationException(
                        "The initial catalog-managed Delta writer supports only catalogManaged, "
                                + "inCommitTimestamp and vacuumProtocolCheck "
                                + "writer protocol; table requires minWriterVersion="
                                + snapshot.getMinWriterVersion() + ", writerFeatures="
                                + snapshot.getWriterFeatures());
            }
        } else if (snapshot.getMinWriterVersion() > 2 || !snapshot.getWriterFeatures().isEmpty()) {
            throw new UnsupportedOperationException(
                    "The initial native Delta writer supports only baseline writer protocol; "
                            + "table requires minWriterVersion=" + snapshot.getMinWriterVersion()
                            + ", writerFeatures=" + snapshot.getWriterFeatures());
        }
    }

    DeltaInsertHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        requireExternalWriteCapability(deltaHandle);
        String applicationId = session == null ? null : session.getQueryId();
        if (writer != null) {
            return writer.beginInsert(deltaHandle, applicationId);
        }
        return catalogAdapter.beginInsert(deltaHandle, applicationId);
    }

    DeltaInsertHandle beginInsertOverwrite(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        requireExternalWriteCapability(deltaHandle);
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        // TRUNCATE enters here without constructing a BE file writer or calling getWriteConfig.
        validateWriteProtocol(deltaHandle, snapshot);
        String applicationId = session == null ? null : session.getQueryId();
        if (writer != null) {
            return writer.beginOverwrite(deltaHandle, snapshot, applicationId);
        }
        return catalogAdapter.beginOverwrite(deltaHandle, applicationId);
    }

    void finishFileInsert(ConnectorSession session, DeltaInsertHandle handle,
            Collection<DeltaFileCommitInfo> files) {
        requireWriteEnabled();
        handle.getWriter().finishInsert(handle, files);
    }

    void abortInsert(ConnectorSession session, DeltaInsertHandle handle) {
        handle.getWriter().abortInsert(handle);
    }

    void truncateTable(ConnectorSession session, ConnectorTableHandle handle) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        requireExternalWriteCapability(deltaHandle);
        DeltaInsertHandle overwrite = beginInsertOverwrite(session, handle, List.of());
        try {
            finishFileInsert(session, overwrite, List.of());
        } catch (RuntimeException e) {
            abortInsert(session, overwrite);
            throw e;
        }
    }

    private void requireWriteEnabled() {
        if (!supportsInsert()) {
            throw new UnsupportedOperationException(
                    "Native Delta INSERT requires delta.write.enabled=true and a supported "
                            + "external or catalog-managed table");
        }
    }

    private void requireExternalWriteCapability(DeltaTableHandle handle) {
        if (writer == null && !handle.supportsExternalWrite()) {
            throw new UnsupportedOperationException(
                    "Unity Catalog does not advertise external-engine write support for Delta table "
                            + handle.getDatabaseName() + "." + handle.getTableName());
        }
    }

    private Map<String, String> getBackendStoragePropertiesForWrite(
            DeltaTableHandle tableHandle) {
        Map<String, String> storageProperties = new LinkedHashMap<>();
        storageProperties.putAll(DeltaStorageProperties.toBackendProperties(properties));
        storageProperties.putAll(catalogAdapter.getBackendStoragePropertiesForWrite(tableHandle));
        return storageProperties;
    }

    private static boolean hasSameWriteSchema(List<ConnectorColumn> tableColumns,
            List<ConnectorColumn> insertColumns) {
        if (tableColumns.size() != insertColumns.size()) {
            return false;
        }
        for (int i = 0; i < tableColumns.size(); i++) {
            ConnectorColumn expected = tableColumns.get(i);
            ConnectorColumn actual = insertColumns.get(i);
            if (!expected.getName().equals(actual.getName())
                    || !hasCompatibleWriteType(expected.getType(), actual.getType())
                    || expected.isNullable() != actual.isNullable()) {
                return false;
            }
        }
        return true;
    }

    static boolean hasCompatibleWriteType(ConnectorType expected, ConnectorType actual) {
        if (expected.equals(actual)) {
            return true;
        }
        if (isDecimalV3(expected.getTypeName()) && isDecimalV3(actual.getTypeName())) {
            return decimalPrecisionMatchesStorageWidth(expected.getPrecision(), actual)
                    && parameterMatches(expected.getPrecision(), actual.getPrecision())
                    && parameterMatches(expected.getScale(), actual.getScale());
        }
        if (!expected.getTypeName().equals(actual.getTypeName())
                || !parameterMatches(expected.getPrecision(), actual.getPrecision())
                || !parameterMatches(expected.getScale(), actual.getScale())
                || !expected.getFieldNames().equals(actual.getFieldNames())
                || expected.getChildren().size() != actual.getChildren().size()) {
            return false;
        }
        for (int i = 0; i < expected.getChildren().size(); i++) {
            if (!hasCompatibleWriteType(expected.getChildren().get(i), actual.getChildren().get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDecimalV3(String typeName) {
        return "DECIMALV3".equals(typeName) || "DECIMAL32".equals(typeName)
                || "DECIMAL64".equals(typeName) || "DECIMAL128".equals(typeName)
                || "DECIMAL256".equals(typeName);
    }

    private static boolean decimalPrecisionMatchesStorageWidth(
            int expectedPrecision, ConnectorType actual) {
        if (expectedPrecision < 0 || actual.getPrecision() >= 0
                || "DECIMALV3".equals(actual.getTypeName())) {
            return true;
        }
        switch (actual.getTypeName()) {
            case "DECIMAL32":
                return expectedPrecision <= 9;
            case "DECIMAL64":
                return expectedPrecision >= 10 && expectedPrecision <= 18;
            case "DECIMAL128":
                return expectedPrecision >= 19 && expectedPrecision <= 38;
            case "DECIMAL256":
                return expectedPrecision >= 39 && expectedPrecision <= 76;
            default:
                return false;
        }
    }

    private static boolean parameterMatches(int expected, int actual) {
        return expected < 0 || actual < 0 || expected == actual;
    }

    static List<ConnectorColumn> toColumns(StructType schema) {
        List<ConnectorColumn> columns = new ArrayList<>(schema.length());
        for (StructField field : schema.fields()) {
            columns.add(new ConnectorColumn(field.getName(),
                    DeltaTypeMapping.fromDeltaType(field.getDataType()), "",
                    field.isNullable(), null));
        }
        return columns;
    }
}
