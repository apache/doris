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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Statement-scoped Iceberg writer metadata.
 *
 * <p>The connector resolves this context before the engine expands omitted columns or DEFAULT. Analysis,
 * sink construction and commit validation then consume the same schema/spec/order/format snapshot, preventing
 * a concurrent metadata change from combining expressions from one schema with files described by another.
 * Cached table columns deliberately do not carry Iceberg write defaults; only {@link #getColumns()} does, so
 * DESCRIBE and SHOW CREATE remain metadata-only while a write still receives typed defaults.</p>
 */
final class IcebergWriteSchemaContext {

    private final String tableName;
    private final Schema schema;
    private final int formatVersion;
    private final Optional<String> branchName;
    private final Optional<UUID> tableUuid;
    private final Optional<String> v1MetadataFileLocation;
    private final Optional<Long> v1MetadataTimestampMillis;
    private final String schemaJson;
    private final Schema mergeSchema;
    private final String mergeSchemaJson;
    private final PartitionSpec partitionSpec;
    private final String partitionSpecJson;
    private final SortOrder sortOrder;
    private final String sortOrderJson;
    private final FileFormat fileFormat;
    private final MetricsConfig metricsConfig;
    private final String fileCompression;
    private final String dataLocation;
    private final Map<String, String> writerProperties;
    private final List<ConnectorColumn> columns;

    static IcebergWriteSchemaContext create(Table table, String tableName,
            Optional<String> branchName, boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        Objects.requireNonNull(table, "table should not be null");
        Objects.requireNonNull(tableName, "tableName should not be null");
        Objects.requireNonNull(branchName, "branchName should not be null");
        Schema schema = branchName.isPresent()
                ? resolveBranchSchema(table, branchName.get(), tableName) : table.schema();
        if (branchName.isPresent()) {
            validateBranchWriterSchema(schema, table.schema(), branchName.get(), tableName);
        }
        int formatVersion = IcebergWriterHelper.getFormatVersion(table);
        TableIdentity identity = pinTableIdentity(table, formatVersion);
        return new IcebergWriteSchemaContext(
                tableName, schema, formatVersion, branchName, identity.uuid,
                identity.v1MetadataFileLocation, identity.v1MetadataTimestampMillis,
                bindPartitionSpec(table.spec(), schema, tableName),
                bindSortOrder(table.sortOrder(), schema, tableName),
                IcebergWriterHelper.getFileFormat(table), MetricsConfig.forTable(table),
                IcebergWritePlanProvider.getFileCompress(table),
                IcebergWritePlanProvider.dataLocation(table),
                ImmutableMap.copyOf(table.properties()),
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    private IcebergWriteSchemaContext(String tableName, Schema schema, int formatVersion,
            Optional<String> branchName, Optional<UUID> tableUuid,
            Optional<String> v1MetadataFileLocation, Optional<Long> v1MetadataTimestampMillis,
            PartitionSpec partitionSpec, SortOrder sortOrder, FileFormat fileFormat,
            MetricsConfig metricsConfig, String fileCompression, String dataLocation,
            Map<String, String> writerProperties, boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        this.tableName = Objects.requireNonNull(tableName, "tableName should not be null");
        this.schema = Objects.requireNonNull(schema, "schema should not be null");
        this.formatVersion = formatVersion;
        this.branchName = Objects.requireNonNull(branchName, "branchName should not be null");
        this.tableUuid = Objects.requireNonNull(tableUuid, "tableUuid should not be null");
        this.v1MetadataFileLocation = Objects.requireNonNull(
                v1MetadataFileLocation, "v1MetadataFileLocation should not be null");
        this.v1MetadataTimestampMillis = Objects.requireNonNull(
                v1MetadataTimestampMillis, "v1MetadataTimestampMillis should not be null");
        Preconditions.checkState(v1MetadataFileLocation.isPresent() == v1MetadataTimestampMillis.isPresent(),
                "Iceberg V1 metadata identity must contain both location and timestamp");
        Preconditions.checkState(!tableUuid.isPresent() || !v1MetadataFileLocation.isPresent(),
                "Iceberg table identity cannot contain both UUID and V1 metadata");
        this.schemaJson = SchemaParser.toJson(schema);
        this.mergeSchema = formatVersion >= 3
                ? IcebergWriterHelper.appendRowLineageFieldsForV3(schema) : schema;
        this.mergeSchemaJson = SchemaParser.toJson(mergeSchema);
        this.partitionSpec = Objects.requireNonNull(partitionSpec, "partitionSpec should not be null");
        this.partitionSpecJson = PartitionSpecParser.toJson(partitionSpec);
        this.sortOrder = Objects.requireNonNull(sortOrder, "sortOrder should not be null");
        this.sortOrderJson = SortOrderParser.toJson(sortOrder);
        this.fileFormat = Objects.requireNonNull(fileFormat, "fileFormat should not be null");
        this.metricsConfig = Objects.requireNonNull(metricsConfig, "metricsConfig should not be null");
        this.fileCompression = Objects.requireNonNull(fileCompression, "fileCompression should not be null");
        this.dataLocation = Objects.requireNonNull(dataLocation, "dataLocation should not be null");
        this.writerProperties = ImmutableMap.copyOf(
                Objects.requireNonNull(writerProperties, "writerProperties should not be null"));
        validateWriterMetadataSources(schema, partitionSpec, sortOrder, tableName);

        ImmutableList.Builder<ConnectorColumn> columnBuilder = ImmutableList.builder();
        for (Types.NestedField field : schema.columns()) {
            ConnectorType type = IcebergTypeMapping.fromIcebergType(
                    field.type(), enableMappingVarbinary, enableMappingTimestampTz);
            ConnectorColumn column = new ConnectorColumn(
                    field.name(), type, field.doc() == null ? "" : field.doc(),
                    field.isOptional(), null, true).withUniqueId(field.fieldId());
            if (isTimestampWithZone(field.type())) {
                column = column.withTimeZone();
            }
            String defaultSql = field.writeDefault() == null
                    ? (field.isOptional() ? "NULL" : null)
                    : toDorisSql(field.type(), field.writeDefault(),
                            enableMappingVarbinary, enableMappingTimestampTz);
            if (defaultSql != null) {
                column = column.withDefaultValueSql(defaultSql);
            }
            columnBuilder.add(column);
        }
        this.columns = columnBuilder.build();
    }

    private static PartitionSpec bindPartitionSpec(
            PartitionSpec partitionSpec, Schema schema, String tableName) {
        if (!partitionSpec.isPartitioned()) {
            return PartitionSpec.builderFor(schema).withSpecId(partitionSpec.specId()).build();
        }
        try {
            return PartitionSpecParser.fromJson(schema, PartitionSpecParser.toJson(partitionSpec));
        } catch (RuntimeException e) {
            throw new DorisConnectorException("Iceberg partition spec " + partitionSpec.specId()
                    + " is incompatible with pinned schema " + schema.schemaId()
                    + " for table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private static SortOrder bindSortOrder(SortOrder sortOrder, Schema schema, String tableName) {
        if (!sortOrder.isSorted()) {
            return SortOrder.unsorted();
        }
        try {
            return SortOrderParser.fromJson(schema, SortOrderParser.toJson(sortOrder));
        } catch (RuntimeException e) {
            throw new DorisConnectorException("Iceberg sort order " + sortOrder.orderId()
                    + " is incompatible with pinned schema " + schema.schemaId()
                    + " for table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private static void validateWriterMetadataSources(
            Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String tableName) {
        Map<Integer, Types.NestedField> topLevelFields = schema.columns().stream()
                .collect(ImmutableMap.toImmutableMap(Types.NestedField::fieldId, field -> field));
        for (PartitionField field : partitionSpec.fields()) {
            if (!topLevelFields.containsKey(field.sourceId())) {
                throw new DorisConnectorException("Iceberg partition field " + field.fieldId()
                        + " references source field " + field.sourceId()
                        + " outside pinned top-level schema " + schema.schemaId()
                        + " for table " + tableName);
            }
        }
        for (SortField field : sortOrder.fields()) {
            if (schema.findField(field.sourceId()) == null) {
                throw new DorisConnectorException("Iceberg sort field references source field "
                        + field.sourceId() + " outside pinned schema " + schema.schemaId()
                        + " for table " + tableName);
            }
        }
    }

    private static Schema resolveBranchSchema(Table table, String branchName, String tableName) {
        SnapshotRef ref = table.refs().get(branchName);
        if (ref == null) {
            throw new DorisConnectorException(branchName + " is not founded in " + tableName);
        }
        if (!ref.isBranch()) {
            throw new DorisConnectorException(branchName
                    + " is a tag, not a branch. Tags cannot be targets for producing snapshots");
        }
        return SnapshotUtil.schemaFor(table, ref.snapshotId());
    }

    private static void validateBranchWriterSchema(
            Schema branchSchema, Schema currentSchema, String branchName, String tableName) {
        Map<Integer, Types.NestedField> branchFields = TypeUtil.indexById(branchSchema.asStruct());
        Map<Integer, Types.NestedField> currentFields = TypeUtil.indexById(currentSchema.asStruct());
        Map<Integer, Integer> currentParents = TypeUtil.indexParents(currentSchema.asStruct());
        for (Types.NestedField currentField : currentFields.values()) {
            Types.NestedField branchField = branchFields.get(currentField.fieldId());
            if (branchField != null) {
                if (currentField.isRequired() && branchField.isOptional()) {
                    throw incompatibleBranchSchema(branchSchema, currentSchema, branchName, tableName,
                            currentField, "is optional in the pinned branch schema and can contain explicit nulls");
                }
                continue;
            }
            Types.NestedField highestMissingField = currentField;
            Integer parentId = currentParents.get(currentField.fieldId());
            while (parentId != null && !branchFields.containsKey(parentId)) {
                highestMissingField = Preconditions.checkNotNull(currentFields.get(parentId),
                        "Iceberg parent field %s is absent from current schema", parentId);
                parentId = currentParents.get(parentId);
            }
            if (highestMissingField.isRequired() && highestMissingField.initialDefault() == null) {
                throw incompatibleBranchSchema(branchSchema, currentSchema, branchName, tableName,
                        highestMissingField,
                        "is absent from the pinned branch schema and has no initial default");
            }
        }
    }

    private static DorisConnectorException incompatibleBranchSchema(
            Schema branchSchema, Schema currentSchema, String branchName, String tableName,
            Types.NestedField field, String incompatibility) {
        return new DorisConnectorException("Iceberg table current schema " + currentSchema.schemaId()
                + " cannot label files written with pinned branch " + branchName + " schema "
                + branchSchema.schemaId() + " for table " + tableName + ": required field "
                + field.name() + " (id " + field.fieldId() + ") " + incompatibility
                + "; retry after updating the branch schema");
    }

    void validateCurrentSchema(Table table, boolean requireCurrentPartitionSpec) {
        Schema currentSchema = branchName.isPresent()
                ? resolveBranchSchema(table, branchName.get(), tableName) : table.schema();
        int currentFormatVersion = IcebergWriterHelper.getFormatVersion(table);
        validateTableIdentity(table, currentFormatVersion);
        if (currentSchema.schemaId() != schema.schemaId() || currentFormatVersion != formatVersion) {
            throw new DorisConnectorException("Iceberg table schema changed during write planning for "
                    + tableName + ": pinned schema " + schema.schemaId() + "/format " + formatVersion
                    + ", current schema " + currentSchema.schemaId() + "/format " + currentFormatVersion
                    + "; retry the statement");
        }
        if (branchName.isPresent()) {
            validateBranchWriterSchema(schema, table.schema(), branchName.get(), tableName);
        }
        PartitionSpec retainedSpec = table.specs().get(partitionSpec.specId());
        if (retainedSpec == null
                || !partitionSpecJson.equals(PartitionSpecParser.toJson(retainedSpec))) {
            throw new DorisConnectorException("Iceberg partition spec changed during write planning for "
                    + tableName + ": pinned spec " + partitionSpec.specId()
                    + " is not available with the same definition; retry the statement");
        }
        if (requireCurrentPartitionSpec) {
            PartitionSpec activeSpec = table.spec();
            if (activeSpec.specId() != partitionSpec.specId()
                    || !partitionSpecJson.equals(PartitionSpecParser.toJson(activeSpec))) {
                throw new DorisConnectorException("Iceberg current partition spec changed during overwrite "
                        + "planning for " + tableName + ": pinned spec " + partitionSpec.specId()
                        + ", current spec " + activeSpec.specId() + "; retry the statement");
            }
        }
        SortOrder retainedSortOrder = table.sortOrders().get(sortOrder.orderId());
        if (retainedSortOrder == null
                || !sortOrderJson.equals(SortOrderParser.toJson(retainedSortOrder))) {
            throw new DorisConnectorException("Iceberg sort order changed during write planning for "
                    + tableName + ": pinned order " + sortOrder.orderId()
                    + " is not available with the same definition; retry the statement");
        }
    }

    private static TableIdentity pinTableIdentity(Table table, int formatVersion) {
        if (table instanceof HasTableOperations) {
            TableMetadata metadata = Preconditions.checkNotNull(
                    ((HasTableOperations) table).operations().current(),
                    "Iceberg table %s has no current metadata", table.name());
            if (metadata.uuid() != null) {
                return TableIdentity.forUuid(UUID.fromString(metadata.uuid()));
            }
            Preconditions.checkState(formatVersion == 1,
                    "Iceberg table %s format %s has no table UUID", table.name(), formatVersion);
            return TableIdentity.forV1Metadata(
                    Preconditions.checkNotNull(metadata.metadataFileLocation(),
                            "Iceberg V1 table %s has no metadata file location", table.name()),
                    metadata.lastUpdatedMillis());
        }
        return TableIdentity.forUuid(Preconditions.checkNotNull(
                table.uuid(), "Iceberg table %s does not expose a table UUID", table.name()));
    }

    private void validateTableIdentity(Table table, int currentFormatVersion) {
        if (tableUuid.isPresent()) {
            if (!tableUuid.equals(pinTableIdentity(table, currentFormatVersion).uuid)) {
                throw tableIdentityChanged();
            }
            return;
        }
        if (!v1MetadataFileLocation.isPresent()) {
            return;
        }
        Preconditions.checkState(table instanceof HasTableOperations,
                "Iceberg V1 table %s does not expose table operations", table.name());
        TableMetadata currentMetadata = Preconditions.checkNotNull(
                ((HasTableOperations) table).operations().current(),
                "Iceberg V1 table %s has no current metadata", table.name());
        boolean sameMetadata = v1MetadataFileLocation.get().equals(currentMetadata.metadataFileLocation())
                && v1MetadataTimestampMillis.get() == currentMetadata.lastUpdatedMillis();
        boolean retainedAncestor = currentMetadata.previousFiles().stream()
                .anyMatch(entry -> v1MetadataFileLocation.get().equals(entry.file())
                        && v1MetadataTimestampMillis.get() == entry.timestampMillis());
        if (!sameMetadata && !retainedAncestor) {
            throw tableIdentityChanged();
        }
    }

    private DorisConnectorException tableIdentityChanged() {
        return new DorisConnectorException("Iceberg table identity changed during write planning for "
                + tableName + "; the table may have been dropped and recreated; retry the statement");
    }

    private static boolean isTimestampWithZone(Type type) {
        return type.isPrimitiveType() && type.typeId() == Type.TypeID.TIMESTAMP
                && ((Types.TimestampType) type).shouldAdjustToUTC();
    }

    private static String toDorisSql(Type type, Object value,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        if (value == null) {
            return "NULL";
        }
        switch (type.typeId()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return String.valueOf(value);
            case DECIMAL:
                return ((BigDecimal) value).toPlainString();
            case STRING:
                return quote((String) value);
            case UUID:
                return binarySql(uuidBytes((UUID) value), enableMappingVarbinary);
            case FIXED:
            case BINARY:
                return binarySql(byteBufferBytes((ByteBuffer) value), enableMappingVarbinary);
            case DATE:
                return quote(LocalDate.ofEpochDay(((Integer) value).longValue()).toString());
            case TIMESTAMP:
                String timestamp = Transforms.identity(type).toHumanString(type, value).replace('T', ' ');
                if (((Types.TimestampType) type).shouldAdjustToUTC() && !enableMappingTimestampTz) {
                    timestamp = timestamp.replaceFirst("(Z|[+-]\\d{2}:\\d{2})$", "");
                }
                return quote(timestamp);
            case LIST:
                Types.ListType listType = (Types.ListType) type;
                List<String> items = new ArrayList<>();
                for (Object item : (List<?>) value) {
                    items.add(toDorisSql(listType.elementType(), item,
                            enableMappingVarbinary, enableMappingTimestampTz));
                }
                return "array(" + String.join(", ", items) + ")";
            case MAP:
                Types.MapType mapType = (Types.MapType) type;
                List<String> entries = new ArrayList<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    entries.add(toDorisSql(mapType.keyType(), entry.getKey(),
                            enableMappingVarbinary, enableMappingTimestampTz));
                    entries.add(toDorisSql(mapType.valueType(), entry.getValue(),
                            enableMappingVarbinary, enableMappingTimestampTz));
                }
                return "map(" + String.join(", ", entries) + ")";
            case STRUCT:
                Types.StructType structType = (Types.StructType) type;
                StructLike struct = (StructLike) value;
                List<String> fields = new ArrayList<>();
                for (int i = 0; i < structType.fields().size(); i++) {
                    Types.NestedField field = structType.fields().get(i);
                    fields.add(quoteStructFieldName(field.name()));
                    fields.add(toDorisSql(field.type(), struct.get(i, Object.class),
                            enableMappingVarbinary, enableMappingTimestampTz));
                }
                return "named_struct(" + String.join(", ", fields) + ")";
            default:
                throw new DorisConnectorException("Unsupported Iceberg write-default type: " + type);
        }
    }

    private static String binarySql(byte[] bytes, boolean enableMappingVarbinary) {
        String hex = BaseEncoding.base16().encode(bytes);
        return enableMappingVarbinary ? "X'" + hex + "'" : "UNHEX('" + hex + "')";
    }

    private static String quote(String value) {
        if (value.indexOf('\\') >= 0) {
            return binarySql(value.getBytes(StandardCharsets.UTF_8), false);
        }
        return quoteStructFieldName(value);
    }

    private static String quoteStructFieldName(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static byte[] uuidBytes(UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static byte[] byteBufferBytes(ByteBuffer value) {
        ByteBuffer duplicate = value.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    List<ConnectorColumn> getColumns() {
        return columns;
    }

    Schema getSchema() {
        return schema;
    }

    String getSchemaJson() {
        return schemaJson;
    }

    Schema getMergeSchema() {
        return mergeSchema;
    }

    String getMergeSchemaJson() {
        return mergeSchemaJson;
    }

    int getFormatVersion() {
        return formatVersion;
    }

    Optional<String> getBranchName() {
        return branchName;
    }

    PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    String getPartitionSpecJson() {
        return partitionSpecJson;
    }

    SortOrder getSortOrder() {
        return sortOrder;
    }

    FileFormat getFileFormat() {
        return fileFormat;
    }

    MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    String getFileCompression() {
        return fileCompression;
    }

    String getDataLocation() {
        return dataLocation;
    }

    Map<String, String> getWriterProperties() {
        return writerProperties;
    }

    private static final class TableIdentity {
        private final Optional<UUID> uuid;
        private final Optional<String> v1MetadataFileLocation;
        private final Optional<Long> v1MetadataTimestampMillis;

        private TableIdentity(Optional<UUID> uuid, Optional<String> v1MetadataFileLocation,
                Optional<Long> v1MetadataTimestampMillis) {
            this.uuid = uuid;
            this.v1MetadataFileLocation = v1MetadataFileLocation;
            this.v1MetadataTimestampMillis = v1MetadataTimestampMillis;
        }

        private static TableIdentity forUuid(UUID uuid) {
            return new TableIdentity(Optional.of(uuid), Optional.empty(), Optional.empty());
        }

        private static TableIdentity forV1Metadata(String metadataFileLocation, long timestampMillis) {
            return new TableIdentity(Optional.empty(), Optional.of(metadataFileLocation),
                    Optional.of(timestampMillis));
        }
    }
}
