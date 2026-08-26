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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Statement-scoped Iceberg write schema and write-default values.
 *
 * <p>The context pins one Iceberg schema before analysis. The analyzer, planner sink and
 * transaction preflight must all use this same instance so a concurrent schema change cannot
 * combine expressions from one schema with a writer schema from another one.
 */
public final class IcebergWriteSchemaContext {
    private final long tableId;
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
    private final List<Column> columns;
    private final List<Column> mergeColumns;
    private final Map<Integer, Types.NestedField> fieldsById;
    private final Map<Integer, Expression> writeDefaultsById;

    /** Pin the statement snapshot's table-current writer schema under the catalog authentication boundary. */
    public static IcebergWriteSchemaContext create(
            IcebergExternalTable dorisTable, Optional<String> branchName) {
        Objects.requireNonNull(dorisTable, "dorisTable should not be null");
        Objects.requireNonNull(branchName, "branchName should not be null");
        try {
            return dorisTable.getCatalog().getExecutionAuthenticator().execute(() -> {
                Table table = dorisTable.getIcebergTable();
                if (branchName.isPresent()) {
                    validateTargetBranch(table, branchName.get(), dorisTable.getName());
                }
                // A branch selects only the snapshot parent/ref. Iceberg validates branch writes
                // with the table-current schema and stamps that schema on the new snapshot.
                Schema schema = branchName.isPresent()
                        ? table.schema()
                        : resolveStatementSchema(table, dorisTable);
                int formatVersion = IcebergUtils.getFormatVersion(table);
                TableIdentity tableIdentity = pinTableIdentity(table, formatVersion);
                Map<String, String> properties = ImmutableMap.copyOf(table.properties());
                return new IcebergWriteSchemaContext(
                        dorisTable.getId(), dorisTable.getName(), schema, formatVersion, branchName,
                        tableIdentity.uuid, tableIdentity.v1MetadataFileLocation,
                        tableIdentity.v1MetadataTimestampMillis,
                        bindPartitionSpec(table.spec(), schema, dorisTable.getName()),
                        bindSortOrder(table.sortOrder(), schema, dorisTable.getName()),
                        IcebergUtils.getFileFormat(table), MetricsConfig.forTable(table),
                        IcebergUtils.getFileCompress(table), IcebergUtils.dataLocation(table), properties,
                        dorisTable.getCatalog().getEnableMappingVarbinary(),
                        dorisTable.getCatalog().getEnableMappingTimestampTz());
            });
        } catch (Exception e) {
            throw new AnalysisException("Failed to pin Iceberg write schema for table "
                    + dorisTable.getName() + ": " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public static IcebergWriteSchemaContext forSchema(Schema schema, int formatVersion,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        return new IcebergWriteSchemaContext(-1L, "test_table", schema, formatVersion,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                FileFormat.PARQUET, MetricsConfig.getDefault(),
                TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                "file:///tmp/test_table/data",
                ImmutableMap.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)),
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    @VisibleForTesting
    public static IcebergWriteSchemaContext forSchema(Schema schema, int formatVersion,
            PartitionSpec partitionSpec, SortOrder sortOrder, FileFormat fileFormat,
            MetricsConfig metricsConfig, String fileCompression, String dataLocation,
            Map<String, String> writerProperties,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        return new IcebergWriteSchemaContext(-1L, "test_table", schema, formatVersion,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                partitionSpec, sortOrder, fileFormat, metricsConfig,
                fileCompression, dataLocation, writerProperties,
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    @VisibleForTesting
    static IcebergWriteSchemaContext forSchemaWithUuidIdentity(
            Schema schema, int formatVersion, UUID tableUuid) {
        return new IcebergWriteSchemaContext(
                -1L, "test_table", schema, formatVersion, Optional.empty(),
                Optional.of(tableUuid), Optional.empty(), Optional.empty(),
                PartitionSpec.unpartitioned(), SortOrder.unsorted(), FileFormat.PARQUET,
                MetricsConfig.getDefault(),
                TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                "file:///tmp/test_table/data",
                ImmutableMap.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)),
                true, true);
    }

    private IcebergWriteSchemaContext(long tableId, String tableName, Schema schema,
            int formatVersion, Optional<String> branchName, Optional<UUID> tableUuid,
            Optional<String> v1MetadataFileLocation,
            Optional<Long> v1MetadataTimestampMillis,
            PartitionSpec partitionSpec, SortOrder sortOrder, FileFormat fileFormat,
            MetricsConfig metricsConfig, String fileCompression, String dataLocation,
            Map<String, String> writerProperties,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        this.tableId = tableId;
        this.tableName = Objects.requireNonNull(tableName, "tableName should not be null");
        this.schema = Objects.requireNonNull(schema, "schema should not be null");
        this.formatVersion = formatVersion;
        this.branchName = Objects.requireNonNull(branchName, "branchName should not be null");
        this.tableUuid = Objects.requireNonNull(tableUuid, "tableUuid should not be null");
        this.v1MetadataFileLocation = Objects.requireNonNull(
                v1MetadataFileLocation, "v1MetadataFileLocation should not be null");
        this.v1MetadataTimestampMillis = Objects.requireNonNull(
                v1MetadataTimestampMillis, "v1MetadataTimestampMillis should not be null");
        Preconditions.checkState(
                this.v1MetadataFileLocation.isPresent()
                        == this.v1MetadataTimestampMillis.isPresent(),
                "Iceberg V1 metadata identity must contain both location and timestamp");
        Preconditions.checkState(
                !this.tableUuid.isPresent() || !this.v1MetadataFileLocation.isPresent(),
                "Iceberg table identity cannot contain both UUID and V1 metadata");
        this.schemaJson = SchemaParser.toJson(schema);
        this.mergeSchema = formatVersion >= IcebergUtils.ICEBERG_ROW_LINEAGE_MIN_VERSION
                ? IcebergUtils.appendRowLineageFieldsForV3(schema) : schema;
        this.mergeSchemaJson = SchemaParser.toJson(mergeSchema);
        this.partitionSpec = Objects.requireNonNull(partitionSpec, "partitionSpec should not be null");
        this.partitionSpecJson = PartitionSpecParser.toJson(partitionSpec);
        this.sortOrder = Objects.requireNonNull(sortOrder, "sortOrder should not be null");
        this.sortOrderJson = SortOrderParser.toJson(sortOrder);
        this.fileFormat = Objects.requireNonNull(fileFormat, "fileFormat should not be null");
        this.metricsConfig = Objects.requireNonNull(metricsConfig, "metricsConfig should not be null");
        this.fileCompression = Objects.requireNonNull(
                fileCompression, "fileCompression should not be null");
        this.dataLocation = Objects.requireNonNull(dataLocation, "dataLocation should not be null");
        this.writerProperties = ImmutableMap.copyOf(
                Objects.requireNonNull(writerProperties, "writerProperties should not be null"));
        validateWriterMetadataSources(schema, partitionSpec, sortOrder, tableName);

        List<Column> parsedColumns = IcebergUtils.parseSchema(
                schema, enableMappingVarbinary, enableMappingTimestampTz);
        this.columns = ImmutableList.copyOf(parsedColumns);
        List<Column> writerColumns = new ArrayList<>(parsedColumns);
        writerColumns.add(IcebergRowId.createHiddenColumn());
        if (formatVersion >= IcebergUtils.ICEBERG_ROW_LINEAGE_MIN_VERSION) {
            Column rowIdColumn = IcebergUtils.parseField(
                    org.apache.iceberg.MetadataColumns.ROW_ID,
                    enableMappingVarbinary, enableMappingTimestampTz);
            rowIdColumn.setIsVisible(false);
            writerColumns.add(rowIdColumn);
            Column sequenceColumn = IcebergUtils.parseField(
                    org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER,
                    enableMappingVarbinary, enableMappingTimestampTz);
            sequenceColumn.setIsVisible(false);
            writerColumns.add(sequenceColumn);
        }
        this.mergeColumns = ImmutableList.copyOf(writerColumns);

        ImmutableMap.Builder<Integer, Types.NestedField> byId = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Expression> defaults = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            byId.put(field.fieldId(), field);
            if (field.writeDefault() != null) {
                DataType targetType = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                        field.type(), enableMappingVarbinary, enableMappingTimestampTz));
                defaults.put(field.fieldId(), toDorisExpression(
                        field.type(), field.writeDefault(), targetType,
                        enableMappingVarbinary, enableMappingTimestampTz));
            }
        }
        this.fieldsById = byId.build();
        this.writeDefaultsById = defaults.build();
    }

    private static PartitionSpec bindPartitionSpec(
            PartitionSpec partitionSpec, Schema schema, String tableName) {
        if (!partitionSpec.isPartitioned()) {
            return PartitionSpec.builderFor(schema)
                    .withSpecId(partitionSpec.specId())
                    .build();
        }
        try {
            return PartitionSpecParser.fromJson(schema, PartitionSpecParser.toJson(partitionSpec));
        } catch (RuntimeException e) {
            throw new AnalysisException("Iceberg partition spec " + partitionSpec.specId()
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
            throw new AnalysisException("Iceberg sort order " + sortOrder.orderId()
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
                throw new AnalysisException("Iceberg partition field " + field.fieldId()
                        + " references source field " + field.sourceId()
                        + " outside pinned top-level schema " + schema.schemaId()
                        + " for table " + tableName);
            }
        }
        for (SortField field : sortOrder.fields()) {
            if (schema.findField(field.sourceId()) == null) {
                throw new AnalysisException("Iceberg sort field references source field "
                        + field.sourceId() + " outside pinned schema " + schema.schemaId()
                        + " for table " + tableName);
            }
        }
    }

    private static void validateTargetBranch(Table table, String branchName, String tableName) {
        SnapshotRef ref = table.refs().get(branchName);
        if (ref == null) {
            throw new AnalysisException(branchName + " is not founded in " + tableName);
        }
        if (!ref.isBranch()) {
            throw new AnalysisException(branchName
                    + " is a tag, not a branch. Tags cannot be targets for producing snapshots");
        }
    }

    private static Schema resolveStatementSchema(Table table, IcebergExternalTable dorisTable) {
        Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(dorisTable);
        if (!snapshot.isPresent()) {
            return table.schema();
        }
        Preconditions.checkState(snapshot.get() instanceof IcebergMvccSnapshot,
                "Expected an Iceberg MVCC snapshot for table %s", dorisTable.getName());
        long schemaId = ((IcebergMvccSnapshot) snapshot.get())
                .getSnapshotCacheValue().getSnapshot().getSchemaId();
        Schema schema = table.schemas().get(Math.toIntExact(schemaId));
        return Preconditions.checkNotNull(schema,
                "Iceberg schema %s is not available in the statement table metadata for %s",
                schemaId, dorisTable.getName());
    }

    /** Resolve a write default by the pinned target field name. */
    public Expression resolveWriteDefault(String columnName) {
        Column column = columns.stream()
                .filter(targetColumn -> targetColumn.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new AnalysisException(
                        "Cannot find column information for DEFAULT(" + columnName + ")"));
        return resolveWriteDefault(column);
    }

    /** Resolve the value used for an omitted column or an explicit DEFAULT. */
    public Expression resolveWriteDefault(Column column) {
        Types.NestedField field = fieldsById.get(column.getUniqueId());
        if (field == null) {
            throw new AnalysisException("Column " + column.getName()
                    + " is not present in pinned Iceberg schema " + getSchemaId());
        }
        Expression writeDefault = writeDefaultsById.get(field.fieldId());
        if (writeDefault != null) {
            return writeDefault;
        }
        DataType targetType = DataType.fromCatalogType(column.getType());
        if (field.isOptional()) {
            return new NullLiteral(targetType);
        }
        throw new AnalysisException("Column has no write default and is required, column=" + field.name());
    }

    /** Validate that the fresh table can commit files described by the pinned writer metadata. */
    public void validateCurrentSchema(Table table) {
        validateCurrentSchema(table, false);
    }

    /**
     * Validate that the fresh table can commit files described by the pinned writer metadata.
     *
     * <p>Every overwrite additionally requires the pinned spec to remain current because both
     * dynamic replacement and static replacement semantics depend on whether and how that spec is
     * partitioned. Appends can safely write an older retained spec, so they only require the pinned
     * definition to remain available.
     */
    public void validateCurrentSchema(Table table, boolean requireCurrentPartitionSpec) {
        if (branchName.isPresent()) {
            validateTargetBranch(table, branchName.get(), tableName);
        }
        Schema currentSchema = table.schema();
        int currentFormatVersion = IcebergUtils.getFormatVersion(table);
        validateTableIdentity(table, currentFormatVersion);
        if (currentSchema.schemaId() != getSchemaId() || currentFormatVersion != formatVersion) {
            throw new AnalysisException("Iceberg table schema changed during write planning for " + tableName
                    + ": pinned schema " + getSchemaId() + "/format " + formatVersion
                    + ", current schema " + currentSchema.schemaId() + "/format " + currentFormatVersion
                    + "; retry the statement");
        }
        String currentDataLocation = IcebergUtils.dataLocation(table);
        if (!dataLocation.equals(currentDataLocation)
                || !writerProperties.equals(table.properties())) {
            throw new AnalysisException("Iceberg table writer properties or data location changed during "
                    + "write planning for " + tableName + "; retry the statement");
        }
        PartitionSpec currentSpec = table.specs().get(partitionSpec.specId());
        if (currentSpec == null || !partitionSpecJson.equals(PartitionSpecParser.toJson(currentSpec))) {
            throw new AnalysisException("Iceberg partition spec changed during write planning for "
                    + tableName + ": pinned spec " + partitionSpec.specId()
                    + " is not available with the same definition; retry the statement");
        }
        if (requireCurrentPartitionSpec) {
            PartitionSpec activeSpec = table.spec();
            if (activeSpec.specId() != partitionSpec.specId()
                    || !partitionSpecJson.equals(PartitionSpecParser.toJson(activeSpec))) {
                throw new AnalysisException("Iceberg current partition spec changed during overwrite "
                        + "planning for " + tableName + ": pinned spec " + partitionSpec.specId()
                        + ", current spec " + activeSpec.specId() + "; retry the statement");
            }
        }
        SortOrder currentSortOrder = table.sortOrders().get(sortOrder.orderId());
        if (currentSortOrder == null || !sortOrderJson.equals(SortOrderParser.toJson(currentSortOrder))) {
            throw new AnalysisException("Iceberg sort order changed during write planning for "
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
            TableIdentity currentIdentity = pinTableIdentity(table, currentFormatVersion);
            if (!tableUuid.equals(currentIdentity.uuid)) {
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
        boolean sameMetadata = v1MetadataFileLocation.get().equals(
                currentMetadata.metadataFileLocation())
                && v1MetadataTimestampMillis.get() == currentMetadata.lastUpdatedMillis();
        boolean retainedAncestor = currentMetadata.previousFiles().stream()
                .anyMatch(entry -> v1MetadataFileLocation.get().equals(entry.file())
                        && v1MetadataTimestampMillis.get() == entry.timestampMillis());
        if (!sameMetadata && !retainedAncestor) {
            throw tableIdentityChanged();
        }
    }

    private AnalysisException tableIdentityChanged() {
        return new AnalysisException("Iceberg table identity changed during write planning for "
                + tableName + "; the table may have been dropped and recreated; retry the statement");
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
            return new TableIdentity(
                    Optional.of(uuid), Optional.empty(), Optional.empty());
        }

        private static TableIdentity forV1Metadata(
                String metadataFileLocation, long metadataTimestampMillis) {
            return new TableIdentity(
                    Optional.empty(), Optional.of(metadataFileLocation),
                    Optional.of(metadataTimestampMillis));
        }
    }

    public int getSchemaId() {
        return schema.schemaId();
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public Optional<String> getBranchName() {
        return branchName;
    }

    public boolean isTargetTable(long candidateTableId) {
        return tableId == candidateTableId;
    }

    public String getSchemaJson() {
        return schemaJson;
    }

    public String getMergeSchemaJson() {
        return mergeSchemaJson;
    }

    public Schema getMergeSchema() {
        return mergeSchema;
    }

    public Schema getSchema() {
        return schema;
    }

    public PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    public String getPartitionSpecJson() {
        return partitionSpecJson;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    public String getFileCompression() {
        return fileCompression;
    }

    public String getDataLocation() {
        return dataLocation;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Column> getMergeColumns() {
        return mergeColumns;
    }

    public Optional<Types.NestedField> findField(Column column) {
        return Optional.ofNullable(fieldsById.get(column.getUniqueId()));
    }

    @VisibleForTesting
    static Expression toDorisExpression(Type icebergType, Object value, DataType targetType,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        Objects.requireNonNull(icebergType, "icebergType should not be null");
        Objects.requireNonNull(targetType, "targetType should not be null");
        if (value == null) {
            return new NullLiteral(targetType);
        }

        switch (icebergType.typeId()) {
            case BOOLEAN:
                return BooleanLiteral.of((Boolean) value);
            case INTEGER:
                return new IntegerLiteral((Integer) value);
            case LONG:
                return new BigIntLiteral((Long) value);
            case FLOAT:
                return new FloatLiteral((Float) value);
            case DOUBLE:
                return new DoubleLiteral((Double) value);
            case DECIMAL:
                return new DecimalV3Literal((DecimalV3Type) targetType, (BigDecimal) value);
            case STRING:
                return new StringLiteral((String) value);
            case UUID:
                return binaryExpression(uuidBytes((UUID) value), targetType);
            case FIXED:
            case BINARY:
                return binaryExpression(byteBufferBytes((ByteBuffer) value), targetType);
            case DATE:
                LocalDate date = LocalDate.ofEpochDay(((Integer) value).longValue());
                return new DateV2Literal(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            case TIMESTAMP:
                long micros = (Long) value;
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                ZoneId literalZone = timestampType.shouldAdjustToUTC() && !enableMappingTimestampTz
                        ? TimeUtils.getDorisZoneId() : ZoneOffset.UTC;
                LocalDateTime dateTime = microsToDateTime(micros, literalZone);
                long microsecond = Math.floorMod(micros, 1_000_000L);
                if (enableMappingTimestampTz && timestampType.shouldAdjustToUTC()) {
                    return new TimestampTzLiteral((TimeStampTzType) targetType,
                            dateTime.getYear(), dateTime.getMonthValue(),
                            dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(),
                            dateTime.getSecond(), microsecond);
                }
                return new DateTimeV2Literal((DateTimeV2Type) targetType,
                        dateTime.getYear(), dateTime.getMonthValue(),
                        dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(),
                        dateTime.getSecond(), microsecond);
            case LIST:
                return listExpression((Types.ListType) icebergType, value, targetType,
                        enableMappingVarbinary, enableMappingTimestampTz);
            case MAP:
                return mapExpression((Types.MapType) icebergType, value, targetType,
                        enableMappingVarbinary, enableMappingTimestampTz);
            case STRUCT:
                return structExpression((Types.StructType) icebergType, value, targetType,
                        enableMappingVarbinary, enableMappingTimestampTz);
            default:
                throw new AnalysisException("Unsupported Iceberg write-default type: " + icebergType);
        }
    }

    private static Expression listExpression(Types.ListType icebergType, Object value, DataType targetType,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        Preconditions.checkArgument(value instanceof List,
                "Iceberg list default should be a List, but is %s", value.getClass());
        DataType elementType = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                icebergType.elementType(), enableMappingVarbinary, enableMappingTimestampTz));
        List<Expression> items = new ArrayList<>();
        for (Object item : (List<?>) value) {
            items.add(toDorisExpression(icebergType.elementType(), item, elementType,
                    enableMappingVarbinary, enableMappingTimestampTz));
        }
        if (items.stream().allMatch(Literal.class::isInstance)) {
            List<Literal> literalItems = items.stream()
                    .map(Literal.class::cast).collect(ImmutableList.toImmutableList());
            return new ArrayLiteral(literalItems, targetType);
        }
        // Legacy UUID/FIXED/BINARY mapping uses UNHEX to materialize raw bytes. Container
        // literals accept literal children only, so preserve that expression in the existing
        // array function path; UNHEX remains executable by older BEs during a rolling upgrade.
        return TypeCoercionUtils.castIfNotSameType(new Array(items), targetType);
    }

    private static Expression mapExpression(Types.MapType icebergType, Object value, DataType targetType,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        Preconditions.checkArgument(value instanceof Map,
                "Iceberg map default should be a Map, but is %s", value.getClass());
        DataType keyType = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                icebergType.keyType(), enableMappingVarbinary, enableMappingTimestampTz));
        DataType valueType = DataType.fromCatalogType(IcebergUtils.icebergTypeToDorisType(
                icebergType.valueType(), enableMappingVarbinary, enableMappingTimestampTz));
        Map<Literal, Literal> items = new LinkedHashMap<>();
        List<Expression> arguments = new ArrayList<>();
        boolean allLiterals = true;
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            Expression key = toDorisExpression(icebergType.keyType(), entry.getKey(), keyType,
                    enableMappingVarbinary, enableMappingTimestampTz);
            Expression mapValue = toDorisExpression(icebergType.valueType(), entry.getValue(), valueType,
                    enableMappingVarbinary, enableMappingTimestampTz);
            arguments.add(key);
            arguments.add(mapValue);
            if (key instanceof Literal && mapValue instanceof Literal) {
                items.put((Literal) key, (Literal) mapValue);
            } else {
                allLiterals = false;
            }
        }
        if (allLiterals) {
            return new MapLiteral(items, targetType);
        }
        return TypeCoercionUtils.castIfNotSameType(
                new CreateMap(arguments.toArray(new Expression[0])), targetType);
    }

    private static Expression structExpression(Types.StructType icebergType, Object value, DataType targetType,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        Preconditions.checkArgument(value instanceof StructLike,
                "Iceberg struct default should be StructLike, but is %s", value.getClass());
        Preconditions.checkArgument(targetType instanceof StructType,
                "Doris struct default type should be StructType, but is %s", targetType);
        StructLike struct = (StructLike) value;
        List<Expression> fields = new ArrayList<>();
        List<Expression> namedFields = new ArrayList<>();
        for (int i = 0; i < icebergType.fields().size(); i++) {
            Types.NestedField childField = icebergType.fields().get(i);
            Type childType = childField.type();
            DataType childDorisType = ((StructType) targetType).getFields().get(i).getDataType();
            Expression child = toDorisExpression(childType, struct.get(i, Object.class), childDorisType,
                    enableMappingVarbinary, enableMappingTimestampTz);
            fields.add(child);
            namedFields.add(new StringLiteral(childField.name()));
            namedFields.add(child);
        }
        if (fields.stream().allMatch(Literal.class::isInstance)) {
            List<Literal> literalFields = fields.stream()
                    .map(Literal.class::cast).collect(ImmutableList.toImmutableList());
            return new StructLiteral(literalFields, targetType);
        }
        return TypeCoercionUtils.castIfNotSameType(
                new CreateNamedStruct(namedFields.toArray(new Expression[0])), targetType);
    }

    private static byte[] uuidBytes(UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static Expression binaryExpression(byte[] bytes, DataType targetType) {
        if (targetType instanceof VarBinaryType) {
            return new VarBinaryLiteral(targetType, bytes);
        }
        Expression rawBytes = new Unhex(new StringLiteral(BaseEncoding.base16().encode(bytes)));
        return TypeCoercionUtils.castIfNotSameType(rawBytes, targetType);
    }

    private static byte[] byteBufferBytes(ByteBuffer value) {
        ByteBuffer duplicate = value.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private static LocalDateTime microsToDateTime(long micros, ZoneId zoneId) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        int nanos = Math.toIntExact(Math.floorMod(micros, 1_000_000L) * 1_000L);
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), zoneId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IcebergWriteSchemaContext)) {
            return false;
        }
        IcebergWriteSchemaContext that = (IcebergWriteSchemaContext) object;
        return tableId == that.tableId
                && formatVersion == that.formatVersion
                && tableName.equals(that.tableName)
                && branchName.equals(that.branchName)
                && tableUuid.equals(that.tableUuid)
                && v1MetadataFileLocation.equals(that.v1MetadataFileLocation)
                && v1MetadataTimestampMillis.equals(that.v1MetadataTimestampMillis)
                && schemaJson.equals(that.schemaJson)
                && partitionSpecJson.equals(that.partitionSpecJson)
                && sortOrderJson.equals(that.sortOrderJson)
                && fileFormat == that.fileFormat
                && fileCompression.equals(that.fileCompression)
                && dataLocation.equals(that.dataLocation)
                && writerProperties.equals(that.writerProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tableName, formatVersion, branchName, tableUuid,
                v1MetadataFileLocation, v1MetadataTimestampMillis, schemaJson, partitionSpecJson,
                sortOrderJson, fileFormat, fileCompression, dataLocation, writerProperties);
    }
}
