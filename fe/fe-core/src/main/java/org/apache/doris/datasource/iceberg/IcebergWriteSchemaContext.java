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
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
    private final String schemaJson;
    private final String mergeSchemaJson;
    private final List<Column> columns;
    private final List<Column> mergeColumns;
    private final Map<Integer, Types.NestedField> fieldsById;
    private final Map<Integer, Expression> writeDefaultsById;

    /** Pin the current main or branch schema under the catalog authentication boundary. */
    public static IcebergWriteSchemaContext create(
            IcebergExternalTable dorisTable, Optional<String> branchName) {
        Objects.requireNonNull(dorisTable, "dorisTable should not be null");
        Objects.requireNonNull(branchName, "branchName should not be null");
        try {
            return dorisTable.getCatalog().getExecutionAuthenticator().execute(() -> {
                Table table = dorisTable.getIcebergTable();
                table.refresh();
                Schema schema = resolveSchema(table, branchName, dorisTable.getName());
                int formatVersion = IcebergUtils.getFormatVersion(table);
                return new IcebergWriteSchemaContext(
                        dorisTable.getId(), dorisTable.getName(), schema, formatVersion, branchName,
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
                Optional.empty(), enableMappingVarbinary, enableMappingTimestampTz);
    }

    private IcebergWriteSchemaContext(long tableId, String tableName, Schema schema,
            int formatVersion, Optional<String> branchName,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        this.tableId = tableId;
        this.tableName = Objects.requireNonNull(tableName, "tableName should not be null");
        this.schema = Objects.requireNonNull(schema, "schema should not be null");
        this.formatVersion = formatVersion;
        this.branchName = Objects.requireNonNull(branchName, "branchName should not be null");
        this.schemaJson = SchemaParser.toJson(schema);
        Schema mergeSchema = formatVersion >= IcebergUtils.ICEBERG_ROW_LINEAGE_MIN_VERSION
                ? IcebergUtils.appendRowLineageFieldsForV3(schema) : schema;
        this.mergeSchemaJson = SchemaParser.toJson(mergeSchema);

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

    private static Schema resolveSchema(Table table, Optional<String> branchName, String tableName) {
        if (!branchName.isPresent()) {
            return table.schema();
        }
        SnapshotRef ref = table.refs().get(branchName.get());
        if (ref == null) {
            throw new AnalysisException("Table " + tableName + " does not have branch named " + branchName.get());
        }
        if (!ref.isBranch()) {
            throw new AnalysisException(branchName.get()
                    + " is a tag, not a branch. Tags cannot be targets for producing snapshots");
        }
        return SnapshotUtil.schemaFor(table, ref.snapshotId());
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

    /** Validate that the table still exposes the schema and format pinned during analysis. */
    public void validateCurrentSchema(Table table) {
        table.refresh();
        Schema currentSchema = resolveSchema(table, branchName, tableName);
        int currentFormatVersion = IcebergUtils.getFormatVersion(table);
        if (currentSchema.schemaId() != getSchemaId() || currentFormatVersion != formatVersion) {
            throw new AnalysisException("Iceberg table schema changed during write planning for " + tableName
                    + ": pinned schema " + getSchemaId() + "/format " + formatVersion
                    + ", current schema " + currentSchema.schemaId() + "/format " + currentFormatVersion
                    + "; retry the statement");
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

    public Schema getSchema() {
        return schema;
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
                LocalDateTime dateTime = microsToDateTime(micros);
                long microsecond = Math.floorMod(micros, 1_000_000L);
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
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

    private static LocalDateTime microsToDateTime(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        int nanos = Math.toIntExact(Math.floorMod(micros, 1_000_000L) * 1_000L);
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
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
                && schemaJson.equals(that.schemaJson);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tableName, formatVersion, branchName, schemaJson);
    }
}
