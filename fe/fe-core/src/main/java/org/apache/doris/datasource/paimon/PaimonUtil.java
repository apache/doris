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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class PaimonUtil {
    private static final Logger LOG = LogManager.getLogger(PaimonUtil.class);
    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();
    private static final Pattern DIGITAL_REGEX = Pattern.compile("\\d+");
    private static final String SYS_TABLE_TYPE_AUDIT_LOG = "audit_log";
    private static final String SYS_TABLE_TYPE_BINLOG = "binlog";
    private static final String TABLE_READ_SEQUENCE_NUMBER_ENABLED = "table-read.sequence-number.enabled";

    public static boolean isDigitalString(String value) {
        return value != null && DIGITAL_REGEX.matcher(value).matches();
    }

    public static List<InternalRow> read(
            Table table, @Nullable int[] projection, @Nullable Predicate predicate,
            Pair<ConfigOption<?>, String>... dynamicOptions)
            throws IOException {
        Map<String, String> options = new HashMap<>();
        for (Pair<ConfigOption<?>, String> pair : dynamicOptions) {
            options.put(pair.getKey().key(), pair.getValue());
        }
        if (!options.isEmpty()) {
            table = table.copy(options);
        }
        ReadBuilder readBuilder = table.newReadBuilder();
        if (projection != null) {
            readBuilder.withProjection(projection);
        }
        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer =
                new InternalRowSerializer(
                        projection == null
                                ? table.rowType()
                                : Projection.of(projection).project(table.rowType()));
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    public static PaimonPartitionInfo generatePartitionInfo(Table table, List<Column> partitionColumns,
            List<PartitionEntry> partitionEntries) {

        if (CollectionUtils.isEmpty(partitionColumns) || partitionEntries.isEmpty()) {
            return PaimonPartitionInfo.EMPTY;
        }

        CoreOptions options = new CoreOptions(table.options());
        RowType partitionType = table.rowType().project(table.partitionKeys());
        if (partitionType.getFields().stream().anyMatch(field ->
                field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            // This metadata is cached by table snapshot, but LTZ values are represented as
            // session-local civil times in Doris. Caching those bounds would let one session
            // reuse pruning metadata produced in another time zone. Keep scan correctness by
            // delegating pruning to Paimon until this cache can carry a time-zone-independent
            // typed representation.
            return PaimonPartitionInfo.UNPRUNABLE;
        }
        InternalRowPartitionComputer partitionComputer = new InternalRowPartitionComputer(
                options.partitionDefaultName(),
                partitionType,
                table.partitionKeys().toArray(new String[0]),
                options.legacyPartitionName());
        List<Type> types = partitionColumns.stream()
                .map(Column::getType)
                .collect(Collectors.toList());
        List<PaimonPartitionCandidate> candidates = Lists.newArrayListWithExpectedSize(partitionEntries.size());
        Map<String, Map<String, String>> displayNameToTypedSpec = Maps.newHashMap();

        for (PartitionEntry partitionEntry : partitionEntries) {
            Map<String, String> typedSpec = getPartitionInfoMap(
                    table, partitionEntry.partition(), TimeUtils.getTimeZone().getID());
            if (typedSpec == null) {
                return PaimonPartitionInfo.UNPRUNABLE;
            }

            List<String> partitionValues = Lists.newArrayListWithExpectedSize(partitionColumns.size());
            LinkedHashMap<String, String> orderedTypedSpec = new LinkedHashMap<>();
            for (Column partitionColumn : partitionColumns) {
                String partitionColumnName = partitionColumn.getName();
                Preconditions.checkState(typedSpec.containsKey(partitionColumnName),
                        "Partition column not found in Paimon typed spec: " + partitionColumnName);
                String partitionValue = typedSpec.get(partitionColumnName);
                partitionValues.add(partitionValue);
                orderedTypedSpec.put(partitionColumnName, partitionValue);
            }

            PartitionItem partitionItem;
            try {
                partitionItem = toListPartitionItem(partitionValues, types);
            } catch (Exception e) {
                LOG.warn("toListPartitionItem failed, partitionColumns: {}, partitionValues: {}",
                        partitionColumns, partitionValues, e);
                return PaimonPartitionInfo.UNPRUNABLE;
            }

            LinkedHashMap<String, String> displaySpec;
            try {
                // Delegate display-name generation to Paimon so partition.default-name and
                // partition.legacy-name exactly follow the table's physical partition naming.
                // The canonical typed spec above remains the logical identity used for pruning.
                displaySpec = partitionComputer.generatePartValues(partitionEntry.partition());
            } catch (Exception e) {
                LOG.warn("Failed to generate Paimon partition display name, table: {}, partition: {}",
                        table.name(), orderedTypedSpec, e);
                return PaimonPartitionInfo.UNPRUNABLE;
            }
            String partitionPath = PartitionPathUtils.generatePartitionPath(displaySpec);
            String displayName = partitionPath.substring(0, partitionPath.length() - 1);
            Map<String, String> previousTypedSpec = displayNameToTypedSpec.putIfAbsent(
                    displayName, orderedTypedSpec);
            if (previousTypedSpec != null) {
                Preconditions.checkState(!previousTypedSpec.equals(orderedTypedSpec),
                        "Duplicate typed Paimon partition: " + displayName);
                // Doris partition metadata and downstream consumers such as MTMV require a
                // stable one-to-one mapping between a partition name and its typed value.
                // Paimon may map distinct values (for example null and blank strings) to the
                // same physical partition name. A private suffix would only make the map key
                // unique; it would be lost when consumers reconstruct a name from PartitionItem.
                // Keep the complete mapping all-or-nothing and delegate pruning to Paimon.
                LOG.warn("Ambiguous Paimon partition display name {}, typed specs: {} and {}; "
                                + "disable Doris partition pruning",
                        displayName, previousTypedSpec, orderedTypedSpec);
                return PaimonPartitionInfo.UNPRUNABLE;
            }
            candidates.add(new PaimonPartitionCandidate(
                    partitionEntry, orderedTypedSpec, partitionItem, displayName));
        }

        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, Partition> nameToPartition = Maps.newHashMap();
        for (PaimonPartitionCandidate candidate : candidates) {
            PartitionEntry entry = candidate.partitionEntry;
            Partition partition = new Partition(candidate.typedSpec, entry.recordCount(),
                    entry.fileSizeInBytes(), entry.fileCount(), entry.lastFileCreationTime(),
                    entry.totalBuckets(), false);
            nameToPartitionItem.put(candidate.displayName, candidate.partitionItem);
            nameToPartition.put(candidate.displayName, partition);
        }
        return new PaimonPartitionInfo(nameToPartitionItem, nameToPartition);
    }

    private static final class PaimonPartitionCandidate {
        private final PartitionEntry partitionEntry;
        private final Map<String, String> typedSpec;
        private final PartitionItem partitionItem;
        private final String displayName;

        private PaimonPartitionCandidate(PartitionEntry partitionEntry, Map<String, String> typedSpec,
                PartitionItem partitionItem, String displayName) {
            this.partitionEntry = partitionEntry;
            this.typedSpec = typedSpec;
            this.partitionItem = partitionItem;
            this.displayName = displayName;
        }
    }

    public static ListPartitionItem toListPartitionItem(List<String> partitionValues, List<Type> types)
            throws AnalysisException {
        Preconditions.checkState(partitionValues.size() == types.size(), partitionValues + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String partitionValue : partitionValues) {
            // Keep a typed null distinct from an empty string and the literal string "null".
            values.add(new PartitionValue(partitionValue, partitionValue == null));
        }
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
        ListPartitionItem listPartitionItem = new ListPartitionItem(Lists.newArrayList(key));
        return listPartitionItem;
    }

    private static Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType,
            boolean enableVarbinaryMapping, boolean enableTimestampTzMapping) {
        int tsScale = 3; // default
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case VARCHAR:
                int varcharLen = ((VarCharType) dataType).getLength();
                if (varcharLen > 65533) {
                    return ScalarType.createStringType();
                }
                return ScalarType.createVarcharType(varcharLen);
            case CHAR:
                int charLen = ((CharType) dataType).getLength();
                if (charLen > 255) {
                    return ScalarType.createStringType();
                }
                return ScalarType.createCharType(charLen);
            case BINARY:
                int binaryLen = ((BinaryType) dataType).getLength();
                return enableVarbinaryMapping ? ScalarType.createVarbinaryType(binaryLen) : Type.STRING;
            case VARBINARY:
                // Paimon VarBinaryType length is in [1, 2147483647]
                int varbinaryLen = ((VarBinaryType) dataType).getLength();
                return enableVarbinaryMapping ? ScalarType.createVarbinaryType(varbinaryLen) : Type.STRING;
            case DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.TimestampType) {
                    tsScale = ((org.apache.paimon.types.TimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                } else if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                if (enableTimestampTzMapping) {
                    return ScalarType.createTimeStampTzType(tsScale);
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case VARIANT:
                // External-table schemas are cached and shared, so the physical marker must not
                // depend on enable_variant_v2. PaimonScanNode checks the global switch per query.
                return VariantType.COMPUTE_V2_INSTANCE;
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Type innerType = paimonPrimitiveTypeToDorisType(arrayType.getElementType(), enableVarbinaryMapping,
                        enableTimestampTzMapping);
                return org.apache.doris.catalog.ArrayType.create(innerType, true);
            case MAP:
                MapType mapType = (MapType) dataType;
                return new org.apache.doris.catalog.MapType(
                        paimonTypeToDorisType(mapType.getKeyType(), enableVarbinaryMapping, enableTimestampTzMapping),
                        paimonTypeToDorisType(mapType.getValueType(), enableVarbinaryMapping,
                                enableTimestampTzMapping));
            case ROW:
                RowType rowType = (RowType) dataType;
                List<DataField> fields = rowType.getFields();
                return new org.apache.doris.catalog.StructType(fields.stream()
                        .map(field -> new org.apache.doris.catalog.StructField(field.name(),
                                paimonTypeToDorisType(field.type(), enableVarbinaryMapping, enableTimestampTzMapping)))
                        .collect(Collectors.toCollection(ArrayList::new)));
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                LOG.warn("Cannot transform unknown type: " + dataType.getTypeRoot());
                return Type.UNSUPPORTED;
        }
    }

    public static Type paimonTypeToDorisType(org.apache.paimon.types.DataType type, boolean enableVarbinaryMapping,
            boolean enableTimestampTzMapping) {
        return paimonPrimitiveTypeToDorisType(type, enableVarbinaryMapping, enableTimestampTzMapping);
    }

    public static boolean containsVariant(Type type) {
        if (type.isVariantType()) {
            return true;
        } else if (type.isArrayType()) {
            return containsVariant(((org.apache.doris.catalog.ArrayType) type).getItemType());
        } else if (type.isMapType()) {
            org.apache.doris.catalog.MapType mapType = (org.apache.doris.catalog.MapType) type;
            return containsVariant(mapType.getKeyType()) || containsVariant(mapType.getValueType());
        } else if (type.isStructType()) {
            return ((org.apache.doris.catalog.StructType) type).getFields().stream()
                    .anyMatch(field -> containsVariant(field.getType()));
        }
        return false;
    }

    public static void updatePaimonColumnUniqueId(Column column, DataType dataType) {
        List<Column> columns = column.getChildren();
        if (columns == null) {
            return;
        }
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                updatePaimonColumnUniqueId(columns.get(0), arrayType.getElementType());
                break;
            case MAP:
                MapType mapType = (MapType) dataType;
                updatePaimonColumnUniqueId(columns.get(0), mapType.getKeyType());
                updatePaimonColumnUniqueId(columns.get(1), mapType.getValueType());
                break;
            case ROW:
                RowType rowType = (RowType) dataType;
                for (int idx = 0; idx < columns.size(); idx++) {
                    updatePaimonColumnUniqueId(columns.get(idx), rowType.getFields().get(idx));
                }
                break;
            default:
                return;
        }
    }

    public static void updatePaimonColumnUniqueId(Column column, DataField field) {
        column.setUniqueId(field.id());
        updatePaimonColumnUniqueId(column, field.type());
    }

    public static void updatePaimonColumnMetadata(Column column, DataField field) {
        updatePaimonColumnUniqueId(column, field);
        updatePaimonColumnTimezone(column, field.type());
    }

    private static void updatePaimonColumnTimezone(Column column, DataType dataType) {
        if (dataType.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            column.setWithTZExtraInfo();
        }
        List<Column> children = column.getChildren();
        if (children == null) {
            return;
        }
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                updatePaimonColumnTimezone(children.get(0), ((ArrayType) dataType).getElementType());
                break;
            case MAP:
                MapType mapType = (MapType) dataType;
                updatePaimonColumnTimezone(children.get(0), mapType.getKeyType());
                updatePaimonColumnTimezone(children.get(1), mapType.getValueType());
                break;
            case ROW:
                RowType rowType = (RowType) dataType;
                for (int idx = 0; idx < children.size(); idx++) {
                    updatePaimonColumnTimezone(children.get(idx), rowType.getFields().get(idx).type());
                }
                break;
            default:
                break;
        }
    }

    public static TField getSchemaInfo(DataType dataType, boolean enableVarbinaryMapping,
            boolean enableTimestampTzMapping) {
        TField field = new TField();
        field.setIsOptional(dataType.isNullable());
        TNestedField nestedField = new TNestedField();
        switch (dataType.getTypeRoot()) {
            case ARRAY: {
                TArrayField listField = new TArrayField();
                org.apache.paimon.types.ArrayType paimonArrayType = (org.apache.paimon.types.ArrayType) dataType;
                TFieldPtr fieldPtr = new TFieldPtr();
                fieldPtr.setFieldPtr(getSchemaInfo(paimonArrayType.getElementType(), enableVarbinaryMapping,
                        enableTimestampTzMapping));
                listField.setItemField(fieldPtr);
                nestedField.setArrayField(listField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.ARRAY);
                field.setType(tColumnType);
                break;
            }
            case MAP: {
                TMapField mapField = new TMapField();
                org.apache.paimon.types.MapType mapType = (org.apache.paimon.types.MapType) dataType;
                TFieldPtr keyField = new TFieldPtr();
                keyField.setFieldPtr(
                        getSchemaInfo(mapType.getKeyType(), enableVarbinaryMapping, enableTimestampTzMapping));
                mapField.setKeyField(keyField);
                TFieldPtr valueField = new TFieldPtr();
                valueField.setFieldPtr(
                        getSchemaInfo(mapType.getValueType(), enableVarbinaryMapping, enableTimestampTzMapping));
                mapField.setValueField(valueField);
                nestedField.setMapField(mapField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.MAP);
                field.setType(tColumnType);
                break;
            }
            case ROW: {
                RowType rowType = (RowType) dataType;
                TStructField structField = getSchemaInfo(rowType.getFields(), enableVarbinaryMapping,
                        enableTimestampTzMapping);
                nestedField.setStructField(structField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.STRUCT);
                field.setType(tColumnType);
                break;
            }
            default:
                field.setType(paimonPrimitiveTypeToDorisType(dataType, enableVarbinaryMapping, enableTimestampTzMapping)
                        .toColumnTypeThrift());
                break;
        }
        return field;
    }

    public static TStructField getSchemaInfo(List<DataField> paimonFields, boolean enableVarbinaryMapping,
            boolean enableTimestampTzMapping) {
        TStructField structField = new TStructField();
        for (DataField paimonField : paimonFields) {
            TField childField = getSchemaInfo(paimonField.type(), enableVarbinaryMapping, enableTimestampTzMapping);
            childField.setName(paimonField.name());
            childField.setId(paimonField.id());
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(childField);
            structField.addToFields(fieldPtr);
        }
        return structField;
    }

    public static TSchema getSchemaInfo(TableSchema paimonTableSchema, boolean enableVarbinaryMapping,
            boolean enableTimestampTzMapping) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(paimonTableSchema.id());
        tSchema.setRootField(
                getSchemaInfo(paimonTableSchema.fields(), enableVarbinaryMapping, enableTimestampTzMapping));
        return tSchema;
    }

    public static TSchema getHistorySchemaInfo(ExternalTable targetTable, TableSchema sourceSchema,
            boolean enableVarbinaryMapping, boolean enableTimestampTzMapping) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(sourceSchema.id());
        tSchema.setRootField(getSchemaInfo(resolveHistorySchemaFields(targetTable, sourceSchema.fields()),
                enableVarbinaryMapping, enableTimestampTzMapping));
        return tSchema;
    }

    private static List<DataField> resolveHistorySchemaFields(ExternalTable targetTable, List<DataField> sourceFields) {
        if (!(targetTable instanceof PaimonSysExternalTable)) {
            return sourceFields;
        }

        PaimonSysExternalTable sysTable = (PaimonSysExternalTable) targetTable;
        boolean withSequenceNumber = isTableReadSequenceNumberEnabled(sysTable);
        switch (sysTable.getSysTableType()) {
            case SYS_TABLE_TYPE_AUDIT_LOG:
                return buildAuditLogHistoryFields(sourceFields, withSequenceNumber);
            case SYS_TABLE_TYPE_BINLOG:
                return buildBinlogHistoryFields(sourceFields, withSequenceNumber);
            default:
                return sourceFields;
        }
    }

    private static List<DataField> buildAuditLogHistoryFields(List<DataField> sourceFields,
            boolean withSequenceNumber) {
        List<DataField> fields = new ArrayList<>(sourceFields.size() + (withSequenceNumber ? 2 : 1));
        fields.add(SpecialFields.ROW_KIND);
        if (withSequenceNumber) {
            fields.add(SpecialFields.SEQUENCE_NUMBER);
        }
        fields.addAll(sourceFields);
        return fields;
    }

    private static List<DataField> buildBinlogHistoryFields(List<DataField> sourceFields,
            boolean withSequenceNumber) {
        List<DataField> fields = new ArrayList<>(sourceFields.size() + (withSequenceNumber ? 2 : 1));
        fields.add(SpecialFields.ROW_KIND);
        if (withSequenceNumber) {
            fields.add(SpecialFields.SEQUENCE_NUMBER);
        }
        for (DataField sourceField : sourceFields) {
            fields.add(sourceField.newType(new ArrayType(sourceField.type().nullable())));
        }
        return fields;
    }

    private static boolean isTableReadSequenceNumberEnabled(PaimonSysExternalTable sysTable) {
        if (!SYS_TABLE_TYPE_AUDIT_LOG.equals(sysTable.getSysTableType())
                && !SYS_TABLE_TYPE_BINLOG.equals(sysTable.getSysTableType())) {
            return false;
        }
        try {
            String optionValue = sysTable.getTableProperties().get(TABLE_READ_SEQUENCE_NUMBER_ENABLED);
            return Boolean.parseBoolean(optionValue);
        } catch (Exception e) {
            LOG.warn("Failed to parse table-read.sequence-number.enabled for Paimon system table {}: {}",
                    sysTable.getName(), e.getMessage());
            return false;
        }
    }

    public static List<Column> parseSchema(Table table, boolean enableVarbinaryMapping,
            boolean enableTimestampTzMapping) {
        List<String> primaryKeys = table.primaryKeys();
        return parseSchema(table.rowType(), primaryKeys, enableVarbinaryMapping, enableTimestampTzMapping);
    }

    public static List<Column> parseSchema(RowType rowType, List<String> primaryKeys, boolean enableVarbinaryMapping,
            boolean enableTimestampTzMapping) {
        List<Column> resSchema = Lists.newArrayListWithCapacity(rowType.getFields().size());
        rowType.getFields().forEach(field -> {
            Column column = new Column(field.name(),
                    PaimonUtil.paimonTypeToDorisType(field.type(), enableVarbinaryMapping, enableTimestampTzMapping),
                    primaryKeys.contains(field.name()),
                    null,
                    field.type().isNullable(),
                    field.description(),
                    true,
                    field.id());
            // Schema selected by relation-local options must expose the same recursive metadata
            // as the normal schema cache, otherwise nested predicates bind to different field IDs.
            updatePaimonColumnMetadata(column, field);
            resSchema.add(column);
        });
        return resSchema;
    }

    public static <T> String encodeObjectToString(T t) {
        byte[] bytes = serializeObject(t);
        return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
    }

    public static <T> byte[] serializeObject(T object) {
        try {
            return InstantiationUtil.serializeObject(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserializeObject(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(bytes, PaimonUtil.class.getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serialize DataSplit using Paimon's native binary format.
     * This format is compatible with paimon-cpp reader.
     * Uses standard Base64 encoding (not URL-safe) for BE compatibility.
     */
    public static String encodeDataSplitToString(DataSplit split) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
            split.serialize(out);
            byte[] bytes = baos.toByteArray();
            return Base64.getEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize DataSplit using Paimon native format", e);
        }
    }

    public static Map<String, String> getPartitionInfoMap(Table table, BinaryRow partitionValues, String timeZone) {
        Map<String, String> partitionInfoMap = new HashMap<>();
        List<String> partitionKeys = table.partitionKeys();
        RowType partitionType = table.rowType().project(partitionKeys);
        RowDataToObjectArrayConverter toObjectArrayConverter = new RowDataToObjectArrayConverter(
                partitionType);
        Object[] partitionValuesArray = toObjectArrayConverter.convert(partitionValues);
        for (int i = 0; i < partitionKeys.size(); i++) {
            try {
                String partitionValue = serializePartitionValue(partitionType.getFields().get(i).type(),
                        partitionValuesArray[i], timeZone);
                partitionInfoMap.put(partitionKeys.get(i), partitionValue);
            } catch (UnsupportedOperationException e) {
                LOG.warn("Failed to serialize table {} partition value for key {}: {}", table.name(),
                        partitionKeys.get(i), e.getMessage());
                return null;
            }
        }
        return partitionInfoMap;
    }

    private static String serializePartitionValue(org.apache.paimon.types.DataType type, Object value,
            String timeZone) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case SMALLINT:
            case TINYINT:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
                if (value == null) {
                    return null;
                }
                return value.toString();
            case FLOAT:
                if (value == null) {
                    return null;
                }
                return Float.toString((Float) value);
            case DOUBLE:
                if (value == null) {
                    return null;
                }
                return Double.toString((Double) value);
            // case binary:
            // case varbinary: should not supported, because if return string with utf8,
            // the data maybe be corrupted
            case DATE:
                if (value == null) {
                    return null;
                }
                // Paimon date is stored as days since epoch
                LocalDate date = LocalDate.ofEpochDay((Integer) value);
                return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIME_WITHOUT_TIME_ZONE:
                if (value == null) {
                    return null;
                }
                // Paimon time is stored as microseconds since midnight in utc
                long micros = (Long) value;
                LocalTime time = LocalTime.ofNanoOfDay(micros * 1000);
                return time.format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (value == null) {
                    return null;
                }
                // Format through Doris' target type instead of translating between Paimon's
                // timestamp text and Doris' partition-literal syntax by hand.
                TimestampType timestampType = (TimestampType) type;
                ScalarType dorisType = ScalarType.createDatetimeV2Type(
                        Math.min(timestampType.getPrecision(), 6));
                return new DateLiteral(((Timestamp) value).toLocalDateTime(), dorisType)
                        .getStringValue();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value == null) {
                    return null;
                }
                // Paimon timestamp with local time zone is stored as Timestamp type in utc
                Timestamp timestamp = (Timestamp) value;
                return timestamp.toLocalDateTime()
                        .atZone(ZoneId.of("UTC"))
                        .withZoneSameInstant(ZoneId.of(timeZone))
                        .toLocalDateTime()
                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            default:
                throw new UnsupportedOperationException("Unsupported type for serializePartitionValue: " + type);
        }
    }

    /**
     * Extracts the reference name (branch or tag name) from table scan parameters.
     *
     * @param scanParams the scan parameters containing reference name information
     * @return the extracted reference name
     * @throws IllegalArgumentException if the reference name is not properly specified
     */
    public static String extractBranchOrTagName(TableScanParams scanParams) {
        if (!scanParams.getMapParams().isEmpty()) {
            if (!scanParams.getMapParams().containsKey(TableScanParams.PARAMS_NAME)) {
                throw new IllegalArgumentException("must contain key 'name' in params");
            }
            return scanParams.getMapParams().get(TableScanParams.PARAMS_NAME);
        } else {
            if (scanParams.getListParams().isEmpty() || scanParams.getListParams().get(0) == null) {
                throw new IllegalArgumentException("must contain a branch/tag name in params");
            }
            return scanParams.getListParams().get(0);
        }
    }

    static Snapshot getPaimonSnapshotByTimestamp(DataTable table, String timestamp, boolean isDigital)
            throws UserException {
        long timestampMillis = 0;
        if (isDigital) {
            timestampMillis = Long.parseLong(timestamp);
        } else {
            // Supported formats include：yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS.
            // use default local time zone.
            timestampMillis = DateTimeUtils.parseTimestampData(timestamp, 3, TimeUtils.getTimeZone()).getMillisecond();
            if (timestampMillis < 0) {
                throw new DateTimeException("can't parse time: " + timestamp);
            }
        }
        Snapshot snapshot = table.snapshotManager().earlierOrEqualTimeMills(timestampMillis);
        if (snapshot == null) {
            Snapshot earliestSnapshot = table.snapshotManager().earliestSnapshot();
            throw new UserException(
                    String.format(
                            "There is currently no snapshot earlier than or equal to timestamp [%s], "
                                    + "the earliest snapshot's timestamp is [%s]",
                            timestampMillis,
                            earliestSnapshot == null
                                    ? "null"
                                    : String.valueOf(earliestSnapshot.timeMillis())));
        }
        return snapshot;
    }

    static Snapshot getPaimonSnapshotBySnapshotId(DataTable table, String snapshotString)
            throws UserException {
        long snapshotId = Long.parseLong(snapshotString);
        try {
            Snapshot snapshot = table.snapshotManager().tryGetSnapshot(snapshotId);
            return snapshot;
        } catch (FileNotFoundException e) {
            throw new UserException("can't find snapshot by id: " + snapshotId, e);
        }
    }

    static Snapshot getPaimonSnapshotByTag(DataTable table, String tagName)
            throws UserException {
        Optional<Tag> tag = table.tagManager().get(tagName);
        return tag.orElseThrow(() -> new UserException("can't find snapshot by tag: " + tagName));
    }


    public static String resolvePaimonBranch(TableScanParams tableScanParams, Table baseTable)
            throws UserException {
        String branchName = extractBranchOrTagName(tableScanParams);
        if (!(baseTable instanceof FileStoreTable)) {
            throw new UserException("Table type should be FileStoreTable but got: " + baseTable.getClass().getName());
        }

        final FileStoreTable fileStoreTable = (FileStoreTable) baseTable;
        if (!fileStoreTable.branchManager().branchExists(branchName)) {
            throw new UserException("can't find branch: " + branchName);
        }
        return branchName;
    }
}
