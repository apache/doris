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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HiveUtil;
import org.apache.doris.datasource.paimon.source.PaimonSource;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class PaimonUtil {
    private static final Logger LOG = LogManager.getLogger(PaimonUtil.class);
    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();
    private static final Pattern DIGITAL_REGEX = Pattern.compile("\\d+");

    private static final List<ConfigOption<?>> PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS = Arrays.asList(
            CoreOptions.SCAN_SNAPSHOT_ID,
            CoreOptions.SCAN_TAG_NAME,
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
            CoreOptions.INCREMENTAL_BETWEEN,
            CoreOptions.INCREMENTAL_TO_AUTO_TAG);

    private static final List<ConfigOption<?>> PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS = Arrays.asList(
            CoreOptions.SCAN_TIMESTAMP_MILLIS,
            CoreOptions.SCAN_TIMESTAMP,
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
            CoreOptions.INCREMENTAL_BETWEEN,
            CoreOptions.INCREMENTAL_TO_AUTO_TAG);

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

    public static PaimonPartitionInfo generatePartitionInfo(List<Column> partitionColumns,
            List<Partition> paimonPartitions) {

        if (CollectionUtils.isEmpty(partitionColumns) || paimonPartitions.isEmpty()) {
            return PaimonPartitionInfo.EMPTY;
        }

        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, Partition> nameToPartition = Maps.newHashMap();
        PaimonPartitionInfo partitionInfo = new PaimonPartitionInfo(nameToPartitionItem, nameToPartition);
        List<Type> types = partitionColumns.stream()
                .map(Column::getType)
                .collect(Collectors.toList());
        Map<String, Type> columnNameToType = partitionColumns.stream()
                .collect(Collectors.toMap(Column::getName, Column::getType));

        for (Partition partition : paimonPartitions) {
            Map<String, String> spec = partition.spec();
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : spec.entrySet()) {
                sb.append(entry.getKey()).append("=");
                // Paimon stores DATE type as days since 1970-01-01 (epoch), so we convert the integer to a date string.
                if (columnNameToType.getOrDefault(entry.getKey(), Type.NULL).isDateV2()) {
                    sb.append(DateTimeUtils.formatDate(Integer.parseInt(entry.getValue()))).append("/");
                } else {
                    sb.append(entry.getValue()).append("/");
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            String partitionName = sb.toString();
            nameToPartition.put(partitionName, partition);
            try {
                // partition values return by paimon api, may have problem,
                // to avoid affecting the query, we catch exceptions here
                nameToPartitionItem.put(partitionName, toListPartitionItem(partitionName, types));
            } catch (Exception e) {
                LOG.warn("toListPartitionItem failed, partitionColumns: {}, partitionValues: {}",
                        partitionColumns, partition.spec(), e);
            }
        }
        return partitionInfo;
    }

    public static ListPartitionItem toListPartitionItem(String partitionName, List<Type> types)
            throws AnalysisException {
        // Partition name will be in format: nation=cn/city=beijing
        // parse it to get values "cn" and "beijing"
        List<String> partitionValues = HiveUtil.toPartitionValues(partitionName);
        Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String partitionValue : partitionValues) {
            // null  will in partition 'null'
            // "null" will in partition 'null'
            // NULL  will in partition 'null'
            // "NULL" will in partition 'NULL'
            // values.add(new PartitionValue(partitionValue, "null".equals(partitionValue)));
            values.add(new PartitionValue(partitionValue, false));
        }
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
        ListPartitionItem listPartitionItem = new ListPartitionItem(Lists.newArrayList(key));
        return listPartitionItem;
    }

    private static Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType) {
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
                return ScalarType.createVarcharType(((VarCharType) dataType).getLength());
            case CHAR:
                return ScalarType.createCharType(((CharType) dataType).getLength());
            case BINARY:
            case VARBINARY:
                return Type.STRING;
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
                return ScalarType.createDatetimeV2Type(tsScale);
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Type innerType = paimonPrimitiveTypeToDorisType(arrayType.getElementType());
                return org.apache.doris.catalog.ArrayType.create(innerType, true);
            case MAP:
                MapType mapType = (MapType) dataType;
                return new org.apache.doris.catalog.MapType(
                        paimonTypeToDorisType(mapType.getKeyType()), paimonTypeToDorisType(mapType.getValueType()));
            case ROW:
                RowType rowType = (RowType) dataType;
                List<DataField> fields = rowType.getFields();
                return new org.apache.doris.catalog.StructType(fields.stream()
                        .map(field -> new org.apache.doris.catalog.StructField(field.name(),
                                paimonTypeToDorisType(field.type())))
                        .collect(Collectors.toCollection(ArrayList::new)));
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                LOG.warn("Cannot transform unknown type: " + dataType.getTypeRoot());
                return Type.UNSUPPORTED;
        }
    }

    public static Type paimonTypeToDorisType(org.apache.paimon.types.DataType type) {
        return paimonPrimitiveTypeToDorisType(type);
    }

    public static void updatePaimonColumnUniqueId(Column column, DataType dataType) {
        List<Column> columns = column.getChildren();
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

    public static TField getSchemaInfo(DataType dataType) {
        TField field = new TField();
        field.setIsOptional(dataType.isNullable());
        TNestedField nestedField = new TNestedField();
        switch (dataType.getTypeRoot()) {
            case ARRAY: {
                TArrayField listField = new TArrayField();
                org.apache.paimon.types.ArrayType paimonArrayType = (org.apache.paimon.types.ArrayType) dataType;
                TFieldPtr fieldPtr = new TFieldPtr();
                fieldPtr.setFieldPtr(getSchemaInfo(paimonArrayType.getElementType()));
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
                keyField.setFieldPtr(getSchemaInfo(mapType.getKeyType()));
                mapField.setKeyField(keyField);
                TFieldPtr valueField = new TFieldPtr();
                valueField.setFieldPtr(getSchemaInfo(mapType.getValueType()));
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
                TStructField structField = getSchemaInfo(rowType.getFields());
                nestedField.setStructField(structField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.STRUCT);
                field.setType(tColumnType);
                break;
            }
            default:
                field.setType(paimonPrimitiveTypeToDorisType(dataType).toColumnTypeThrift());
                break;
        }
        return field;
    }

    public static TStructField getSchemaInfo(List<DataField> paimonFields) {
        TStructField structField = new TStructField();
        for (DataField paimonField : paimonFields) {
            TField childField = getSchemaInfo(paimonField.type());
            childField.setName(paimonField.name());
            childField.setId(paimonField.id());
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(childField);
            structField.addToFields(fieldPtr);
        }
        return structField;
    }

    public static TSchema getSchemaInfo(TableSchema paimonTableSchema) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(paimonTableSchema.id());
        tSchema.setRootField(getSchemaInfo(paimonTableSchema.fields()));
        return tSchema;
    }

    public static List<Column> parseSchema(Table table) {
        List<String> primaryKeys = table.primaryKeys();
        return parseSchema(table.rowType(), primaryKeys);
    }

    public static List<Column> parseSchema(RowType rowType, List<String> primaryKeys) {
        List<Column> resSchema = Lists.newArrayListWithCapacity(rowType.getFields().size());
        rowType.getFields().forEach(field -> {
            resSchema.add(new Column(field.name().toLowerCase(),
                    PaimonUtil.paimonTypeToDorisType(field.type()),
                    primaryKeys.contains(field.name()),
                    null,
                    field.type().isNullable(),
                    field.description(),
                    true,
                    field.id()));
        });
        return resSchema;
    }

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            case BINARY:
            case VARBINARY:
                if (value == null) {
                    return null;
                }
                return new String((byte[]) value, StandardCharsets.UTF_8);
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
                // Paimon timestamp is stored as Timestamp type in utc
                return ((Timestamp) value).toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
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
     * Builds a snapshot-specific table for time travel queries.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param tableSnapshot the snapshot specification (type + value)
     * @return a Table instance configured for the specified time travel query
     * @throws UserException if snapshot configuration is invalid
     */
    public static Table buildSnapshotTable(Table baseTable, TableSnapshot tableSnapshot)
            throws UserException {
        final String value = tableSnapshot.getValue();
        final TableSnapshot.VersionType type = tableSnapshot.getType();
        final boolean isDigital = DIGITAL_REGEX.matcher(value).matches();

        switch (type) {
            case TIME:
                return isDigital
                        ? buildSnapshotTimestampMillisTable(baseTable, value)
                        : buildSnapshotTimestampTable(baseTable, value);

            case VERSION:
                if (isDigital) {
                    return buildSnapshotIdTable(baseTable, value);
                }
                return buildTagTable(baseTable, value);

            default:
                throw new UserException(String.format("Unsupported version type: %s", type));
        }
    }

    /**
     * Builds a table configured to read from a specific snapshot ID.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param snapshotId the snapshot ID as a string
     * @return a Table instance configured to read from the specified snapshot ID
     */
    public static Table buildSnapshotIdTable(Table baseTable, String snapshotId) {
        Map<String, String> options = new HashMap<>(
                PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS.size() + 3);

        // For Paimon FROM_SNAPSHOT startup mode, must set only one key in:
        // [scan_tag_name, scan_watermark, scan_snapshot_id]
        options.put(CoreOptions.SCAN_TAG_NAME.key(), null);
        options.put(CoreOptions.SCAN_WATERMARK.key(), null);
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    /**
     * Builds a table configured to read from a specific timestamp.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param timestampStr the timestamp as a string
     * @return a Table instance configured to read from the specified timestamp
     */
    public static Table buildSnapshotTimestampTable(Table baseTable, String timestampStr) {
        Map<String, String> options = new HashMap<>(
                PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS.size() + 3);

        // For Paimon FROM_TIMESTAMP startup mode, must set only one key in:
        // [scan_timestamp, scan_timestamp_millis]
        options.put(CoreOptions.SCAN_MODE.key(), StartupMode.FROM_TIMESTAMP.toString());
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), timestampStr);
        options.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    /**
     * Builds a table configured to read from a specific timestamp in milliseconds.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param timestampStr the timestamp in milliseconds as a string
     * @return a Table instance configured to read from the specified timestamp
     */
    public static Table buildSnapshotTimestampMillisTable(Table baseTable, String timestampStr) {
        Map<String, String> options = new HashMap<>(
                PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS.size() + 3);

        // For Paimon FROM_TIMESTAMP startup mode, must set only one key in:
        // [scan_timestamp, scan_timestamp_millis]
        options.put(CoreOptions.SCAN_MODE.key(), StartupMode.FROM_TIMESTAMP.toString());
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), null);
        options.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), timestampStr);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS));

        return baseTable.copy(options);
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


    /**
     * Builds a branch-specific table for time travel queries.
     *
     * @param source the Paimon source containing catalog and table information
     * @param baseTable the base Paimon table
     * @param branchName the branch name
     * @return a Table instance configured to read from the specified branch
     * @throws UserException if branch does not exist
     */
    public static Table buildBranchTable(PaimonSource source, Table baseTable, String branchName) throws UserException {

        if (!checkBranchExists(baseTable, branchName)) {
            throw new UserException(String.format("Branch '%s' does not exist", branchName));
        }

        PaimonExternalCatalog catalog = (PaimonExternalCatalog) source.getCatalog();
        ExternalTable externalTable = (ExternalTable) source.getTargetTable();
        return catalog.getPaimonTable(externalTable.getOrBuildNameMapping(), branchName, null);
    }

    /**
     * Builds a tag-specific table for time travel queries.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param tagName the tag name
     * @return a Table instance configured to read from the specified tag
     * @throws UserException if tag does not exist
     */
    public static Table buildTagTable(Table baseTable, String tagName) throws UserException {
        if (!checkTagsExists(baseTable, tagName)) {
            throw new UserException(String.format("Tag '%s' does not exist", tagName));
        }

        Map<String, String> options = new HashMap<>(
                PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS.size() + 3);

        // For Paimon FROM_SNAPSHOT startup mode, must set only one key in:
        // [scan_tag_name, scan_watermark, scan_snapshot_id]
        options.put(CoreOptions.SCAN_TAG_NAME.key(), tagName);
        options.put(CoreOptions.SCAN_WATERMARK.key(), null);
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    /**
     * Creates a map of conflicting Paimon options with null values for exclusion.
     *
     * @param illegalOptions the list of ConfigOptions that should be set to null
     * @return a HashMap containing the illegal options as keys with null values
     */
    public static Map<String, String> excludePaimonConflictOptions(List<ConfigOption<?>> illegalOptions) {
        return illegalOptions.stream()
                .collect(HashMap::new,
                        (m, option) -> m.put(option.key(), null),
                        HashMap::putAll);
    }

    /**
     * Checks if a tag exists in the given table.
     *
     * @param baseTable the Paimon table
     * @param tagName the tag name to check
     * @return true if tag exists, false otherwise
     * @throws UserException if table is not a FileStoreTable
     */
    public static boolean checkTagsExists(Table baseTable, String tagName) throws UserException {
        if (!(baseTable instanceof FileStoreTable)) {
            throw new UserException("Table type should be FileStoreTable but got: " + baseTable.getClass().getName());
        }

        final FileStoreTable fileStoreTable = (FileStoreTable) baseTable;
        return fileStoreTable.tagManager().tagExists(tagName);
    }

    /**
     * Checks if a branch exists in the given table.
     *
     * @param baseTable the Paimon table
     * @param branchName the branch name to check
     * @return true if branch exists, false otherwise
     * @throws UserException if table is not a FileStoreTable
     */
    public static boolean checkBranchExists(Table baseTable, String branchName) throws UserException {
        if (!(baseTable instanceof FileStoreTable)) {
            throw new UserException("Table type should be FileStoreTable but got: " + baseTable.getClass().getName());
        }

        final FileStoreTable fileStoreTable = (FileStoreTable) baseTable;
        return fileStoreTable.branchManager().branchExists(branchName);
    }
}
