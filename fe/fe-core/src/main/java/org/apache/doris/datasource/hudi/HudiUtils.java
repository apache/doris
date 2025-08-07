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

package org.apache.doris.datasource.hudi;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hudi.source.HudiCachedPartitionProcessor;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class HudiUtils {
    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Convert different query instant time format to the commit time format.
     * Currently we support three kinds of instant time format for time travel
     * query:
     * 1、yyyy-MM-dd HH:mm:ss
     * 2、yyyy-MM-dd
     * This will convert to 'yyyyMMdd000000'.
     * 3、yyyyMMddHHmmss
     */
    public static String formatQueryInstant(String queryInstant) throws ParseException {
        int instantLength = queryInstant.length();
        if (instantLength == 19 || instantLength == 23) { // for yyyy-MM-dd HH:mm:ss[.SSS]
            if (instantLength == 19) {
                queryInstant += ".000";
            }
            return HoodieInstantTimeGenerator.getInstantForDateString(queryInstant);
        } else if (instantLength == HoodieInstantTimeGenerator.SECS_INSTANT_ID_LENGTH
                || instantLength == HoodieInstantTimeGenerator.MILLIS_INSTANT_ID_LENGTH) { // for yyyyMMddHHmmss[SSS]
            HoodieInstantTimeGenerator.parseDateFromInstantTime(queryInstant); // validate the format
            return queryInstant;
        } else if (instantLength == 10) { // for yyyy-MM-dd
            LocalDate date = LocalDate.parse(queryInstant, DEFAULT_DATE_FORMATTER);
            return HoodieInstantTimeGenerator.formatDate(java.sql.Date.valueOf(date));
        } else {
            throw new IllegalArgumentException("Unsupported query instant time format: " + queryInstant
                    + ", Supported time format are: 'yyyy-MM-dd HH:mm:ss[.SSS]' "
                    + "or 'yyyy-MM-dd' or 'yyyyMMddHHmmss[SSS]'");
        }
    }

    public static String convertAvroToHiveType(Schema schema) {
        Schema.Type type = schema.getType();
        LogicalType logicalType = schema.getLogicalType();

        switch (type) {
            case BOOLEAN:
                return "boolean";
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return "date";
                }
                if (logicalType instanceof LogicalTypes.TimeMillis) {
                    return handleUnsupportedType(schema);
                }
                return "int";
            case LONG:
                if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    return "timestamp";
                }
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    return handleUnsupportedType(schema);
                }
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    return String.format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
                }
                return "string";
            case ARRAY:
                String arrayElementType = convertAvroToHiveType(schema.getElementType());
                return String.format("array<%s>", arrayElementType);
            case RECORD:
                List<Field> recordFields = schema.getFields();
                if (recordFields.isEmpty()) {
                    throw new IllegalArgumentException("Record must have fields");
                }
                String structFields = recordFields.stream()
                        .map(field -> String.format("%s:%s", field.name(), convertAvroToHiveType(field.schema())))
                        .collect(Collectors.joining(","));
                return String.format("struct<%s>", structFields);
            case MAP:
                Schema mapValueType = schema.getValueType();
                String mapValueTypeString = convertAvroToHiveType(mapValueType);
                return String.format("map<string,%s>", mapValueTypeString);
            case UNION:
                List<Schema> unionTypes = schema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .collect(Collectors.toList());
                if (unionTypes.size() == 1) {
                    return convertAvroToHiveType(unionTypes.get(0));
                }
                break;
            default:
                break;
        }

        throw new IllegalArgumentException(
                String.format("Unsupported type: %s for column: %s", type.getName(), schema.getName()));
    }

    private static String handleUnsupportedType(Schema schema) {
        throw new IllegalArgumentException(String.format("Unsupported logical type: %s", schema.getLogicalType()));
    }

    public static void updateHudiColumnUniqueId(Column column, Types.Field hudiInternalfield) {
        column.setUniqueId(hudiInternalfield.fieldId());

        List<Types.Field> hudiInternalfields = new ArrayList<>();
        switch (hudiInternalfield.type().typeId()) {
            case ARRAY:
                hudiInternalfields = ((Types.ArrayType) hudiInternalfield.type()).fields();
                break;
            case MAP:
                hudiInternalfields = ((Types.MapType) hudiInternalfield.type()).fields();
                break;
            case RECORD:
                hudiInternalfields = ((Types.RecordType) hudiInternalfield.type()).fields();
                break;
            default:
                return;
        }

        List<Column> childColumns = column.getChildren();
        for (int idx = 0; idx < childColumns.size(); idx++) {
            updateHudiColumnUniqueId(childColumns.get(idx), hudiInternalfields.get(idx));
        }
    }

    public static Type fromAvroHudiTypeToDorisType(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();

        switch (columnType) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INT:
                return handleIntType(logicalType);
            case LONG:
                return handleLongType(logicalType);
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case STRING:
                return Type.STRING;
            case FIXED:
            case BYTES:
                return handleFixedOrBytesType(logicalType);
            case ARRAY:
                return handleArrayType(avroSchema);
            case RECORD:
                return handleRecordType(avroSchema);
            case MAP:
                return handleMapType(avroSchema);
            case UNION:
                return handleUnionType(avroSchema);
            default:
                return Type.UNSUPPORTED;
        }
    }

    private static Type handleIntType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.Date) {
            return ScalarType.createDateV2Type();
        }
        if (logicalType instanceof LogicalTypes.TimeMillis) {
            return ScalarType.createTimeV2Type(3);
        }
        return Type.INT;
    }

    private static Type handleLongType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.TimeMicros) {
            return ScalarType.createTimeV2Type(6);
        }
        if (logicalType instanceof LogicalTypes.TimestampMillis) {
            return ScalarType.createDatetimeV2Type(3);
        }
        if (logicalType instanceof LogicalTypes.TimestampMicros) {
            return ScalarType.createDatetimeV2Type(6);
        }
        return Type.BIGINT;
    }

    private static Type handleFixedOrBytesType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.Decimal) {
            int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
            int scale = ((LogicalTypes.Decimal) logicalType).getScale();
            return ScalarType.createDecimalV3Type(precision, scale);
        }
        return Type.STRING;
    }

    private static Type handleArrayType(Schema avroSchema) {
        Type innerType = fromAvroHudiTypeToDorisType(avroSchema.getElementType());
        return ArrayType.create(innerType, true);
    }

    private static Type handleRecordType(Schema avroSchema) {
        ArrayList<StructField> fields = new ArrayList<>();
        avroSchema.getFields().forEach(
                f -> fields.add(new StructField(f.name(), fromAvroHudiTypeToDorisType(f.schema()))));
        return new StructType(fields);
    }

    private static Type handleMapType(Schema avroSchema) {
        return new MapType(Type.STRING, fromAvroHudiTypeToDorisType(avroSchema.getValueType()));
    }

    private static Type handleUnionType(Schema avroSchema) {
        List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                .collect(Collectors.toList());
        if (nonNullMembers.size() == 1) {
            return fromAvroHudiTypeToDorisType(nonNullMembers.get(0));
        }
        return Type.UNSUPPORTED;
    }

    public static HudiMvccSnapshot getHudiMvccSnapshot(Optional<TableSnapshot> tableSnapshot,
            HMSExternalTable hmsTable) {
        long timestamp = 0L;
        if (tableSnapshot.isPresent()) {
            String queryInstant = tableSnapshot.get().getValue().replaceAll("[-: ]", "");
            timestamp = Long.parseLong(queryInstant);
        } else {
            timestamp = getLastTimeStamp(hmsTable);
        }

        return new HudiMvccSnapshot(HudiUtils.getPartitionValues(tableSnapshot, hmsTable), timestamp);
    }

    public static long getLastTimeStamp(HMSExternalTable hmsTable) {
        HoodieTableMetaClient hudiClient = hmsTable.getHudiClient();
        HoodieTimeline timeline = hudiClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> snapshotInstant = timeline.lastInstant();
        if (!snapshotInstant.isPresent()) {
            return 0L;
        }
        return Long.parseLong(snapshotInstant.get().requestedTime());
    }

    public static TablePartitionValues getPartitionValues(Optional<TableSnapshot> tableSnapshot,
            HMSExternalTable hmsTable) {
        TablePartitionValues partitionValues = new TablePartitionValues();

        HoodieTableMetaClient hudiClient = hmsTable.getHudiClient();
        HudiCachedPartitionProcessor processor = (HudiCachedPartitionProcessor) Env.getCurrentEnv()
                .getExtMetaCacheMgr().getHudiPartitionProcess(hmsTable.getCatalog());
        boolean useHiveSyncPartition = hmsTable.useHiveSyncPartition();

        if (tableSnapshot.isPresent()) {
            if (tableSnapshot.get().getType() == TableSnapshot.VersionType.VERSION) {
                // Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF`";
                return partitionValues;
            }
            String queryInstant = tableSnapshot.get().getValue().replaceAll("[-: ]", "");
            try {
                partitionValues = hmsTable.getCatalog().getExecutionAuthenticator().execute(() ->
                        processor.getSnapshotPartitionValues(hmsTable, hudiClient, queryInstant, useHiveSyncPartition));
            } catch (Exception e) {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
        } else {
            HoodieTimeline timeline = hudiClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
            Option<HoodieInstant> snapshotInstant = timeline.lastInstant();
            if (!snapshotInstant.isPresent()) {
                return partitionValues;
            }
            try {
                partitionValues = hmsTable.getCatalog().getExecutionAuthenticator().execute(()
                        -> processor.getPartitionValues(hmsTable, hudiClient, useHiveSyncPartition));
            } catch (Exception e) {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
        return partitionValues;
    }

    public static HoodieTableMetaClient buildHudiTableMetaClient(String hudiBasePath, Configuration conf) {
        HadoopStorageConfiguration hadoopStorageConfiguration = new HadoopStorageConfiguration(conf);
        return HiveMetaStoreClientHelper.ugiDoAs(
            conf,
            () -> HoodieTableMetaClient.builder()
                .setConf(hadoopStorageConfiguration).setBasePath(hudiBasePath).build());
    }



    public static HudiSchemaCacheValue getSchemaCacheValue(HMSExternalTable hmsTable, String queryInstant) {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(hmsTable.getCatalog());
        SchemaCacheKey key = new HudiSchemaCacheKey(hmsTable.getOrBuildNameMapping(), Long.parseLong(queryInstant));
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(key);
        return (HudiSchemaCacheValue) schemaCacheValue.get();
    }

    public static TStructField getSchemaInfo(List<Types.Field> hudiFields) {
        TStructField structField = new TStructField();
        for (Types.Field field : hudiFields) {
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(getSchemaInfo(field));
            structField.addToFields(fieldPtr);
        }
        return structField;
    }


    public static TField getSchemaInfo(Types.Field hudiInternalField) {
        TField root = new TField();
        root.setName(hudiInternalField.name());
        root.setId(hudiInternalField.fieldId());
        root.setIsOptional(hudiInternalField.isOptional());

        TNestedField nestedField = new TNestedField();
        switch (hudiInternalField.type().typeId()) {
            case ARRAY: {
                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.ARRAY);
                root.setType(tColumnType);

                TArrayField listField = new TArrayField();
                List<Types.Field> hudiFields = ((Types.ArrayType) hudiInternalField.type()).fields();
                TFieldPtr fieldPtr = new TFieldPtr();
                fieldPtr.setFieldPtr(getSchemaInfo(hudiFields.get(0)));
                listField.setItemField(fieldPtr);
                nestedField.setArrayField(listField);
                root.setNestedField(nestedField);
                break;
            } case MAP: {
                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.MAP);
                root.setType(tColumnType);

                TMapField mapField = new TMapField();
                List<Types.Field> hudiFields = ((Types.MapType) hudiInternalField.type()).fields();
                TFieldPtr keyPtr = new TFieldPtr();
                keyPtr.setFieldPtr(getSchemaInfo(hudiFields.get(0)));
                mapField.setKeyField(keyPtr);
                TFieldPtr valuePtr = new TFieldPtr();
                valuePtr.setFieldPtr(getSchemaInfo(hudiFields.get(1)));
                mapField.setValueField(valuePtr);
                nestedField.setMapField(mapField);
                root.setNestedField(nestedField);
                break;
            } case RECORD: {
                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.STRUCT);
                root.setType(tColumnType);

                List<Types.Field> hudiFields = ((Types.RecordType) hudiInternalField.type()).fields();
                nestedField.setStructField(getSchemaInfo(hudiFields));
                root.setNestedField(nestedField);
                break;
            } default: {
                root.setType(fromAvroHudiTypeToDorisType(AvroInternalSchemaConverter.convert(
                        hudiInternalField.type(), hudiInternalField.name())).toColumnTypeThrift());
                break;
            }
        }
        return root;
    }

    public static TSchema getSchemaInfo(InternalSchema hudiInternalSchema) {
        TSchema tschema = new TSchema();
        tschema.setSchemaId(hudiInternalSchema.schemaId());
        tschema.setRootField(getSchemaInfo(hudiInternalSchema.getRecord().fields()));
        return tschema;
    }
}
