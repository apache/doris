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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hudi.source.HudiCachedPartitionProcessor;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class HudiUtils {
    private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Convert different query instant time format to the commit time format.
     * Currently we support three kinds of instant time format for time travel query:
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
            HoodieActiveTimeline.parseDateFromInstantTime(queryInstant); // validate the format
            return queryInstant;
        } else if (instantLength == 10) { // for yyyy-MM-dd
            return HoodieActiveTimeline.formatDate(defaultDateFormat.parse(queryInstant));
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

    public static TablePartitionValues getPartitionValues(Optional<TableSnapshot> tableSnapshot,
            HMSExternalTable hmsTable) {
        TablePartitionValues partitionValues = new TablePartitionValues();
        if (hmsTable.getPartitionColumns().isEmpty()) {
            //isn't partition table.
            return partitionValues;
        }

        HoodieTableMetaClient hudiClient = hmsTable.getHudiClient();
        HudiCachedPartitionProcessor processor = (HudiCachedPartitionProcessor) Env.getCurrentEnv()
                .getExtMetaCacheMgr().getHudiPartitionProcess(hmsTable.getCatalog());
        boolean useHiveSyncPartition = hmsTable.useHiveSyncPartition();

        if (tableSnapshot.isPresent()) {
            if (tableSnapshot.get().getType() == TableSnapshot.VersionType.VERSION) {
                // Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF`";
                return partitionValues;
            }
            String queryInstant = tableSnapshot.get().getTime().replaceAll("[-: ]", "");
            try {
                partitionValues = hmsTable.getCatalog().getPreExecutionAuthenticator().execute(() ->
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
                partitionValues = hmsTable.getCatalog().getPreExecutionAuthenticator().execute(()
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
}
