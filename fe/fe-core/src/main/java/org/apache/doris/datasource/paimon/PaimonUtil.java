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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.hive.HiveUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class PaimonUtil {
    private static final Logger LOG = LogManager.getLogger(PaimonUtil.class);

    public static List<InternalRow> read(
            Table table, @Nullable int[][] projection, @Nullable Predicate predicate,
            Pair<ConfigOption<?>, String>... dynamicOptions)
            throws IOException {
        Map<String, String> options = new HashMap<>();
        for (Pair<ConfigOption<?>, String> pair : dynamicOptions) {
            options.put(pair.getKey().key(), pair.getValue());
        }
        table = table.copy(options);
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


    /*
    https://paimon.apache.org/docs/0.9/maintenance/system-tables/#partitions-table
    +---------------+----------------+--------------------+--------------------+------------------------+
    |  partition    |   record_count |  file_size_in_bytes|          file_count|        last_update_time|
    +---------------+----------------+--------------------+--------------------+------------------------+
    |  [1]          |           1    |             645    |                1   | 2024-06-24 10:25:57.400|
    +---------------+----------------+--------------------+--------------------+------------------------+
    org.apache.paimon.table.system.PartitionsTable.TABLE_TYPE
    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "record_count", new BigIntType(false)),
                            new DataField(2, "file_size_in_bytes", new BigIntType(false)),
                            new DataField(3, "file_count", new BigIntType(false)),
                            new DataField(4, "last_update_time", DataTypes.TIMESTAMP_MILLIS())));
    */
    public static PaimonPartition rowToPartition(InternalRow row) {
        String partition = row.getString(0).toString();
        long recordCount = row.getLong(1);
        long fileSizeInBytes = row.getLong(2);
        long fileCount = row.getLong(3);
        long lastUpdateTime = row.getTimestamp(4, 3).getMillisecond();
        return new PaimonPartition(partition, recordCount, fileSizeInBytes, fileCount, lastUpdateTime);
    }

    public static PaimonPartitionInfo generatePartitionInfo(List<Column> partitionColumns,
            List<PaimonPartition> paimonPartitions) throws AnalysisException {
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, PaimonPartition> nameToPartition = Maps.newHashMap();
        PaimonPartitionInfo partitionInfo = new PaimonPartitionInfo(nameToPartitionItem, nameToPartition);
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return partitionInfo;
        }
        for (PaimonPartition paimonPartition : paimonPartitions) {
            String partitionName = getPartitionName(partitionColumns, paimonPartition.getPartitionValues());
            nameToPartition.put(partitionName, paimonPartition);
            nameToPartitionItem.put(partitionName, toListPartitionItem(partitionName, partitionColumns));
        }
        return partitionInfo;
    }

    private static String getPartitionName(List<Column> partitionColumns, String partitionValueStr) {
        Preconditions.checkNotNull(partitionValueStr);
        String[] partitionValues = partitionValueStr.replace("[", "").replace("]", "")
                .split(",");
        Preconditions.checkState(partitionColumns.size() == partitionValues.length);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionColumns.size(); ++i) {
            if (i != 0) {
                sb.append("/");
            }
            sb.append(partitionColumns.get(i).getName()).append("=").append(partitionValues[i]);
        }
        return sb.toString();
    }

    public static ListPartitionItem toListPartitionItem(String partitionName, List<Column> partitionColumns)
            throws AnalysisException {
        List<Type> types = partitionColumns.stream()
                .map(Column::getType)
                .collect(Collectors.toList());
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
            case BINARY:
            case CHAR:
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

    /**
     * https://paimon.apache.org/docs/0.9/maintenance/system-tables/#schemas-table
     * demo:
     * 0
     * [{"id":0,"name":"user_id","type":"BIGINT NOT NULL"},
     * {"id":1,"name":"item_id","type":"BIGINT"},
     * {"id":2,"name":"behavior","type":"STRING"},
     * {"id":3,"name":"dt","type":"STRING NOT NULL"},
     * {"id":4,"name":"hh","type":"STRING NOT NULL"}]
     * ["dt"]
     * ["dt","hh","user_id"]
     * {"owner":"hadoop","provider":"paimon"}
     * 2024-12-03 15:38:14.734
     *
     * @param row
     * @return
     */
    public static PaimonSchema rowToSchema(InternalRow row) {
        long schemaId = row.getLong(0);
        String fieldsStr = row.getString(1).toString();
        String partitionKeysStr = row.getString(2).toString();
        List<DataField> fields = JsonSerdeUtil.fromJson(fieldsStr, new TypeReference<List<DataField>>() {
        });
        List<String> partitionKeys = JsonSerdeUtil.fromJson(partitionKeysStr, new TypeReference<List<String>>() {
        });
        return new PaimonSchema(schemaId, fields, partitionKeys);
    }
}
