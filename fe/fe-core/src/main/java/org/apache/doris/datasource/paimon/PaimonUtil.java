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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.hive.HiveUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
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
    public static List<InternalRow> read(
            Table table, @Nullable int[][] projection, Pair<ConfigOption<?>, String>... dynamicOptions)
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
}
