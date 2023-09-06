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

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DorisRangePartitioner extends DorisPartitioner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadJobV2.class);

    private EtlJobConfig.EtlPartitionInfo partitionInfo;
    private List<PartitionRangeKey> partitionRangeKeys;
    List<Integer> partitionKeyIndexes;

    public DorisRangePartitioner(EtlJobConfig.EtlPartitionInfo partitionInfo,
                                 List<Integer> partitionKeyIndexes,
                                 List<PartitionRangeKey> partitionRangeKeys) {
        this.partitionInfo = partitionInfo;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.partitionRangeKeys = partitionRangeKeys;
    }

    public static DorisRangePartitioner build(
            EtlJobConfig.EtlPartitionInfo partitionInfo,
            List<EtlColumn> columns,
            List<String> keyAndPartitionColumnNames) throws SparkDppException {

        List<Integer> partitionKeyIndex = new ArrayList<>();
        List<Class> partitionKeySchema = new ArrayList<>();
        for (String key : partitionInfo.partitionColumnRefs) {
            for (EtlColumn column : columns) {
                if (column.columnName.equals(key)) {
                    partitionKeyIndex.add(keyAndPartitionColumnNames.indexOf(key));
                    partitionKeySchema.add(DppUtils.getJavaClassFromColumn(column));
                    break;
                }
            }
        }

        List<PartitionRangeKey> partitionRangeKeys = createPartitionRangeKeys(partitionInfo, partitionKeySchema);
        return new DorisRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);
    }


    public static List<PartitionRangeKey> createPartitionRangeKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws SparkDppException {
        List<PartitionRangeKey> partitionRangeKeys = new ArrayList<>();

        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            PartitionRangeKey partitionRangeKey = new DorisRangePartitioner.PartitionRangeKey();
            List<Object> startKeyColumns = new ArrayList<>();

            for (int i = 0; i < partition.startKeys.size(); i++) {
                Object value = partition.startKeys.get(i);
                startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
            }
            partitionRangeKey.startKeys = new DppColumns(startKeyColumns);

            if (!partition.isMaxPartition) {
                partitionRangeKey.isMaxPartition = false;
                List<Object> endKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.endKeys.size(); i++) {
                    Object value = partition.endKeys.get(i);
                    endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.endKeys = new DppColumns(endKeyColumns);
            } else {
                partitionRangeKey.isMaxPartition = true;
            }

            partitionRangeKeys.add(partitionRangeKey);
        }
        return partitionRangeKeys;
    }

    public int numPartitions() {
        if (partitionInfo == null) {
            return 0;
        }
        if (partitionInfo.partitionType.equalsIgnoreCase(UNPARTITIONED_TYPE)) {
            return 1;
        }
        return partitionInfo.partitions.size();
    }

    public int getPartition(Object var1) {
        if (partitionInfo.partitionType != null
                && partitionInfo.partitionType.equalsIgnoreCase(UNPARTITIONED_TYPE)) {
            return 0;
        }
        DppColumns key = (DppColumns) var1;
        // get the partition columns from key as partition key
        DppColumns partitionKey = new DppColumns(key, partitionKeyIndexes);
        // TODO: optimize this by use binary search
        for (int i = 0; i < partitionRangeKeys.size(); ++i) {
            if (partitionRangeKeys.get(i).isRowContained(partitionKey)) {
                return i;
            }
        }
        return -1;
    }

    public static class PartitionRangeKey implements Serializable {
        public boolean isMaxPartition;
        public DppColumns startKeys;
        public DppColumns endKeys;

        public boolean isRowContained(DppColumns row) {
            if (isMaxPartition) {
                return startKeys.compareTo(row) <= 0;
            } else {
                return startKeys.compareTo(row) <= 0 && endKeys.compareTo(row) > 0;
            }
        }

        public String toString() {
            return "PartitionRangeKey{"
                    +  "isMaxPartition=" + isMaxPartition
                    +  ", startKeys=" + startKeys
                    +  ", endKeys=" + endKeys
                    + '}';
        }
    }
}
