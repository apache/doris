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

import org.apache.doris.sparkdpp.EtlJobConfig;

import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.List;

public class DorisRangePartitioner extends Partitioner {
    private static final String UNPARTITIONED_TYPE = "UNPARTITIONED";
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
