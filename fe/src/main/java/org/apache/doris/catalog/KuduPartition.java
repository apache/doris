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

package org.apache.doris.catalog;

import com.google.common.base.Joiner;

import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RangePartitionBound;

import java.util.List;

/*
 * describe the partition Info of a kudu table.
 * include hash partition and range partition
 */
public class KuduPartition {

    public enum KuduPartitionType {
        RANGE,
        HASH
    }

    private KuduPartitionType type;
    private List<String> partitionColNames;

    // only for hash partition
    private int bucketsNum;

    // only for range partition
    private List<KuduRange> ranges;

    public static KuduPartition createHashPartition(List<String> partColNames, int bucketsNum) {
        KuduPartition kuduPartitionInfo = new KuduPartition(KuduPartitionType.HASH,
                                                                    partColNames,
                                                                    bucketsNum,
                                                                    null);
        return kuduPartitionInfo;
    }

    public static KuduPartition createRangePartition(List<String> partColNames,
                                                     List<KuduRange> ranges) {
        KuduPartition kuduPartitionInfo = new KuduPartition(KuduPartitionType.RANGE,
                                                            partColNames,
                                                            -1,
                                                            ranges);
        return kuduPartitionInfo;
    }

    private KuduPartition(KuduPartitionType type,
                          List<String> partColNames,
                          int bucketsNum,
                          List<KuduRange> ranges) {
        this.type = type;
        this.partitionColNames = partColNames;
        this.bucketsNum = bucketsNum;
        this.ranges = ranges;
    }

    public static class KuduRange {
        private String partitionName;

        private PartialRow lower;
        private PartialRow upper;

        private RangePartitionBound lowerBound;
        private RangePartitionBound upperBound;

        public KuduRange(String partitionName,
                         PartialRow lower,
                         PartialRow upper,
                         RangePartitionBound lowerBound,
                         RangePartitionBound upperBound) {
            this.partitionName = partitionName;
            this.lower = lower;
            this.upper = upper;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("PARTITION `").append(partitionName).append("` ");
            if (lowerBound == RangePartitionBound.EXCLUSIVE_BOUND) {
                sb.append("(");
            } else {
                sb.append("[");
            }
            sb.append(lower.toString()).append(" -- ");
            sb.append(upper.toString());
            if (upperBound == RangePartitionBound.EXCLUSIVE_BOUND) {
                sb.append(")");
            } else {
                sb.append("]");
            }

            return sb.toString();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (type == KuduPartitionType.HASH) {
            sb.append("DISTRIBUTED BY HASH(");
            sb.append(Joiner.on(", ").join(partitionColNames)).append(") ");
            sb.append("BUCKETS ").append(bucketsNum);
        } else {
            sb.append("PARTITION BY RANGE(");
            sb.append(Joiner.on(", ").join(partitionColNames)).append(") (\n");
            sb.append(Joiner.on(",\n").join(ranges)).append("\n)");
        }

        return sb.toString();
    }
}
