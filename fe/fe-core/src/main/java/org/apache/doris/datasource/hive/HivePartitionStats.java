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

package org.apache.doris.datasource.hive;

import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;

public class HivePartitionStats {
    private static final HivePartitionStats EMPTY = new HivePartitionStats(
            new ColumnStatisticBuilder().setCount(-1).setDataSize(-1).build());
    private final ColumnStatistic tableStats;

    public static HivePartitionStats empty() {
        return EMPTY;
    }

    public static HivePartitionStats fromTableStats(long numRows, long totalBytes) {
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder();
        builder.setCount(numRows).setDataSize(totalBytes);
        return new HivePartitionStats(builder.build());
    }

    public HivePartitionStats(ColumnStatistic tableStats) {
        this.tableStats = tableStats;
    }

    public ColumnStatistic getTableStats() {
        return tableStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HivePartitionStats(");
        sb.append("tableStats(numRows=").append(tableStats.count);
        sb.append(", totalSize=").append(tableStats.dataSize);
        sb.append("))");
        return sb.toString();
    }

    public static HivePartitionStats merge(HivePartitionStats current, HivePartitionStats update) {
        // Empty table statistics can should skip merge
        if ((long) current.getTableStats().count == -1 || (long) update.getTableStats().count <= 0) {
            return current;
        } else if ((long) current.getTableStats().count == 0 && (long) update.getTableStats().count > 0) {
            return update;
        }
        return new HivePartitionStats(reduce(current.getTableStats(), update.getTableStats(), ReduceOperator.ADD));
    }

    public static HivePartitionStats reduce(HivePartitionStats current,
                                            HivePartitionStats update, ReduceOperator operator) {
        return HivePartitionStats.fromTableStats(
            reduce((long) current.getTableStats().count, (long) update.getTableStats().count, operator),
            reduce((long) current.getTableStats().dataSize, (long) update.getTableStats().dataSize, operator));
    }

    public static ColumnStatistic reduce(ColumnStatistic current, ColumnStatistic update, ReduceOperator operator) {
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder();
        builder.setCount(reduce((long) current.count, (long) update.count, operator));
        builder.setDataSize(reduce((long) current.dataSize, (long) update.dataSize, operator));
        return builder.build();
    }

    public static long reduce(long current, long update, ReduceOperator operator) {
        if (current >= 0 && update >= 0) {
            switch (operator) {
                case ADD:
                    return current + update;
                case SUBTRACT:
                    return current - update;
                case MAX:
                    return Math.max(current, update);
                case MIN:
                    return Math.min(current, update);
                default:
                    break;
            }
            throw new IllegalArgumentException("Unexpected operator: " + operator);
        }
        return 0;
    }

    public enum ReduceOperator {
        ADD,
        SUBTRACT,
        MIN,
        MAX,
    }
}
