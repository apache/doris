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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/util/Statistics.java
// and modified by Doris

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.statistics.CommonStatistics;
import org.apache.doris.datasource.statistics.CommonStatistics.ReduceOperator;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class HivePartitionStatistics {
    public static final HivePartitionStatistics EMPTY =
            new HivePartitionStatistics(CommonStatistics.EMPTY, ImmutableMap.of());

    private final CommonStatistics commonStatistics;
    private final Map<String, HiveColumnStatistics> columnStatisticsMap;

    public HivePartitionStatistics(
            CommonStatistics commonStatistics,
            Map<String, HiveColumnStatistics> columnStatisticsMap) {
        this.commonStatistics = commonStatistics;
        this.columnStatisticsMap = columnStatisticsMap;
    }

    public CommonStatistics getCommonStatistics() {
        return commonStatistics;
    }

    public Map<String, HiveColumnStatistics> getColumnStatisticsMap() {
        return columnStatisticsMap;
    }

    public static HivePartitionStatistics fromCommonStatistics(long rowCount, long fileCount, long totalFileBytes) {
        return new HivePartitionStatistics(
                new CommonStatistics(rowCount, fileCount, totalFileBytes),
                ImmutableMap.of()
        );
    }

    // only used to update the parameters of partition or table.
    public static HivePartitionStatistics merge(HivePartitionStatistics current, HivePartitionStatistics update) {
        if (current.getCommonStatistics().getRowCount() <= 0) {
            return update;
        } else if (update.getCommonStatistics().getRowCount() <= 0) {
            return current;
        }

        return new HivePartitionStatistics(
                CommonStatistics
                        .reduce(current.getCommonStatistics(), update.getCommonStatistics(), ReduceOperator.ADD),
                // TODO merge columnStatisticsMap
                current.getColumnStatisticsMap());
    }

    public static HivePartitionStatistics reduce(
            HivePartitionStatistics first,
            HivePartitionStatistics second,
            ReduceOperator operator) {
        CommonStatistics left = first.getCommonStatistics();
        CommonStatistics right = second.getCommonStatistics();
        return HivePartitionStatistics.fromCommonStatistics(
                CommonStatistics.reduce(left.getRowCount(), right.getRowCount(), operator),
                CommonStatistics.reduce(left.getFileCount(), right.getFileCount(), operator),
                CommonStatistics.reduce(left.getTotalFileBytes(), right.getTotalFileBytes(), operator));
    }

    public static CommonStatistics reduce(
            CommonStatistics current,
            CommonStatistics update,
            ReduceOperator operator) {
        return new CommonStatistics(
                CommonStatistics.reduce(current.getRowCount(), update.getRowCount(), operator),
                CommonStatistics.reduce(current.getFileCount(), update.getFileCount(), operator),
                CommonStatistics.reduce(current.getTotalFileBytes(), update.getTotalFileBytes(), operator));
    }

}
