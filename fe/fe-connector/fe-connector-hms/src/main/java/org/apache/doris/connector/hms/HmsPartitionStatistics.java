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

package org.apache.doris.connector.hms;

/**
 * Statistics for a Hive partition/table write. SPI-clean port of fe-core's
 * {@code datasource.hive.HivePartitionStatistics}, reduced to the basic (common) statistics.
 *
 * <p>The legacy type also carried a {@code Map<String, HiveColumnStatistics>} column-statistics map,
 * but the INSERT/commit write path never populates it (it is always empty, and legacy {@code merge}
 * does not actually merge column stats), and {@code HiveColumnStatistics} is a fe-core type. It is
 * therefore dropped from the plugin DTO — only the row/file/byte common statistics travel.</p>
 */
public final class HmsPartitionStatistics {

    public static final HmsPartitionStatistics EMPTY =
            new HmsPartitionStatistics(HmsCommonStatistics.EMPTY);

    private final HmsCommonStatistics commonStatistics;

    public HmsPartitionStatistics(HmsCommonStatistics commonStatistics) {
        this.commonStatistics = commonStatistics;
    }

    public HmsCommonStatistics getCommonStatistics() {
        return commonStatistics;
    }

    public static HmsPartitionStatistics fromCommonStatistics(long rowCount, long fileCount,
            long totalFileBytes) {
        return new HmsPartitionStatistics(
                new HmsCommonStatistics(rowCount, fileCount, totalFileBytes));
    }

    public static HmsPartitionStatistics merge(HmsPartitionStatistics current,
            HmsPartitionStatistics update) {
        if (current.getCommonStatistics().getRowCount() <= 0) {
            return update;
        } else if (update.getCommonStatistics().getRowCount() <= 0) {
            return current;
        }
        return new HmsPartitionStatistics(HmsCommonStatistics.reduce(
                current.getCommonStatistics(), update.getCommonStatistics(),
                HmsCommonStatistics.ReduceOperator.ADD));
    }

    public static HmsPartitionStatistics reduce(HmsPartitionStatistics first,
            HmsPartitionStatistics second, HmsCommonStatistics.ReduceOperator operator) {
        HmsCommonStatistics left = first.getCommonStatistics();
        HmsCommonStatistics right = second.getCommonStatistics();
        return HmsPartitionStatistics.fromCommonStatistics(
                HmsCommonStatistics.reduce(left.getRowCount(), right.getRowCount(), operator),
                HmsCommonStatistics.reduce(left.getFileCount(), right.getFileCount(), operator),
                HmsCommonStatistics.reduce(left.getTotalFileBytes(), right.getTotalFileBytes(), operator));
    }
}
