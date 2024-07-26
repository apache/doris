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

package org.apache.doris.datasource.statistics;

/**
 * This class provides operations related to file statistics, including object and field granularity add, min, max
 * and other merge operations
 */
public class CommonStatistics {

    public static final CommonStatistics EMPTY = new CommonStatistics(0L, 0L, 0L);

    private final long rowCount;
    private final long fileCount;
    private final long totalFileBytes;

    public CommonStatistics(long rowCount, long fileCount, long totalFileBytes) {
        this.fileCount = fileCount;
        this.rowCount = rowCount;
        this.totalFileBytes = totalFileBytes;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getFileCount() {
        return fileCount;
    }

    public long getTotalFileBytes() {
        return totalFileBytes;
    }

    public static CommonStatistics reduce(
            CommonStatistics current,
            CommonStatistics update,
            ReduceOperator operator) {
        return new CommonStatistics(
                reduce(current.getRowCount(), update.getRowCount(), operator),
                reduce(current.getFileCount(), update.getFileCount(), operator),
                reduce(current.getTotalFileBytes(), update.getTotalFileBytes(), operator));
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
                    throw new IllegalArgumentException("Unexpected operator: " + operator);
            }
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
