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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.clearspring.analytics.util.Lists;

/**
 * There are the statistics of table.
 * The table stats are mainly used to provide input for the Optimizer's cost model.
 * <p>
 * The description of table stats are following:
 * 1. @rowCount: The row count of table.
 * 2. @dataSize: The data size of table.
 * 3. @nameToColumnStats: <@String columnName, @ColumnStats columnStats>
 *      Each column in the Table will have corresponding @ColumnStats.
 *      Those @ColumnStats are recorded in @nameToColumnStats form of MAP.
 *      This facilitates the optimizer to quickly find the corresponding
 *      @ColumnStats based on the column name.
 *
 * @rowCount: The row count of table.
 * @dataSize: The data size of table.
 * <p>
 * The granularity of the statistics is whole table.
 * For example:
 * "@rowCount = 1000" means that the row count is 1000 in the whole table.
 */
public class TableStats {

    public static final String ROW_COUNT = "row_count";
    public static final String DATA_SIZE = "data_size";

    private static final Predicate<Long> DESIRED_ROW_COUNT_PRED = (v) -> v >= -1L;
    private static final Predicate<Long> DESIRED_DATA_SIZE_PRED = (v) -> v >= -1L;

    private long rowCount = -1;
    private long dataSize = -1;
    private Map<String, ColumnStats> nameToColumnStats = Maps.newConcurrentMap();

    public void updateTableStats(Map<String, String> statsNameToValue) throws AnalysisException {
        for (Map.Entry<String, String> entry : statsNameToValue.entrySet()) {
            String statsName = entry.getKey();
            if (statsName.equalsIgnoreCase(ROW_COUNT)) {
                rowCount = Util.getLongPropertyOrDefault(entry.getValue(), rowCount,
                        DESIRED_ROW_COUNT_PRED, ROW_COUNT + " should >= -1");
            } else if (statsName.equalsIgnoreCase(DATA_SIZE)) {
                dataSize = Util.getLongPropertyOrDefault(entry.getValue(), dataSize,
                        DESIRED_DATA_SIZE_PRED, DATA_SIZE + " should >= -1");
            }
        }
    }

    public void updateColumnStats(String columnName, Type columnType, Map<String, String> statsNameToValue)
            throws AnalysisException {
        ColumnStats columnStats = nameToColumnStats.get(columnName);
        if (columnStats == null) {
            columnStats = new ColumnStats();
            nameToColumnStats.put(columnName, columnStats);
        }
        columnStats.updateStats(columnType, statsNameToValue);
    }

    public List<String> getShowInfo() {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(rowCount));
        result.add(Long.toString(dataSize));
        return result;
    }

    public Map<String, ColumnStats> getNameToColumnStats() {
        return nameToColumnStats;
    }
}
