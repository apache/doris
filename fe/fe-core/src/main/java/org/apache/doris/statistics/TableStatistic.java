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

import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TableStatistic {

    private static final Logger LOG = LogManager.getLogger(TableStatistic.class);

    public static TableStatistic UNKNOWN = new TableStatisticBuilder()
            .setRowCount(0).setUpdateTime("NULL").setLastAnalyzeTimeInMs(0L)
            .build();

    public final long rowCount;
    public final long lastAnalyzeTimeInMs;
    public final String updateTime;

    public TableStatistic(long rowCount, long lastAnalyzeTimeInMs, String updateTime) {
        this.rowCount = rowCount;
        this.lastAnalyzeTimeInMs = lastAnalyzeTimeInMs;
        this.updateTime = updateTime;
    }

    // TODO: use thrift
    public static TableStatistic fromResultRow(ResultRow resultRow) {
        try {
            TableStatisticBuilder tableStatisticBuilder = new TableStatisticBuilder();
            long rowCount = Long.parseLong(resultRow.getColumnValue("count"));
            String updateTime = resultRow.getColumnValue("update_time");
            long lastAnalyzeTimeInMs = Long
                    .parseLong(resultRow.getColumnValue("last_analyze_time_in_ms"));
            tableStatisticBuilder.setRowCount(rowCount);
            tableStatisticBuilder.setLastAnalyzeTimeInMs(lastAnalyzeTimeInMs);
            tableStatisticBuilder.setUpdateTime(updateTime);
            return tableStatisticBuilder.build();
        } catch (DdlException e) {
            LOG.warn("Failed to deserialize table statistics", e);
            return TableStatistic.UNKNOWN;
        }
    }
}
