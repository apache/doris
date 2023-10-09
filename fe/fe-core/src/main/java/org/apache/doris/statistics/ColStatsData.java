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

import org.apache.doris.statistics.util.StatisticsUtil;

import java.util.StringJoiner;

/**
 * Used to convert data from ResultRow.
 * 0: id
 * 1: catalog_id
 * 2: db_id
 * 3: tbl_id
 * 4: idx_id
 * 5: col_id
 * 6: part_id
 * 7: count
 * 8: ndv
 * 9: null_count
 * 10: min
 * 11: max
 * 12: data_size_in_bytes
 * 13: update_time
 */
public class ColStatsData {
    public final StatsId statsId;
    public final long count;
    public final long ndv;

    public final long nullCount;

    public final String minLit;
    public final String maxLit;

    public final long dataSizeInBytes;

    public final String updateTime;

    public ColStatsData(ResultRow row) {
        this.statsId = new StatsId(row);
        this.count = (long) Double.parseDouble(row.get(7));
        this.ndv = (long) Double.parseDouble(row.getWithDefault(8, "0"));
        this.nullCount = (long) Double.parseDouble(row.getWithDefault(9, "0"));
        this.minLit = row.get(10);
        this.maxLit = row.get(11);
        this.dataSizeInBytes = (long) Double.parseDouble(row.getWithDefault(12, "0"));
        this.updateTime = row.get(13);
    }

    public String toSQL(boolean roundByParentheses) {
        StringJoiner sj = null;
        if (roundByParentheses) {
            sj = new StringJoiner(",", "(" + statsId.toSQL() + ",", ")");
        } else {
            sj = new StringJoiner(",", statsId.toSQL(), "");
        }
        sj.add(String.valueOf(count));
        sj.add(String.valueOf(ndv));
        sj.add(String.valueOf(nullCount));
        sj.add(StatisticsUtil.quote(minLit));
        sj.add(StatisticsUtil.quote(maxLit));
        sj.add(String.valueOf(dataSizeInBytes));
        sj.add(StatisticsUtil.quote(updateTime));
        return sj.toString();
    }
}
