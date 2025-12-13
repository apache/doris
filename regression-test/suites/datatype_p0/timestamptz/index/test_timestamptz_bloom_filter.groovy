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

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
suite("test_timestamptz_bloom_filter") {
    def time_zone = "+08:00"
    sql "set time_zone = '${time_zone}'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_bloom_filter`;
    """
    sql """
        CREATE TABLE `timestamptz_bloom_filter` (
          `id` INT,
          `name` VARCHAR(50),
          `ts_tz` TIMESTAMPTZ
        ) 
        PROPERTIES (
        "replication_num" = "1",
        "bloom_filter_columns" = "ts_tz"
        );
    """

    for (int i = 0; i < 1; ++i) {

    streamLoad {
        table "timestamptz_bloom_filter"
        file """test_timestamptz_bloom_filter.csv"""
        set 'column_separator', '|'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            // def json = parseJson(result)
            // assertEquals("success", json.Status.toLowerCase())
            // assertEquals(1003, json.NumberTotalRows)
            // assertEquals(1003, json.NumberLoadedRows)
            // assertEquals(0, json.NumberFilteredRows)
        }
    }
    }

    qt_bloom_filter0 """
        SELECT * FROM timestamptz_bloom_filter WHERE ts_tz = '2000-01-01 00:00:00+08:00' order by 1,2,3;
    """
    // RowsBloomFilterFiltered: 4.064K (4064)
    qt_bloom_filter1 """
    SELECT * FROM timestamptz_bloom_filter WHERE ts_tz in ('2000-01-01 00:00:00+08:00', '2000-01-01 10:00:00+08:00', '2000-01-01 00:20:00+08:00') order by 1, 2, 3, 4;
    """
}