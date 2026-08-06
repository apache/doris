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

import java.text.SimpleDateFormat

suite("test_dateadd_with_other_timeunit") {
    def tableName = "test_date_add_with_other_timeunit"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                test_datetime datetime(6) NULL COMMENT "",
                test_date date NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(test_datetime)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(test_datetime) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
            )
        """
    sql """ insert into ${tableName} values ("2025-10-29 10:10:10", "2025-10-29"), ("2025-10-24 01:02:03.123456", "2025-10-24"); """
    
    // DAY_SECOND
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -1:-1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -1:-1:-1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1:-1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1:-1:-1" DAY_SECOND); """

    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 -1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 -1:-1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 -1:-1:-1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 -1:1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 -1:-1:1" DAY_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 -1:-1:-1" DAY_SECOND); """

    qt_sql """ select date_add(test_datetime, INTERVAL "1 1:1:1" DAY_SECOND) result from ${tableName}; """
    qt_sql """ select date_add(test_datetime, INTERVAL " 1  1 : 1 : 1 " DAY_SECOND) result from ${tableName}; """

    qt_sql """ select date_add(test_date, INTERVAL "1 1:1:1" DAY_SECOND) result from ${tableName}; """
    qt_sql """ select date_add(test_date, INTERVAL " 1  1 : 1 : 1 " DAY_SECOND) result from ${tableName}; """

    qt_sql """ select date_add("2025-10-29", INTERVAL "1 1:1:1" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1 1:1:-1" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 1:1:1" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -1:1:1" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -1:-1:1" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -1:-1:-1" DAY_SECOND); """

    qt_add_day_second_1 """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1:1:1xxx" DAY_SECOND); """
    qt_add_day_second_2 """ select date_add("2025-10-29", INTERVAL "-1 -1:1:1xx" DAY_SECOND); """
    qt_add_day_second_3 """ select date_add(test_datetime, INTERVAL '1' DAY_SECOND) result from ${tableName}; """
    qt_add_day_second_4 """ select date_add(test_datetime, INTERVAL '1 2' DAY_SECOND) result from ${tableName}; """
    qt_add_day_second_5 """ select date_add(test_datetime, INTERVAL '1 2:3' DAY_SECOND) result from ${tableName}; """
    qt_add_day_second_6 """ select date_add(test_datetime, INTERVAL 'xx 00:00:01' DAY_SECOND) result from ${tableName}; """
    qt_add_day_second_7 """ select date_add(test_datetime, interval '1 xx:00:01' day_second) result from ${tableName}; """
    qt_add_day_second_8 """ select date_add(test_datetime, INTERVAL '1 00:xx:01' DAY_SECOND) result from ${tableName}; """
    qt_add_day_second_9 """ select date_add(test_datetime, INTERVAL '1 00:00:xx' DAY_SECOND) result from ${tableName}; """

    // DAY_HOUR
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 -1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1 -10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 -1" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 10" DAY_HOUR); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1 -10" DAY_HOUR); """

    qt_sql """ select date_add(test_datetime, INTERVAL "1 1" DAY_HOUR) result from ${tableName}; """
    qt_sql """ select date_add(test_datetime, INTERVAL " 1    1" DAY_HOUR) result from ${tableName}; """

    qt_sql """ select date_add(test_date, INTERVAL "-1 1" DAY_HOUR) result from ${tableName}; """
    qt_sql """ select date_add(test_date, INTERVAL " -1  -1" DAY_HOUR) result from ${tableName}; """

    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1 1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1 10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1 -10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1 1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1 -1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1 10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1 -10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -1" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 10" DAY_HOUR); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -10" DAY_HOUR); """

    qt_add_day_hour_1 """ select date_add("2025-10-29 10:10:10", INTERVAL '1' DAY_HOUR); """
    qt_add_day_hour_2 """ select date_add("2025-10-29 10:10:10", INTERVAL 'xx 10' DAY_HOUR) ; """
    qt_add_day_hour_3 """ select date_add("2025-10-29 10:10:10", INTERVAL '1 xx' DAY_HOUR) ; """
    qt_add_day_hour_4 """ select date_add(test_datetime, INTERVAL '1' DAY_HOUR) result from ${tableName}; """
    qt_add_day_hour_5 """ select date_add(test_datetime, INTERVAL 'xx 10' DAY_HOUR) result from ${tableName}; """
    qt_add_day_hour_6 """ select date_add(test_datetime, INTERVAL '1 xx' DAY_HOUR) result from ${tableName}; """

    // MINUTE_SECOND
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1:1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1:-1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1:10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "1:-10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:-1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:-10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1:1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1:-1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1:10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1:-10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1:1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1:-1" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1:10" MINUTE_SECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1:-10" MINUTE_SECOND); """

    qt_sql """ select date_add(test_datetime, INTERVAL "1:1" MINUTE_SECOND) result from ${tableName}; """
    qt_sql """ select date_add(test_datetime, INTERVAL "1:1" MINUTE_SECOND) result from ${tableName}; """

    qt_sql """ select date_add(test_date, INTERVAL "-1:1" MINUTE_SECOND) result from ${tableName}; """
    qt_sql """ select date_add(test_date, INTERVAL "-1:-1" MINUTE_SECOND) result from ${tableName}; """

    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1:1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1:-1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1:10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1:-10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:-1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1:-10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1:1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1:-1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1:10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1:-10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1:1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1:-1" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1:10" MINUTE_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1:-10" MINUTE_SECOND); """

    qt_add_minute_second_1 """ select date_add("2025-10-29 10:10:10", INTERVAL '1' MINUTE_SECOND) ; """
    qt_add_minute_second_2 """ select date_add("2025-10-29 10:10:10", INTERVAL 'xx:10' MINUTE_SECOND) ; """
    qt_add_minute_second_3 """ select date_add("2025-10-29 10:10:10", INTERVAL '1:xx' MINUTE_SECOND) ; """
    qt_add_minute_second_4 """ select date_add(test_datetime, INTERVAL '1' MINUTE_SECOND) result from ${tableName}; """
    qt_add_minute_second_5 """ select date_add(test_datetime, INTERVAL 'xx:10' MINUTE_SECOND) result from ${tableName}; """
    qt_add_minute_second_6 """ select date_add(test_datetime, INTERVAL '1:xx' MINUTE_SECOND) result from ${tableName}; """

    // SECOND_MICROSECOND
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "1.1" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "-1.1" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "1.12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "1.-12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "-1.12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "-1.-12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "-1.12345678" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29 10:10:10.123456", INTERVAL "-1.12345678" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1.1" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1.-1" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1.12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "1.-12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1.1" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1.-1" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1.12" SECOND_MICROSECOND); """
    testFoldConst """ select date_add("2025-10-29", INTERVAL "-1.-123443" SECOND_MICROSECOND); """

    qt_sql """ select date_add(test_datetime, INTERVAL "1.1" SECOND_MICROSECOND) result from ${tableName}; """
    qt_sql """ select date_add(test_datetime, INTERVAL "1.123423432" SECOND_MICROSECOND) result from ${tableName}; """

    qt_sql """ select date_add(test_date, INTERVAL "-1.1" SECOND_MICROSECOND) result from ${tableName}; """
    qt_sql """ select date_add(test_date, INTERVAL "-1.-11233213123" SECOND_MICROSECOND) result from ${tableName}; """

    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1.1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1.-1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1.13231" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1.-13231" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1.1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1.-1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1.1234567" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1.-1234567" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1.1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1.-1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1.12" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "1.-12" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1.1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1.-1" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1.1234567" SECOND_MICROSECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1.-1234567" SECOND_MICROSECOND); """

    qt_add_second_microsecond_1 """ select date_add("2025-10-29 10:10:10", INTERVAL '1' SECOND_MICROSECOND) ; """
    qt_add_second_microsecond_2 """ select date_add("2025-10-29 10:10:10", INTERVAL 'xx.10' SECOND_MICROSECOND) ; """
    qt_add_second_microsecond_3 """ select date_add("2025-10-29 10:10:10", INTERVAL '1.xx' SECOND_MICROSECOND) ; """
    qt_add_second_microsecond_4 """ select date_add(test_datetime, INTERVAL '1' SECOND_MICROSECOND) result from ${tableName}; """
    qt_add_second_microsecond_5 """ select date_add(test_datetime, INTERVAL 'xx.10' SECOND_MICROSECOND) result from ${tableName}; """
    qt_add_second_microsecond_6 """ select date_add(test_datetime, INTERVAL '1.xx' SECOND_MICROSECOND) result from ${tableName}; """

}
