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
                test_datetime datetime(4) NULL COMMENT "",
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
    sql """ insert into ${tableName} values ("2025-10-29 10:10:10", "2025-10-29"), ("2025-10-24 01:02:03.4567", "2025-10-24"); """
    
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
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1:1:1 43" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "-1 -1:1:1xxx" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29 10:10:10", INTERVAL "1 1:1:1.1234" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -1:1:1 34" DAY_SECOND); """
    qt_sql """ select date_add("2025-10-29", INTERVAL "-1 -1:1:1xx" DAY_SECOND); """

    test {
        sql """ select date_add(test_datetime, INTERVAL '1' DAY_SECOND) result from ${tableName}; """
        exception "Invalid time format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL '1 2' DAY_SECOND) result from ${tableName}; """
        exception "Invalid time format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL '1 2:3' DAY_SECOND) result from ${tableName}; """
        exception "Invalid time format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL '1 2:3:4.5678' DAY_SECOND) result from ${tableName}; """
        exception "Invalid seconds format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL 'xx 00:00:01' DAY_SECOND) result from ${tableName}; """
        exception "Invalid days format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL '1 xx:00:01' DAY_SECOND) result from ${tableName}; """
        exception "Invalid hours format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL '1 00:xx:01' DAY_SECOND) result from ${tableName}; """
        exception "Invalid minutes format"
    }

    test {
        sql """ select date_add(test_datetime, INTERVAL '1 00:00:xx' DAY_SECOND) result from ${tableName}; """
        exception "Invalid seconds format"
    }

}
