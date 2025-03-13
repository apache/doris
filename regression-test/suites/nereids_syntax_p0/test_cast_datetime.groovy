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

import java.sql.Date
import java.time.LocalDateTime

suite("test_cast_datetime") {

    sql "drop table if exists casttbl"
    sql """CREATE TABLE casttbl ( 
        a int null,
        mydate date NULL,
        mydatev2 DATEV2 null,
        mydatetime datetime null,
        mydatetimev2 datetimev2 null
    ) ENGINE=OLAP
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false"
    );
    """

    sql "insert into casttbl values (1, '2000-01-01', '2000-01-01 12:12:12', '2000-01-01', '2000-01-01 12:12:12');"

    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false"

//when BE storage support 'Date < Date', we should remove this case
//currently, if we rewrite expr to CAST(mydatetime AS DATE) < date '2019-06-01', BE returns 0 tuple. 
    qt_1 "select count(1) from casttbl where CAST(CAST(mydatetime AS DATE) AS DATETIME) < date '2019-06-01';"

    sql "insert into casttbl values(2, '', '', '', ''), (3, '2020', '2020', '2020', '2020')"
    qt_2 "select * from casttbl"
    qt_3 "select a, '' = mydate, '' = mydatev2, '' = mydatetime, '' = mydatetimev2 from casttbl"

    qt_4 "select '' > date '2019-06-01'"
    qt_5 "select '' > date_sub('2019-06-01', -10)"
    qt_7 "select '' > cast('2019-06-01 00:00:00' as datetime)"

    def dates = [
        '', '2020', '08-09', 'abcd', '2020-15-20', '2020-10-32', '2021-02-29', '99999-10-10', '10-30', '10-30 10:10:10', '2020-01 00:00:00'
    ]
    for (def s : dates) {
        test {
            sql "select date_add('${s}', 10)"
            result([[null]])
        }
    }

    test {
        sql "SELECT MonthName('abcd-ef-gh')"
        result([[null]])
    }

    test {
        sql "select cast('123.123' as date)"
        result([[Date.valueOf('2012-03-12')]])
    }

    test {
        sql "select cast('123.123' as datetime)"
        result([[LocalDateTime.parse('2012-03-12T03:00:00')]])
    }

    test {
        sql "select cast('123.123.123' as datetime)"
        //check checker('2012-03-12T03:12:03')
        result([[LocalDateTime.parse('2012-03-12T03:12:03')]])
    }

    test {
        sql "SELECT DATEADD(DAY, 1, '2025年06月20日')"
        result([[LocalDateTime.parse('2025-06-21T00:00:00')]])
    }
}
