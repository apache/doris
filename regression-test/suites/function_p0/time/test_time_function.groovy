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

suite("test_time_function") {
    sql """
    drop table if exists test_time_function;
    """
    sql """
    CREATE TABLE test_time_function
(
    `key` VARCHAR(255),
    `num0` DOUBLE ,
    `num1` DOUBLE ,
    `num2` DOUBLE ,
    `num3` DOUBLE ,
    `num4` DOUBLE ,
    `str0` VARCHAR(255),
    `str1` VARCHAR(255),
    `str2` VARCHAR(255),
    `str3` VARCHAR(255),
    `int0` INTEGER,
    `int1` INTEGER,
    `int2` INTEGER,
    `int3` INTEGER,
    `bool0` BOOLEAN,
    `bool1` BOOLEAN,
    `bool2` BOOLEAN,
    `bool3` BOOLEAN,
    `date0` DATE,
    `date1` DATE,
    `date2` DATE,
    `date3` DATE,
    `time0` datetime,
    `time1` VARCHAR(255),
    `datetime0` datetime,
    `datetime1` VARCHAR(255),
    `zzz` VARCHAR(255)
)  DUPLICATE KEY(`key`) DISTRIBUTED BY HASH(`key`) BUCKETS 1
    properties("replication_num" = "1");
    """

    def csvFile = """test_time_to_sec.csv"""
    streamLoad {
        table "test_time_function"
        file """${csvFile}"""
        set 'column_separator', ','
        set 'strict_mode', 'false'
        set 'max_filter_ratio', '1'
        set 'trim_double_quotes', 'true'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(17, json.NumberTotalRows)
            assertEquals(17, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }


    qt_sql """
    SELECT
    SUM(
        (
            (
                (
                    TO_DAYS(DATE_SUB(`test_time_function`.`date3`, INTERVAL 400 DAY)) - TO_DAYS(`test_time_function`.`date0`)
                ) + (
                    TIME_TO_SEC(
                        ADDDATE(
                            DATE_SUB(`test_time_function`.`date3`, INTERVAL 400 DAY),
                            INTERVAL 0 SECOND
                        )
                    ) - TIME_TO_SEC(ADDDATE(`test_time_function`.`date0`, INTERVAL 0 SECOND))
                ) / (60 * 60 * 24)
            ) + (
                (
                    TO_DAYS(DATE_ADD(`test_time_function`.`date3`, INTERVAL 500 DAY)) - TO_DAYS(`test_time_function`.`date2`)
                ) + (
                    TIME_TO_SEC(
                        ADDDATE(
                            DATE_ADD(`test_time_function`.`date3`, INTERVAL 500 DAY),
                            INTERVAL 0 SECOND
                        )
                    ) - TIME_TO_SEC(ADDDATE(`test_time_function`.`date2`, INTERVAL 0 SECOND))
                ) / (60 * 60 * 24)
            )
        )
    ) AS `TEMP(Test)(2422363430)(0)`
FROM
    `test_time_function`
    """
}   
