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

suite("load_nestedtype_with_timezone", "p0") {
    // create table with array<datetime>, array<date>, array<datetimev2> and array<datev2>
    // map<datetime, datetime>, map<date, date>, map<datetimev2, datetimev2> and map<datev2, datev2>
    // struct<f1:datetime, f2:date, f3:datetimev2, f4:datev2>

    sql """ DROP TABLE IF EXISTS test_nested_type_with_timezone; """
    sql """ CREATE TABLE IF NOT EXISTS test_nested_type_with_timezone (
                                id INT NULL,
                                array_datetime ARRAY<datetime> NULL ,
                                array_date ARRAY<date> NULL ,
                                array_datetimev2 ARRAY<datetimev2> NULL ,
                                array_datev2 ARRAY<datev2> NULL ,
                                map_datetime MAP<datetime, datetime> NULL ,
                                map_date MAP<date, date> NULL ,
                                map_datetimev2 MAP<datetimev2, datetimev2> NULL ,
                                map_datev2 MAP<datev2, datev2> NULL ,
                                struct_datetime STRUCT<f1:datetime, f2:date, f3:datetimev2, f4:datev2> NULL
                            ) ENGINE=OLAP
                             DUPLICATE KEY(`id`)
                             DISTRIBUTED BY HASH(`id`) BUCKETS 1
                             PROPERTIES (
                             "replication_allocation" = "tag.location.default: 1"
                             );
    """

    // stream_load data
    streamLoad {
        table "test_nested_type_with_timezone"
        set 'column_separator', '|'
        file "test_nested_type_with_timezone.csv"

        check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        log.info("Stream load result: ${result}".toString())
                        def json = parseJson(result)
                        assertEquals("success", json.Status.toLowerCase())
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows + json.NumberFilteredRows)
                    }
    }

    order_qt_sql "SELECT * FROM test_nested_type_with_timezone ORDER BY id"
}