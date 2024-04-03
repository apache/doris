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

suite("test_size_function") {
    sql """ set enable_nereids_planner = false; """
    // literal

    qt_sql "SELECT size(array_shuffle(['aaa', null, 'bbb', 'fff'])), array_shuffle(['aaa', null, 'bbb', 'fff'], 0), shuffle(['aaa', null, 'bbb', 'fff'], 0)"
    qt_sql """select size(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17")), array_shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0), shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0)"""
    qt_sql "SELECT array_size(array_shuffle(['aaa', null, 'bbb', 'fff'])), array_shuffle(['aaa', null, 'bbb', 'fff'], 0), shuffle(['aaa', null, 'bbb', 'fff'], 0)"
    qt_sql """select array_size(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17")), array_shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0), shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0)"""
    qt_sql "SELECT (cardinality(['aaa', null, 'bbb', 'fff'])), array_shuffle(['aaa', null, 'bbb', 'fff'], 0), shuffle(['aaa', null, 'bbb', 'fff'], 0)"
    qt_sql """select cardinality(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17")), array_shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0), shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0)"""

    qt_sql "SELECT size(map(1,2,2,null));"
    qt_sql "SELECT map_size(map(1,2,2,null));"

    // table
    def tableName = "test_size"
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE IF NOT EXISTS `${tableName}` (
                    `id` int(11) NULL,
                    `c_array1` array<int(11)> NULL,
                    `c_array2` array<varchar(20)> NULL,
                    `c_map` map<int(11), string> NULL
                ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
        sql """INSERT INTO ${tableName} values
            (0, [2], ['123', '124', '125'], {1: "", null: "a", 2: "b"}),
            (1, [1,2,3,4,5], ['234', '124', '125'], {}),
            (2, [1,2,10,12,10], ['345', '234', '123'], {1: "a", 2: "b", 3: "c"}),
            (3, [1,3,4,2], ['222', '444', '555'], {11: NULL, 0:"ss"});
        """
        qt_select_00 " select size(c_array1), size(c_array2), size(c_map) from ${tableName} order by id;"
}