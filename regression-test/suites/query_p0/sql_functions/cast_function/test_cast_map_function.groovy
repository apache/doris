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

suite("test_cast_map_function", "query") {
    def tableName = "tbl_test_cast_map_function"
    // array functions only supported in vectorized engine

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` Map<char(7), int(11)> NOT NULL COMMENT "",
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    // insert into for implicate cast
    sql """ INSERT INTO ${tableName} VALUES(1, {"aa": 1, "b": 2, "1234567": 77}) """

    qt_select """ select * from ${tableName} order by k1; """

    qt_select " select cast({} as MAP<INT,INT>);"
}
