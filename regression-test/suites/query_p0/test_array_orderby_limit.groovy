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

suite("test_array_char_orderby", "query") {
    // define a sql table
    def testTable = "test_array_char_orderby"

    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL,
              `k2` array<array<char(50)>> NULL,
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "disable_auto_compaction" = "false"
            )
            """
    // prepare data
    sql """ INSERT INTO ${testTable} VALUES (100, [['abc']]) """
    // set topn_opt_limit_threshold = 1024 to make sure _internal_service to be request with proto request
    sql """ set topn_opt_limit_threshold = 1024 """

    explain{
        sql("select * from ${testTable} order by k1 limit 1")
        contains "TOPN"
    }

    qt_select """ select * from ${testTable} order by k1 limit 1 """
}
