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

suite("test_array_with_single_row_max_than_uint16", "query") {
    // define a sql table
    sql """set enable_nereids_planner=false"""
    List<List<Object>> backends =  sql """ select * from backends(); """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def dataFilePath = context.config.dataPath + "/query_p0/"
    def testTable = "arr_dd"
    sql "DROP TABLE IF EXISTS ${testTable}"
    def outFilePath= dataFilePath
    if (backends.size() > 1) {
        // cluster mode need to make sure all be has this data
        outFilePath="/"
        def transFile01 = "${dataFilePath}/arr_max.orc"
        for (List<Object> backend : backends) {
            def be_host = backend[1]
            scpFiles("root", be_host, transFile01, outFilePath, false)
        }
    }

    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL,
              `k2` int(11) NULL,
              `c_decimal` array<decimalv3(18, 5)> NULL DEFAULT "[]"
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
    qt_sql """
            insert into ${testTable} select * from local(
                "file_path" = "${outFilePath}/arr_max.orc",
                "backend_id" = "${be_id}",
                "format" = "orc");"""

    qt_select """ select * from ${testTable} order by k1"""
}
