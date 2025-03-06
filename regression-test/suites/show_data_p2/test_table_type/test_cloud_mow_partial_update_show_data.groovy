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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
import org.codehaus.groovy.runtime.IOGroovyMethods

 // loading one data 10 times, expect data size not rising
suite("test_cloud_mow_partial_update_show_data","p2, nonConcurrent") {
    //cloud-mode
    if (!isCloudMode()) {
        logger.info("not cloud mode, not run")
        return
    }

    def main = {
        def tableName="test_cloud_mow_partial_update_show_data"
        sql "DROP TABLE IF EXISTS ${tableName};"
        sql """ CREATE TABLE ${tableName} (
                    id varchar(50) NOT NULL COMMENT "userid",    
                    a1 varchar(100) DEFAULT 'anone' COMMENT "",
                    a2 varchar(100) DEFAULT 'anone' COMMENT "",
                    a3 bigint(20) DEFAULT '1' COMMENT "",
                    a4 bigint(20) DEFAULT '1' COMMENT "",
                    a5 bigint(11) DEFAULT '1' COMMENT "",
                    a6 bigint(11) DEFAULT '1' COMMENT "",
                    a7 bigint(11) DEFAULT '1' COMMENT "",
                    a8 bigint(20) DEFAULT '1' COMMENT "",
                    a9 bigint(20) DEFAULT '1' COMMENT "",
                    a10 bigint(20) DEFAULT '1' COMMENT "",
                    a11 bigint(20) DEFAULT '1' COMMENT "",
                    a12 bigint(20) DEFAULT '1' COMMENT "",
                    a13 bigint(20) DEFAULT '1' COMMENT "",
                    a14 bigint(20) DEFAULT '1' COMMENT "",
                    a15 varchar(100) DEFAULT 'anone' COMMENT "",
                    a16 varchar(100) DEFAULT 'anone' COMMENT "",
                    a17 varchar(100) DEFAULT 'anone' COMMENT "",
                    a18 bigint(20) DEFAULT '1' COMMENT "",
                    a19 varchar(100) DEFAULT 'anone' COMMENT "",
                    a20 varchar(100) DEFAULT 'anone' COMMENT "",
                    a21 bigint(20) DEFAULT '1' COMMENT "",
                    a22 bigint(20) DEFAULT '1' COMMENT "",
                    a23 bigint(20) DEFAULT '1' COMMENT "",
                    a24 bigint(20) DEFAULT '1' COMMENT "",
                    a25 varchar(100) DEFAULT 'anone' COMMENT "",
                    a26 varchar(100) DEFAULT 'anone' COMMENT "",
                    a27 varchar(100) DEFAULT 'anone' COMMENT "",
                    a28 bigint(20) DEFAULT '1' COMMENT "",
                    a29 bigint(20) DEFAULT '1' COMMENT "",
                    a30 bigint(20) DEFAULT '1' COMMENT "",
                    a31 varchar(100) DEFAULT 'anone' COMMENT "",
                    a32 varchar(100) DEFAULT 'anone' COMMENT "",
                    a33 bigint(20) DEFAULT '1' COMMENT "",
                    a34 bigint(11) DEFAULT '1' COMMENT "",
                    a35 bigint(11) DEFAULT '1' COMMENT "",
                    a36 bigint(20) DEFAULT '1' COMMENT "",
                    a37 bigint(20) DEFAULT '1' COMMENT "",
                    a38 bigint(20) DEFAULT '1' COMMENT "",
                    a39 varchar(100) DEFAULT 'anone' COMMENT "",
                    a40 varchar(100) DEFAULT 'anone' COMMENT "",
                    a41 bigint(11) DEFAULT '1' COMMENT "",
                    a42 bigint(20) DEFAULT '1' COMMENT "",
                    a43 varchar(100) DEFAULT 'anone' COMMENT "",
                    a44 varchar(100) DEFAULT 'anone' COMMENT "",
                    a45 varchar(100) DEFAULT 'anone' COMMENT "",
                    a46 bigint(20) DEFAULT '1' COMMENT "",
                    a47 bigint(11) DEFAULT '1' COMMENT "",
                    a48 bigint(20) DEFAULT '1' COMMENT "",
                    a49 bigint(20) DEFAULT '1' COMMENT "",
                    a50 bigint(20) DEFAULT '1' COMMENT "",
                    a51 bigint(11) DEFAULT '1' COMMENT "",
                    a52 bigint(20) DEFAULT '1' COMMENT "",
                    a53 varchar(100) DEFAULT 'anone' COMMENT "",
                    a54 varchar(100) DEFAULT 'anone' COMMENT "",
                    a55 varchar(100) DEFAULT 'anone' COMMENT "",
                    a56 varchar(100) DEFAULT 'anone' COMMENT "",
                    a57 bigint(20) DEFAULT '1' COMMENT "",
                    a58 bigint(11) DEFAULT '1' COMMENT "",
                    a59 varchar(100) DEFAULT 'anone' COMMENT "",
                    a60 bigint(11) DEFAULT '1' COMMENT "",
                    a61 varchar(100) DEFAULT 'anone' COMMENT "",
                    a62 bigint(11) DEFAULT '1' COMMENT "",
                    a63 varchar(100) DEFAULT 'anone' COMMENT "",
                    a64 varchar(100) DEFAULT 'anone' COMMENT "",
                    a65 bigint(20) DEFAULT '1' COMMENT "",
                    a66 bigint(20) DEFAULT '1' COMMENT "",
                    a67 varchar(100) DEFAULT 'anone' COMMENT "",
                    a68 varchar(100) DEFAULT 'anone' COMMENT "",
                    a69 varchar(100) DEFAULT 'anone' COMMENT "",
                    a70 varchar(100) DEFAULT 'anone' COMMENT "",
                    a71 bigint(20) DEFAULT '1' COMMENT "",
                    a72 bigint(20) DEFAULT '1' COMMENT "",
                    a73 bigint(20) DEFAULT '1' COMMENT "",
                    a74 bigint(20) DEFAULT '1' COMMENT "",
                    a75 bigint(20) DEFAULT '1' COMMENT "",
                    a76 bigint(20) DEFAULT '1' COMMENT "",
                    a77 bigint(20) DEFAULT '1' COMMENT "",
                    a78 bigint(20) DEFAULT '1' COMMENT "",
                    a79 bigint(20) DEFAULT '1' COMMENT "",
                    a80 bigint(20) DEFAULT '1' COMMENT "",
                    a81 varchar(100) DEFAULT 'anone' COMMENT "",
                    a82 varchar(100) DEFAULT 'anone' COMMENT "",
                    a83 varchar(100) DEFAULT 'anone' COMMENT "",
                    a84 bigint(11) DEFAULT '1' COMMENT "",
                    a85 bigint(11) DEFAULT '1' COMMENT "",
                    a86 varchar(100) DEFAULT 'anone' COMMENT "",
                    a87 varchar(100) DEFAULT 'anone' COMMENT "",
                    a88 bigint(20) DEFAULT '1' COMMENT "",
                    a89 bigint(20) DEFAULT '1' COMMENT "",
                    a90 varchar(100) DEFAULT 'anone' COMMENT "",
                    a91 varchar(100) DEFAULT 'anone' COMMENT "",
                    a92 varchar(100) DEFAULT 'anone' COMMENT "",
                    a93 varchar(100) DEFAULT 'anone' COMMENT "",
                    a94 bigint(20) DEFAULT '1' COMMENT "",
                    a95 bigint(20) DEFAULT '1' COMMENT "",
                    a96 varchar(100) DEFAULT 'anone' COMMENT "",
                    a97 varchar(100) DEFAULT 'anone' COMMENT "",
                    a98 bigint(20) DEFAULT '1' COMMENT "",
                    a99 bigint(11) DEFAULT '1' COMMENT "",
                    a100 varchar(100) DEFAULT 'anone' COMMENT "",
                    a101 varchar(100) DEFAULT 'anone' COMMENT "",
                    a102 varchar(100) DEFAULT 'anone' COMMENT "",
                    a103 varchar(100) DEFAULT 'anone' COMMENT "",
                    a104 varchar(500) DEFAULT 'anone' COMMENT "",
                    a105 bigint(20) DEFAULT '1' COMMENT "",
                    a106 varchar(100) DEFAULT 'anone' COMMENT "",
                    a107 varchar(100) DEFAULT 'anone' COMMENT "",
                    a108 varchar(100) DEFAULT 'anone' COMMENT "",
                    a109 bigint(20) DEFAULT '1' COMMENT "",
                    a110 varchar(100) DEFAULT 'anone' COMMENT "",
                    a111 varchar(100) DEFAULT 'anone' COMMENT "",
                    a112 bigint(11) DEFAULT '1' COMMENT "",
                    a113 varchar(100) DEFAULT 'anone' COMMENT "",
                    a114 varchar(100) DEFAULT 'anone' COMMENT "",
                    a115 bigint(11) DEFAULT '1' COMMENT "",
                    a116 varchar(100) DEFAULT 'anone' COMMENT "",
                    a117 bigint(11) DEFAULT '1' COMMENT "",
                    a118 varchar(100) DEFAULT 'anone' COMMENT "",
                    a119 varchar(100) DEFAULT 'anone' COMMENT "",
                    a120 bigint(20) DEFAULT '1' COMMENT "",
                    a121 bigint(20) DEFAULT '1' COMMENT "",
                    a122 bigint(20) DEFAULT '1' COMMENT "",
                    a123 bigint(20) DEFAULT '1' COMMENT "",
                    a124 bigint(20) DEFAULT '1' COMMENT "",
                    a125 bigint(20) DEFAULT '1' COMMENT "",
                    a126 bigint(20) DEFAULT '1' COMMENT "",
                    a127 bigint(20) DEFAULT '1' COMMENT "",
                    a128 bigint(20) DEFAULT '1' COMMENT "",
                    a129 bigint(20) DEFAULT '1' COMMENT "",
                    a130 bigint(20) DEFAULT '1' COMMENT "",
                    a131 varchar(100) DEFAULT 'anone' COMMENT "",
                    a132 varchar(100) DEFAULT 'anone' COMMENT "",
                    a133 varchar(100) DEFAULT 'anone' COMMENT "",
                    a134 varchar(100) DEFAULT 'anone' COMMENT "",
                    a135 varchar(100) DEFAULT 'anone' COMMENT "",
                    a136 bigint(20) DEFAULT '1' COMMENT "",
                    a137 varchar(100) DEFAULT 'anone' COMMENT "",
                    a138 varchar(100) DEFAULT 'anone' COMMENT "",
                    a139 varchar(100) DEFAULT 'anone' COMMENT "",
                    a140 varchar(100) DEFAULT 'anone' COMMENT "",
                    a141 varchar(100) DEFAULT 'anone' COMMENT "",
                    a142 bigint(20) DEFAULT '1' COMMENT "",
                    a143 varchar(100) DEFAULT 'anone' COMMENT "",
                    a144 varchar(100) DEFAULT 'anone' COMMENT "",
                    a145 bigint(20) DEFAULT '1' COMMENT "",
                    a146 varchar(100) DEFAULT 'anone' COMMENT "",
                    a147 varchar(100) DEFAULT 'anone' COMMENT "",
                    a148 bigint(20) DEFAULT '1' COMMENT "",
                    a149 varchar(100) DEFAULT 'anone' COMMENT "",
                    a150 bigint(20) DEFAULT '1' COMMENT "",
                    a151 varchar(100) DEFAULT 'anone' COMMENT "",
                    a152 bigint(11) DEFAULT '1' COMMENT "",
                    a153 varchar(100) DEFAULT 'anone' COMMENT "",
                    a154 bigint(20) DEFAULT '1' COMMENT "",
                    a155 bigint(20) DEFAULT '1' COMMENT "",
                    a156 varchar(100) DEFAULT 'anone' COMMENT "",
                    a157 varchar(100) DEFAULT 'anone' COMMENT "",
                    a158 varchar(100) DEFAULT 'anone' COMMENT "",
                    a159 varchar(100) DEFAULT 'anone' COMMENT "",
                    a160 bigint(20) DEFAULT '1' COMMENT "",
                    a161 bigint(20) DEFAULT '1' COMMENT "",
                    a162 bigint(11) DEFAULT '1' COMMENT "",
                    a163 bigint(20) DEFAULT '1' COMMENT "",
                    a164 bigint(20) DEFAULT '1' COMMENT "",
                    a165 bigint(20) DEFAULT '1' COMMENT "",
                    a166 bigint(20) DEFAULT '1' COMMENT "",
                    a167 varchar(100) DEFAULT 'anone' COMMENT "",
                    a168 varchar(100) DEFAULT 'anone' COMMENT "",
                    a169 varchar(100) DEFAULT 'anone' COMMENT "",
                    a170 varchar(100) DEFAULT 'anone' COMMENT "",
                    a171 varchar(150) DEFAULT 'anone' COMMENT "",
                    a172 bigint(20) DEFAULT '1' COMMENT "",
                    a173 varchar(100) DEFAULT 'anone' COMMENT "",
                    a174 bigint(20) DEFAULT '1' COMMENT "",
                    a175 varchar(100) DEFAULT 'anone' COMMENT "",
                    a176 bigint(11) DEFAULT '1' COMMENT "",
                    a177 bigint(20) DEFAULT '1' COMMENT "",
                    a178 varchar(100) DEFAULT 'anone' COMMENT "",
                    a179 bigint(11) DEFAULT '1' COMMENT "",
                    a180 varchar(100) DEFAULT 'anone' COMMENT "",
                    a181 bigint(20) DEFAULT '1' COMMENT "",
                    a182 varchar(100) DEFAULT 'anone' COMMENT "",
                    a183 varchar(100) DEFAULT 'anone' COMMENT "",
                    a184 varchar(100) DEFAULT 'anone' COMMENT "",
                    a185 bigint(20) DEFAULT '1' COMMENT "",
                    a186 varchar(100) DEFAULT 'anone' COMMENT "",
                    a187 bigint(20) DEFAULT '1' COMMENT "",
                    a188 varchar(100) DEFAULT 'anone' COMMENT "",
                    a189 varchar(100) DEFAULT 'anone' COMMENT "",
                    a190 varchar(100) DEFAULT 'anone' COMMENT "",
                    a191 varchar(100) DEFAULT 'anone' COMMENT "",
                    a192 bigint(20) DEFAULT '1' COMMENT "",
                    a193 bigint(11) DEFAULT '1' COMMENT "",
                    a194 varchar(100) DEFAULT 'anone' COMMENT "",
                    a195 bigint(20) DEFAULT '1' COMMENT "",
                    a196 varchar(100) DEFAULT 'anone' COMMENT "",
                    a197 varchar(100) DEFAULT 'anone' COMMENT "",
                    a198 varchar(100) DEFAULT 'anone' COMMENT "",
                    a199 bigint(20) DEFAULT '1' COMMENT "",
                    a200 varchar(100) DEFAULT 'anone' COMMENT ""
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`id`) BUCKETS 3
                PROPERTIES (
                "store_row_column" = "true"
                );
                """
        List<String> tablets = get_tablets_from_table(tableName)
        def loadTimes = [1, 10]
        Map<String, List> sizeRecords = ["apiSize":[], "mysqlSize":[], "cbsSize":[]]
        for (int i in loadTimes){
            // stream load 1 time, record each size
            repeate_stream_load_same_data(tableName, i, "regression/show_data/fullData.1.gz")
            def rows = sql_return_maparray "select count(*) as count from ${tableName};"
            logger.info("table ${tableName} has ${rows[0]["count"]} rows")
            // 加一下触发compaction的机制
            trigger_compaction(tablets)

            stream_load_partial_update_data(tableName)
            rows = sql_return_maparray "select count(*) as count from ${tableName};"
            logger.info("table ${tableName} has ${rows[0]["count"]} rows")
            trigger_compaction(tablets)

            // 然后 sleep 1min， 等fe汇报完
            sleep(10 * 1000)
            sql "select count(*) from ${tableName}"
            sleep(10 * 1000)

            sizeRecords["apiSize"].add(caculate_table_data_size_through_api(tablets))
            sizeRecords["cbsSize"].add(caculate_table_data_size_in_backend_storage(tablets))
            sizeRecords["mysqlSize"].add(show_table_data_size_through_mysql(tableName))
            logger.info("after ${i} times stream load, mysqlSize is: ${sizeRecords["mysqlSize"][-1]}, apiSize is: ${sizeRecords["apiSize"][-1]}, storageSize is: ${sizeRecords["cbsSize"][-1]}")
        }

        // expect mysqlSize == apiSize == storageSize
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["apiSize"][0])
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["cbsSize"][0])
        // expect load 1 times ==  load 10 times
        logger.info("after 1 time stream load, size is ${sizeRecords["mysqlSize"][0]}, after 10 times stream load, size is ${sizeRecords["mysqlSize"][1]}")
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["mysqlSize"][1])
        assertEquals(sizeRecords["apiSize"][0], sizeRecords["apiSize"][1])
        assertEquals(sizeRecords["cbsSize"][0], sizeRecords["cbsSize"][1])
    }

    set_config_before_show_data_test()
    sleep(10 * 1000)
    main()
    set_config_after_show_data_test()
    sleep(10 * 1000)
}
