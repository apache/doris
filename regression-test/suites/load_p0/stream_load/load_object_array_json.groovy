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

suite("load_object_array_json", "p0") {
    // define a sql table
    def testTable = "load_object_array_json"

    def create_test_table = {
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `id` bigint(20) NOT NULL COMMENT "",
                `retail_order_bill_id` bigint(20) NULL COMMENT "",
                `owner_id` int(11) NULL COMMENT "",
                `amount_tag` decimal(12, 4) NULL COMMENT "",
                `barcode` varchar(128) NULL COMMENT "",
                `status` int(11) NULL COMMENT "",
                `amount_retail` decimal(12, 4) NULL COMMENT "",
                `amount` decimal(12, 4) NULL COMMENT "",
                `qty` int(11) NULL COMMENT "",
                `timestamp` datetime NULL COMMENT "时间戳",
                `price_cost` decimal(12, 4) NULL COMMENT "",
                `is_gift` int(11) NULL COMMENT "",
                `amount_discount` decimal(12, 4) NULL COMMENT "",
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `retail_order_bill_id`)
            DISTRIBUTED BY HASH(`retail_order_bill_id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
    }

    def load_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table table_name

            // set http request header params
            set 'strip_outer_array', 'true'
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'columns', 'id,owner_id,amount_tag,barcode,retail_order_bill_id,status,amount_retail,amount,qty,timestamp,price_cost,is_gift,amount_discount'
            set 'jsonpaths', '[\"$.id\",\"$.owner_id\",\"$.amount_tag\",\"$.barcode\",\"$.retail_order_bill_id\",\"$.status\",\"$.amount_retail\",\"$.amount\",\"$.qty\",\"$.timestamp\",\"$.price_cost\",\"$.is_gift\",\"$.amount_discount\"]'
            set 'json_root', '$.data'
            set 'fuzzy_parse', 'false'
            set 'max_filter_ratio', '1'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows
                             + json.NumberFilteredRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def check_data_correct = {table_name ->
        sql "sync"
        // select the table and check whether the data is correct
        qt_select "select * from ${table_name} order by id"
    }

    // case1: import array data in json format and enable vectorized engine
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table.call()

        load_data.call(testTable, 'test_json_object_array.csv')

        check_data_correct(testTable)

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
