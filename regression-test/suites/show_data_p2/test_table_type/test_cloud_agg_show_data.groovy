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
suite("test_cloud_agg_show_data","p2") {
    //cloud-mode
    if (!isCloudMode()) {
        logger.info("not cloud mode, not run")
        return
    }

    def main = {
        def tableName="test_cloud_agg_show_data"
        sql "DROP TABLE IF EXISTS ${tableName};"
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
            ( L_ORDERKEY INTEGER NOT NULL, 
            L_PARTKEY INTEGER NOT NULL, 
            L_SUPPKEY INTEGER NOT NULL, 
            L_LINENUMBER INTEGER NOT NULL,
            L_QUANTITY DECIMAL(15,2) SUM,
            L_EXTENDEDPRICE DECIMAL(15,2) SUM,
            L_DISCOUNT DECIMAL(15,2) SUM,
            L_TAX DECIMAL(15,2) SUM,
            L_RETURNFLAG CHAR(1) REPLACE,
            L_LINESTATUS CHAR(1) REPLACE,
            L_SHIPDATE DATE MAX,
            L_COMMITDATE DATE MAX,
            L_RECEIPTDATE DATE MAX,
            L_SHIPINSTRUCT CHAR(25) REPLACE,
            L_SHIPMODE CHAR(10) REPLACE,
            L_COMMENT VARCHAR(44) REPLACE,
            L_NULL VARCHAR REPLACE
            )
        AGGREGATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
        DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
        """
        List<String> tablets = get_tablets_from_table(tableName)
        def loadTimes = [1, 10]
        Map<String, List> sizeRecords = ["apiSize":[], "mysqlSize":[], "cbsSize":[]]
        for (int i in loadTimes){
            // stream load 1 time, record each size
            repeate_stream_load_same_data(tableName, i, "regression/tpch/sf0.1/lineitem.tbl.gz")
            def rows = sql_return_maparray "select count(*) as count from ${tableName};"
            logger.info("table ${tableName} has ${rows[0]["count"]} rows")
            // 加一下触发compaction的机制
            trigger_compaction(tablets)

            // 然后 sleep 1min， 等fe汇报完
            sleep(60 * 1000)
            sql "select count(*) from ${tableName}"

            sizeRecords["apiSize"].add(caculate_table_data_size_through_api(tablets))
            sizeRecords["cbsSize"].add(caculate_table_data_size_in_backend_storage(tablets))
            sizeRecords["mysqlSize"].add(show_table_data_size_through_mysql(tableName))
            sleep(60 * 1000)
            logger.info("after ${i} times stream load, mysqlSize is: ${sizeRecords["mysqlSize"][-1]}, apiSize is: ${sizeRecords["apiSize"][-1]}, storageSize is: ${sizeRecords["cbsSize"][-1]}")

        }

        // expect mysqlSize == apiSize == storageSize
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["apiSize"][0])
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["cbsSize"][0])
        // expect mysqlSize == apiSize == storageSize
        assertEquals(sizeRecords["mysqlSize"][1], sizeRecords["apiSize"][1])
        assertEquals(sizeRecords["mysqlSize"][1], sizeRecords["cbsSize"][1])
        // expect 10 * 1 times on agg table >= load 10 times on agg table >= 1 times on agg table
        assertTrue(10*sizeRecords["mysqlSize"][0]>=sizeRecords["mysqlSize"][1])
        assertTrue(sizeRecords["mysqlSize"][1]>=sizeRecords["mysqlSize"][0])
    }

    main()
}
