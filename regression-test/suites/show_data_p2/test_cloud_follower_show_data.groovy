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
suite("test_cloud_follower_show_data","p2, nonConcurrent") {
    //cloud-mode
    if (!isCloudMode()) {
        logger.info("not cloud mode, not run")
        return
    }

    def create_table = { String tableName ->
        sql "DROP TABLE IF EXISTS ${tableName};"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName}(
              L_ORDERKEY    INTEGER NOT NULL,
              L_PARTKEY     INTEGER NOT NULL,
              L_SUPPKEY     INTEGER NOT NULL,
              L_LINENUMBER  INTEGER NOT NULL,
              L_QUANTITY    DECIMAL(15,2) NOT NULL,
              L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
              L_DISCOUNT    DECIMAL(15,2) NOT NULL,
              L_TAX         DECIMAL(15,2) NOT NULL,
              L_RETURNFLAG  CHAR(1) NOT NULL,
              L_LINESTATUS  CHAR(1) NOT NULL,
              L_SHIPDATE    DATE NOT NULL,
              L_COMMITDATE  DATE NOT NULL,
              L_RECEIPTDATE DATE NOT NULL,
              L_SHIPINSTRUCT CHAR(25) NOT NULL,
              L_SHIPMODE     CHAR(10) NOT NULL,
              L_COMMENT      VARCHAR(44) NOT NULL,
              L_NULL         VARCHAR
            )
            UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    }


    def check = {String tableName -> 
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

        sleep(10 * 1000)
        def result = sql """show frontends;"""
        logger.info("result:" + result)
        for (int i = 0; i < result.size(); i++) {
            if (result[i][8] == "false" && result[i][11] == "true") {
                def follower_ip = result[i][1]
                //jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round
                def old_url = context.config.jdbcUrl
                def new_jdbc_url = old_url.replaceAll("://[^:]+:", "://${follower_ip}:")
                logger.info("new_jdbc_url: " + new_jdbc_url)

                connect('root', '', new_jdbc_url) {
                    sql "select count(*) from regression_test_show_data_p2.${tableName}"
                    sql "use regression_test_show_data_p2"

                    sizeRecords["mysqlSize"].add(show_table_data_size_through_mysql(tableName))
                }
            }
        }

        for (int i = 0; i < sizeRecords["mysqlSize"].size(); i++) {
            if (i > 0) {
                assertEquals(sizeRecords["mysqlSize"][i], sizeRecords["mysqlSize"][i-1])
            }
        }
    }

    def main = {
        def tableName="test_cloud_follower_show_data"
        create_table(tableName)
        check(tableName)
    }

    set_config_before_show_data_test()
    sleep(10 * 1000)
    main()
    set_config_after_show_data_test()
    sleep(10 * 1000)
}
