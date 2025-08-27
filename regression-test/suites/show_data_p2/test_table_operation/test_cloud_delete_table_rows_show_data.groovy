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
suite("test_cloud_delete_table_rows_show_data","p2, nonConcurrent") {
    //cloud-mode
    if (!isCloudMode()) {
        logger.info("not cloud mode, not run")
        return
    }

    def create_normal_table = { String tableName ->
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

    def create_dynamic_partition_table = { String tableName ->
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
            PARTITION BY RANGE(L_ORDERKEY) 
            (
              PARTITION p1 VALUES LESS THAN (100000),                                                                                                                                                    
              PARTITION p3 VALUES LESS THAN (300000),
              PARTITION p5 VALUES LESS THAN (500000),
              PARTITION other VALUES LESS THAN (MAXVALUE)
            )
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    }

    def create_auto_partition_table = { String tableName ->
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
            UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY,
              L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS,
              L_SHIPDATE)
            AUTO PARTITION BY RANGE (date_trunc(`L_SHIPDATE`, 'year'))
            (
            )
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
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
            tablets = get_tablets_from_table(tableName)
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
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["mysqlSize"][1])
        assertEquals(sizeRecords["apiSize"][0], sizeRecords["apiSize"][1])
        assertEquals(sizeRecords["cbsSize"][0], sizeRecords["cbsSize"][1])

        sql """set delete_without_partition=true;"""
        sql """delete from ${tableName} where L_ORDERKEY >=0;"""

        // 加一下触发compaction的机制
        trigger_compaction(tablets)

        // 然后 sleep 1min， 等fe汇报完
        sleep(10 * 1000)
        sql "select count(*) from ${tableName}"
        sleep(10 * 1000)

        sizeRecords["apiSize"].add(caculate_table_data_size_through_api(tablets))
        sizeRecords["cbsSize"].add(caculate_table_data_size_in_backend_storage(tablets))
        sizeRecords["mysqlSize"].add(show_table_data_size_through_mysql(tableName))
        // expect mysqlSize == apiSize == storageSize
        logger.info("after delete, mysqlSize is: ${sizeRecords["mysqlSize"][2]}, apiSize is: ${sizeRecords["apiSize"][2]}, storageSize is: ${sizeRecords["cbsSize"][2]}")
        assertEquals(sizeRecords["mysqlSize"][2], sizeRecords["apiSize"][2])
        assertEquals(sizeRecords["mysqlSize"][2], sizeRecords["cbsSize"][2])
    }

    def main = {
        def tableName = "test_cloud_delete_table_rows_show_data"
        create_normal_table(tableName) 
        check(tableName)
        tableName = "test_cloud_delete_table_rows_dynamic_partition_show_data"
        create_dynamic_partition_table(tableName) 
        check(tableName)
        tableName = "test_cloud_delete_table_rows_auto_partition_show_data"
        create_auto_partition_table(tableName) 
        check(tableName)
    }

    set_config_before_show_data_test()
    sleep(10 * 1000)
    main()
    set_config_after_show_data_test()
    sleep(10 * 1000)
}
