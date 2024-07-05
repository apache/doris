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

suite("test_spark_load_with_index_p0", "p0") {

    def set_be_config = { key, value ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def test = { format ->
        // Need spark cluster, upload data file to hdfs
        def testTable = "tbl_test_spark_load"
        def testTable2 = "tbl_test_spark_load2"
        def testResource = "spark_resource"
        def yarnAddress = "master:8032"
        def hdfsAddress = "hdfs://master:9000"
        def hdfsWorkingDir = "hdfs://master:9000/doris"
        brokerName =getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        
        def create_test_table = {testTablex ->
            def result1 = sql """
                CREATE TABLE IF NOT EXISTS ${testTablex} (
                    c_int int(11) NULL,
                    c_char char(15) NULL,
                    c_varchar varchar(100) NULL,
                    c_bool boolean NULL,
                    c_tinyint tinyint(4) NULL,
                    c_smallint smallint(6) NULL,
                    c_bigint bigint(20) NULL,
                    c_largeint largeint(40) NULL,
                    c_float float NULL,
                    c_double double NULL,
                    c_decimal decimal(6, 3) NULL,
                    c_decimalv3 decimal(6, 3) NULL,
                    c_date date NULL,
                    c_datev2 date NULL,
                    c_datetime datetime NULL,
                    c_datetimev2 datetime NULL,
                    INDEX idx_c_varchar(c_varchar) USING INVERTED,
                    INDEX idx_c_datetime(c_datetime) USING INVERTED
                )
                DISTRIBUTED BY HASH(c_int) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = ${format}
                )
                """
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        }

        def create_spark_resource = {sparkType, sparkMaster, sparkQueue ->
            def result1 = sql """
                CREATE EXTERNAL RESOURCE "${testResource}"
                PROPERTIES
                (
                    "type" = "spark",
                    "spark.master" = "yarn",
                    "spark.submit.deployMode" = "cluster",
                    "spark.executor.memory" = "1g",
                    "spark.yarn.queue" = "default",
                    "spark.hadoop.yarn.resourcemanager.address" = "${yarnAddress}",
                    "spark.hadoop.fs.defaultFS" = "${hdfsAddress}",
                    "working_dir" = "${hdfsWorkingDir}",
                    "broker" = "${brokerName}",
                    "broker.username" = "${hdfsUser}",
                    "broker.password" = "${hdfsPasswd}"
                );
                """
            
            // DDL/DML return 1 row and 3 column, the only value is update row count
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Create resource should update 0 rows")
        }

        def load_from_hdfs_use_spark = {testTablex, testTablex2, label, hdfsFilePath1, hdfsFilePath2 ->
            def result1= sql """
                            LOAD LABEL ${label}
                            (
                                DATA INFILE("${hdfsFilePath1}")
                                INTO TABLE ${testTablex}
                                COLUMNS TERMINATED BY ",",
                                DATA INFILE("${hdfsFilePath2}")
                                INTO TABLE ${testTablex2}
                                COLUMNS TERMINATED BY "|"
                            )
                            WITH RESOURCE '${testResource}'
                            (
                                "spark.executor.memory" = "2g",
                                "spark.shuffle.compress" = "true"
                            )
                            PROPERTIES
                            (
                                "timeout" = "3600"
                            );
                            """
            
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
        }
        
        def check_load_result = {checklabel, testTablex, testTablex2 ->
            max_try_milli_secs = 10000
            while(max_try_milli_secs) {
                result = sql "show load where label = '${checklabel}'"
                if(result[0][2] == "FINISHED") {
                    sql "sync"
                    qt_select "select * from ${testTablex} order by c_int"
                    qt_select "select * from ${testTablex2} order by c_int"
                    break
                } else {
                    sleep(1000) // wait 1 second every time
                    max_try_milli_secs -= 1000
                    if(max_try_milli_secs <= 0) {
                        assertEquals(1, 2)
                    }
                }
            }
        }

        // if 'enableHdfs' in regression-conf.groovy has been set to true,
        if (enableHdfs()) {
            def hdfs_txt_file_path1 = uploadToHdfs "load_p0/spark_load/all_types1.txt"
            def hdfs_txt_file_path2 = uploadToHdfs "load_p0/spark_load/all_types2.txt"
            try {
                sql "DROP TABLE IF EXISTS ${testTable}"
                sql "DROP TABLE IF EXISTS ${testTable2}"
                create_test_table.call(testTable)
                create_test_table.call(testTable2)
                def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
                load_from_hdfs.call(testTable, testTable2, test_load_label, hdfs_txt_file_path1, hdfs_txt_file_path2)
                check_load_result.call(test_load_label, testTable, testTable2)

            } finally {
                try_sql("DROP TABLE IF EXISTS ${testTable}")
                try_sql("DROP TABLE IF EXISTS ${testTable2}")
            }
        }
    }
    
    set_be_config("inverted_index_ram_dir_enable", "true")
    test.call("V1")
    test.call("V2")
    set_be_config("inverted_index_ram_dir_enable", "false")
    test.call("V1")
    test.call("V2")
    set_be_config("inverted_index_ram_dir_enable", "true")
}
