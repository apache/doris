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

suite("test_multiple_file_group", "load_p0") {

    def testTable = "tbl_test_broker_load"

    def hdfsUserName = "doris"
    String hdfs_port = context.config.otherConfigs.get("hdfs_port")
    def defaultFS = "hdfs://127.0.0.1:${hdfs_port}"

    def hdfs_file_path1 = "/user/doris/preinstalled_data/broker_load_test/1.csv"
    def hdfs_file_path2 = "/user/doris/preinstalled_data/broker_load_test/2.csv"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` LARGEINT NOT NULL COMMENT "",
              `k3` STRING NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES (0, 10, "doris0") """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    // used to check multiple fileGroup
    def load_from_hdfs = {testTablex, label, hdfsFilePath1, hdfsFilePath2, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath1}")
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                            (k1, k2, k3),
                            DATA INFILE("${hdfsFilePath2}")
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                            (k1, k2, k3))
                        with HDFS (
                        "fs.defaultFS" = "${defaultFS}",
                        "hadoop.username"="${hdfsUser}"
                        )
                        PROPERTIES  (
                        "timeout"="1200");
                        """
        
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def check_load_result = {checklabel, testTablex ->
        max_try_milli_secs = 10000
        while(max_try_milli_secs) {
            result = sql "show load where label = '${checklabel}'"
            if(result[0][2] == "FINISHED") {
                sql "sync"
                qt_select "select * from ${testTablex} order by k1"
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

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        brokerName =getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        // import array data by hdfs in orc format by multiple fileGroups.
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            def path1 = "${defaultFS}" + "${hdfs_file_path1}"
            def path2 = "${defaultFS}" + "${hdfs_file_path2}"
            load_from_hdfs.call(testTable, test_load_label, path1, path2, "csv",
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

}
