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

suite("test_broker_load_with_partition", "load_p0") {
    // define a sql table
    def testTable = "tbl_test_broker_load_with_partition"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                `k1` BIGINT NOT NULL,
                `k2` DATE NULL,
                `k3` INT(11) NOT NULL,
                `k4` INT(11) NOT NULL,
                `v5` BIGINT SUM NULL DEFAULT "0"
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`)
            COMMENT 'OLAP'
            PARTITION BY RANGE(`k2`)
            (
                PARTITION `p202309` VALUES LESS THAN ("2023-10-01"),
                PARTITION `p202310` VALUES LESS THAN ("2023-11-01")
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 16
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "storage_format" = "V2"
            );
            """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                        (1,2023-09-01,1,1,1)
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")

        def result3 = sql """
            ALTER TABLE ${testTable} ADD TEMPORARY PARTITION partition_t VALUES LESS THAN ("2023-11-01");
            """
        assertTrue(result3.size() == 1)
        assertTrue(result3[0].size() == 1)
        assertTrue(result3[0][0] == 0, "Alter table should update 0 rows")
    }

    def load_from_hdfs_partition = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            PARTITION(`p202309`)
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                        )
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """

        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def load_from_hdfs_tmp_partition = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            TEMPORARY PARTITION (`partition_t`)
                            COLUMNS TERMINATED BY ","
                            FORMAT as "${format}"
                        )
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """

        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def check_load_result = {checklabel, testTablex ->
        max_try_milli_secs = 10000
        while(max_try_milli_secs) {
            result = sql "show load where label = '${checklabel}'"
            log.info("result: ${result}")
            if(result[0][2] == "FINISHED") {
                //sql "sync"
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

    def check_load_tmp_partition_result = {checklabel, testTablex ->
        max_try_milli_secs = 10000
        while(max_try_milli_secs) {
            result = sql "show load where label = '${checklabel}'"
            log.info("result: ${result}")
            if(result[0][2] == "FINISHED") {
                //sql "sync"
                qt_select "select * from ${testTablex} TEMPORARY PARTITION (`partition_t`) order by k1"
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
    // the test will run these case as below.
    if (enableHdfs()) {
        brokerName = getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        def hdfs_csv_file_path = uploadToHdfs "load_p0/broker_load/broker_load_with_partition.csv"
        //def hdfs_csv_file_path = "hdfs://ip:port/testfile"

        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs_partition.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                                brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)

            def test_tmp_partition_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs_tmp_partition.call(testTable, test_tmp_partition_load_label,
                                hdfs_csv_file_path, "csv", brokerName, hdfsUser, hdfsPasswd)

            check_load_tmp_partition_result.call(test_tmp_partition_load_label, testTable)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }
}
