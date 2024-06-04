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

suite("test_broker_load_seq", "load_p0") {
    // define a sql table
    def testTable = "tbl_test_broker"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                user_id bigint,
                date date,
                group_id bigint,
                modify_date date,
                keyword VARCHAR(128)
            ) ENGINE=OLAP
            UNIQUE KEY(user_id, date, group_id)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH (user_id) BUCKETS 32
            PROPERTIES (
                "function_column.sequence_col" = 'modify_date',
                "replication_num" = "1",
                "in_memory" = "false"
            );
            """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        log.info("result1: ${result1}")
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                        (1,'2020-02-22',1,'2020-02-21','a')
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def load_from_hdfs_norm = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
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
        log.info("result1: ${result1}")
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def check_load_result = {checklabel, testTablex ->
        max_try_milli_secs = 10000
        while(max_try_milli_secs) {
            result = sql "show load where label = '${checklabel}'"
            if(result[0][2] == "FINISHED") {
                //sql "sync"
                qt_select "select * from ${testTablex} order by user_id, date, group_id"
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
        def hdfs_csv_file_path = uploadToHdfs "load_p0/broker_load/broker_load.csv"
        //def hdfs_csv_file_path = "hdfs://ip:port/testfile"

        // case1: import csv data from hdfs with out where
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs_norm.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                    brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }
}
