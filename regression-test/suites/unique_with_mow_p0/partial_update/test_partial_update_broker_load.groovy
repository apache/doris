
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

suite("test_primary_key_partial_update_broker_load", "p0", "external_docker") {

    if (enableHdfs()) {
        brokerName = getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        def load_from_hdfs = {testTable, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
            def result1= sql """
                            LOAD LABEL ${label} (
                                DATA INFILE("${hdfsFilePath}")
                                INTO TABLE ${testTable}
                                FORMAT as "${format}")
                            with BROKER "${brokerName}" (
                            "username"="${hdfsUser}",
                            "password"="${hdfsPasswd}")
                            PROPERTIES  (
                            "timeout"="1200",
                            "max_filter_ratio"="0");
                            """
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
        }

        def wait_for_load_result = {checklabel, testTable ->
            max_try_milli_secs = 10000
            while(max_try_milli_secs) {
                result = sql "show load where label = '${checklabel}'"
                if(result[0][2] == "FINISHED") {
                    sql "sync"
                    qt_select "select * from ${testTable} order by k1"
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

        def tableName = "test_primary_key_partial_update_broker_load"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE ${tableName} (
                    `id` int(11) NOT NULL COMMENT "用户 ID",
                    `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                    `score` int(11) NOT NULL COMMENT "用户得分",
                    `test` int(11) NULL COMMENT "null test",
                    `dft` int(11) DEFAULT "4321")
                    UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
        """
        sql """insert into ${tableName} values(2, "bob", 2000, 223, 2),(1, "alice", 1000, 123, 1),(3,"tom",3000,3);"""
        qt_sql """ select * from ${tableName} order by id; """
        def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
        load_from_hdfs(tableName, test_load_label, "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/partial_upate/update.csv", "csv", brokerName, hdfsUser, hdfsPasswd)
        wait_for_load_result(test_load_label, tableName)
        qt_sql """select * from ${tableName} order by id;"""
    }

}
