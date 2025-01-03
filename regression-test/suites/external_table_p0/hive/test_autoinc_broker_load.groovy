
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

suite("test_autoinc_broker_load", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def brokerName = getBrokerName()
        def hdfsUser = getHdfsUser()
        def hdfsPasswd = getHdfsPasswd()
        def hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        def test_dir = "user/doris/preinstalled_data/data_case/autoinc"

        def load_from_hdfs = {columns, testTable, label, testFile, format ->
            def result1= sql """ LOAD LABEL ${label} (
                                DATA INFILE("hdfs://${externalEnvIp}:${hdfs_port}/${test_dir}/${testFile}")
                                INTO TABLE ${testTable}
                                COLUMNS TERMINATED BY ","
                                (${columns})
                                ) with HDFS (
                                    "fs.defaultFS"="hdfs://${externalEnvIp}:${hdfs_port}",
                                    "hadoop.username"="${hdfsUser}")
                                    PROPERTIES  (
                                    "timeout"="1200",
                                    "max_filter_ratio"="0");"""
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
        }

        def wait_for_load_result = {checklabel, testTable ->
            def max_try_milli_secs = 10000
            while(max_try_milli_secs) {
                def result = sql "show load where label = '${checklabel}'"
                if(result[0][2] == "FINISHED") {
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

        def table = "test_autoinc_broker_load"
        sql "drop table if exists ${table}"
        sql """ CREATE TABLE IF NOT EXISTS `${table}` (
            `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
            `name` varchar(65533) NOT NULL COMMENT "用户姓名",
            `value` int(11) NOT NULL COMMENT "用户得分"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "enable_unique_key_merge_on_write" = "true") """
        
        def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
        load_from_hdfs("name, value", table, test_load_label, "auto_inc_basic.csv", "csv")
        wait_for_load_result(test_load_label, table)
        qt_sql "select * from ${table} order by id;"
        sql """ insert into ${table} values(0, "Bob", 123), (2, "Tom", 323), (4, "Carter", 523);"""
        qt_sql "select * from ${table} order by id"
        sql "drop table if exists ${table};"


        table = "test_autoinc_broker_load2"
        sql "drop table if exists ${table}"
        sql """ CREATE TABLE IF NOT EXISTS `${table}` (
            `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
            `name` varchar(65533) NOT NULL COMMENT "用户姓名",
            `value` int(11) NOT NULL COMMENT "用户得分"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "enable_unique_key_merge_on_write" = "true");"""
        test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
        load_from_hdfs("id, name, value", table, test_load_label, "auto_inc_with_null.csv", "csv")
        wait_for_load_result(test_load_label, table)
        sql "sync"
        qt_sql "select * from ${table};"
        sql """ insert into ${table} values(0, "Bob", 123), (2, "Tom", 323), (4, "Carter", 523);"""
        qt_sql "select * from ${table} order by id"
        sql "drop table if exists ${table};"
    }
}
