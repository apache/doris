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

suite("test_wal_mem_back_pressure_fault_injection","nonConcurrent") {


    def tableName = "wal_test"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS `wal_baseall` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    sql """
        CREATE TABLE IF NOT EXISTS `wal_test` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace_if_not_null null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    streamLoad {
        table "wal_baseall"
        db "regression_test_fault_injection_p0"
        set 'column_separator', ','
        file "baseall.txt"
    }

    def enable_back_pressure = {
        try {
            def fes = sql_return_maparray "show frontends"
            def bes = sql_return_maparray "show backends"
            logger.info("frontends: ${fes}")
                def fe = fes[0]
                def be = bes[0]
                    def url = "jdbc:mysql://${fe.Host}:${fe.QueryPort}/"
                    logger.info("observer url: " + url)
                        StringBuilder sb = new StringBuilder();
                        sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}")
                        sb.append("/rest/v2/manager/node/set_config/be")
                        sb.append(" -H \"Content-Type: application/json\" -H \"Authorization: Basic cm9vdDo= \"")
                        sb.append(""" -d \"{\\"group_commit_queue_mem_limit\\": {\\"node\\": [\\"${be.Host}:${be.HttpPort}\\"],\\"value\\": \\"0\\",\\"persist\\": \\"false\\"}}\"""")
                        String command = sb.toString()
                        logger.info(command)
                        def process = command.execute()
        } finally {
        }
    }

    def disable_back_pressure = {
        try {
            def fes = sql_return_maparray "show frontends"
            def bes = sql_return_maparray "show backends"
            logger.info("frontends: ${fes}")
                def fe = fes[0]
                def be = bes[0]
                    def url = "jdbc:mysql://${fe.Host}:${fe.QueryPort}/"
                    logger.info("observer url: " + url)
                        StringBuilder sb = new StringBuilder();
                        sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}")
                        sb.append("/rest/v2/manager/node/set_config/be")
                        sb.append(" -H \"Content-Type: application/json\" -H \"Authorization: Basic cm9vdDo= \"")
                        sb.append(""" -d \"{\\"group_commit_queue_mem_limit\\": {\\"node\\": [\\"${be.Host}:${be.HttpPort}\\"],\\"value\\": \\"67108864\\",\\"persist\\": \\"false\\"}}\"""")
                        String command = sb.toString()
                        logger.info(command)
                        def process = command.execute()
        } finally {
        }
    }

    boolean finish = false
    enable_back_pressure()
    def thread1 = new Thread({
    sql """ set group_commit = async_mode; """
        try {
            sql """insert into ${tableName} select * from wal_baseall where k1 <= 3"""
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('Communications link failure'))
        } 
    disable_back_pressure()
    finish  = true
    })
    thread1.start()

    for(int i = 0;i<10;i++){
        def processList = sql "show processlist"
        logger.info(processList.toString())
        processList.each { item ->
            logger.info(item[1].toString())
            logger.info(item[11].toString())
            if (item[11].toString() == "insert into ${tableName} select * from wal_baseall where k1 <= 3".toString()){
                def res = sql "kill ${item[1]}"
                logger.info(res.toString())
            }
        }
    }

    thread1.join()

}