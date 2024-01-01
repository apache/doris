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

import org.codehaus.groovy.runtime.IOGroovyMethods
import org.apache.doris.regression.util.Http

suite("test_load_stream_stub_close_wait_fault_injection", "nonConcurrent") {
    sql """ set enable_memtable_on_sink_node=true """
    sql """ DROP TABLE IF EXISTS `baseall` """
    sql """ DROP TABLE IF EXISTS `test` """
    sql """
        CREATE TABLE IF NOT EXISTS `baseall` (
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
        CREATE TABLE IF NOT EXISTS `test` (
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

    GetDebugPoint().clearDebugPointsForAllBEs()
    streamLoad {
        table "baseall"
        db "regression_test_fault_injection_p0"
        set 'column_separator', ','
        file "baseall.txt"
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadStreamStub::LoadStreamReplyHandler::on_closed.close_wait")
        def thread1 = new Thread({
            try {
                def res = sql "insert into test select * from baseall where k1 <= 3"
                logger.info(res.toString())
            } catch(Exception e) {
                logger.info(e.getMessage())
                assertTrue(e.getMessage().contains("Communications link failure"))
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("LoadStreamStub::LoadStreamReplyHandler::on_closed.close_wait")
            }
        })
        thread1.start()

        sleep(1000)

        def processList = sql "show processlist"
        logger.info(processList.toString())
        processList.each { item ->
            logger.info(item[1].toString())
            logger.info(item[11].toString())
            if (item[11].toString() == "insert into test select * from baseall where k1 <= 3".toString()){
                def res = sql "kill ${item[1]}"
                logger.info(res.toString())
            }
        }
    } catch(Exception e) {
        logger.info(e.getMessage())
    }

    sql """ DROP TABLE IF EXISTS `baseall` """
    sql """ DROP TABLE IF EXISTS `test` """
    sql """ set enable_memtable_on_sink_node=false """
}