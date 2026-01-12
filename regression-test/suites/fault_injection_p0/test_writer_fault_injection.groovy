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

suite("test_writer_fault_injection", "nonConcurrent") {
    sql """ set enable_memtable_on_sink_node=false """
    try {
        sql """
            CREATE TABLE IF NOT EXISTS `test_writer_fault_injection_source` (
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
            CREATE TABLE IF NOT EXISTS `test_writer_fault_injection_dest` (
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
            table "test_writer_fault_injection_source"
            db "regression_test_fault_injection_p0"
            set 'column_separator', ','
            file "baseall.txt"
        }

        def load_with_injection = { injection, error_msg="", success=false->
            try {
                GetDebugPoint().enableDebugPointForAllBEs(injection)
                sql "insert into test_writer_fault_injection_dest select * from test_writer_fault_injection_source where k1 <= 3"
                assertTrue(success, String.format("expected Exception '%s', actual success", error_msg))
            } catch(Exception e) {
                logger.info(e.getMessage())
                assertTrue(e.getMessage().contains(error_msg),
                        String.format("expected '%s', actual '%s'", error_msg, e.getMessage()))
            } finally {
                sleep 1000 // wait some time for instance finish before disable injection
                GetDebugPoint().disableDebugPointForAllBEs(injection)
            }
        }

        // VTabletWriter close logic injection tests
        // Test VNodeChannel close_wait with full gc injection
        load_with_injection("VNodeChannel.close_wait_full_gc")
        // Test VNodeChannel try_send_and_fetch_status with full gc injection
        load_with_injection("VNodeChannel.try_send_and_fetch_status_full_gc")
        // Test VNodeChannel close_wait when cancelled
        load_with_injection("VNodeChannel.close_wait.cancelled")
        // Test IndexChannel close_wait with timeout
        load_with_injection("IndexChannel.close_wait.timeout")
        // Test VTabletWriter close with _close_status not ok
        load_with_injection("VTabletWriter.close.close_status_not_ok")
        // Test IndexChannel check_each_node_channel_close with close_status not ok
        load_with_injection("IndexChannel.check_each_node_channel_close.close_status_not_ok")
    } finally {
        sql """ set enable_memtable_on_sink_node=true """
    }
}
