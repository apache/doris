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

suite("test_disable_move_memtable", "nonConcurrent") {
    if (!isCloudMode()) {
        sql """ set enable_memtable_on_sink_node=true """
        sql """ DROP TABLE IF EXISTS `baseall` """
        sql """ DROP TABLE IF EXISTS `test` """
        sql """ DROP TABLE IF EXISTS `baseall1` """
        sql """ DROP TABLE IF EXISTS `test1` """
        sql """ sync """
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
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties(
                "light_schema_change" = "true",
                "replication_num" = "1"
            )
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
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties(
                "light_schema_change" = "true",
                "replication_num" = "1"
            )
            """
        sql """
            CREATE TABLE IF NOT EXISTS `baseall1` (
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
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties(
                "light_schema_change" = "false",
                "replication_num" = "1"
            )
            """
        sql """
            CREATE TABLE IF NOT EXISTS `test1` (
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
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties(
                "light_schema_change" = "false",
                "replication_num" = "1"
            )
        """

        GetDebugPoint().clearDebugPointsForAllBEs()
        streamLoad {
            table "baseall"
            db "regression_test_fault_injection_p0"
            set 'column_separator', ','
            file "baseall.txt"
        }
        sql """ sync """

        def insert_into_value_with_injection = { injection, tableName, error_msg->
            try {
                GetDebugPoint().enableDebugPointForAllBEs(injection)
                sql """ insert into ${tableName} values(true, 10, 1000, 1, 1, 1, 'a', 2024-01-01, 2024-01-01, 'a', 1, 1, "hello", 1) """
            } catch(Exception e) {
                logger.info(e.getMessage())
                assertTrue(e.getMessage().contains(error_msg))
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(injection)
            }
        }

        def insert_into_select_with_injection = { injection, tableName, error_msg->
            try {
                GetDebugPoint().enableDebugPointForAllBEs(injection)
                sql "insert into ${tableName} select * from baseall where k1 <= 3"
            } catch(Exception e) {
                logger.info(e.getMessage())
                assertTrue(e.getMessage().contains(error_msg))
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(injection)
            }
        }

        def stream_load_with_injection = { injection, tableName, res->
            try {
                GetDebugPoint().enableDebugPointForAllBEs(injection)
                streamLoad {
                    table tableName
                    db "regression_test_fault_injection_p0"
                    set 'column_separator', ','
                    set 'memtable_on_sink_node', 'true'
                    file "baseall.txt"

                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        log.info("res: ${result}".toString())
                        def json = parseJson(result)
                        assertEquals("${res}".toString(), json.Status.toLowerCase().toString())
                    }
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(injection)
            }
        }

        sql """ set enable_insert_strict = false """
        insert_into_value_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test", "unknown destination tuple descriptor")
        insert_into_value_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test1", "success")
        sql """ set group_commit = sync_mode """
        insert_into_value_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test", "unknown destination tuple descriptor")
        insert_into_value_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test1", "success")
        sql """ set group_commit = sync_mode """
        insert_into_value_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test", "unknown destination tuple descriptor")
        insert_into_value_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test1", "success")
        sql """ set group_commit = off_mode """
        insert_into_select_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test", "unknown destination tuple descriptor")
        insert_into_select_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "test1", "success")
        sql """ set enable_insert_strict = true """

        if (isGroupCommitMode()) {
            def ret = sql "SHOW FRONTEND CONFIG like '%stream_load_default_memtable_on_sink_node%';"
            logger.info("${ret}")
            try {
                sql "ADMIN SET FRONTEND CONFIG ('stream_load_default_memtable_on_sink_node' = 'true')"
                sql """ set enable_nereids_planner=true """
                stream_load_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "baseall", "fail")
                stream_load_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "baseall1", "fail")
            } finally {
                sql "ADMIN SET FRONTEND CONFIG ('stream_load_default_memtable_on_sink_node' = '${ret[0][1]}')"
            }
            return
        }

        stream_load_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "baseall", "fail")
        stream_load_with_injection("VTabletWriterV2._init._output_tuple_desc_null", "baseall1", "success")

        sql """ set enable_memtable_on_sink_node=false """
        sql """ DROP TABLE IF EXISTS `baseall` """
        sql """ DROP TABLE IF EXISTS `test` """
        sql """ DROP TABLE IF EXISTS `baseall1` """
        sql """ DROP TABLE IF EXISTS `test1` """
        sql """ sync """
    }
}
