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

suite("load_stream_fault_injection", "nonConcurrent") {
    // init query case data
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

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string:[:]]

    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    def reset_be_param = { paramName ->
        // for eache be node, reset paramName to default
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def original_value = backendId_to_params.get(id).get(paramName)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, original_value))
            assertTrue(out.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        // for eache be node, get param value by default
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            // get the config value from be
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            // parsing
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            // get original value
            paramValue = resultList[2]
            backendId_to_params.get(id, [:]).put(paramName, paramValue)
        }
    }

    def load_with_injection = { injection, expect_errmsg, success=false->
        try {
            GetDebugPoint().enableDebugPointForAllBEs(injection)
            sql "insert into test select * from baseall where k1 <= 3"
            assertTrue(success, String.format("Expected Exception '%s', actual success", expect_errmsg))
        } catch(Exception e) {
            // assertTrue(e.getMessage().contains("Process has no memory available"))  // the msg should contain the root cause
            logger.info(e.getMessage())
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(injection)
        }
    }

    def load_with_injection2 = { injection1, injection2, error_msg, success=false->
        try {
            GetDebugPoint().enableDebugPointForAllBEs(injection1)
            GetDebugPoint().enableDebugPointForAllBEs(injection2)
            sql "insert into test select * from baseall where k1 <= 3"
            assertTrue(success, String.format("expected Exception '%s', actual success", expect_errmsg))
        } catch(Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains(error_msg))
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(injection1)
            GetDebugPoint().disableDebugPointForAllBEs(injection2)
        }
    }

    // LoadStreamWriter create file failed
    load_with_injection("LocalFileSystem.create_file_impl.open_file_failed", "")
    // LoadStreamWriter append_data meet null file writer error
    load_with_injection("LoadStreamWriter.append_data.null_file_writer", "")
    // LoadStreamWriter close_segment meet not bad segid error
    load_with_injection("LoadStreamWriter.close_segment.bad_segid", "")
    // LoadStreamWriter close_segment meet null file writer error
    load_with_injection("LoadStreamWriter.close_segment.null_file_writer", "")
    // LoadStreamWriter close_segment meet file writer failed to close error
    load_with_injection("LocalFileWriter.close.failed", "")
    // LoadStreamWriter close_writer/add_segment meet not inited error
    load_with_injection("TabletStream.init.uninited_writer", "")
    // LoadStreamWriter add_segment meet not bad segid error
    load_with_injection("LoadStreamWriter.add_segment.bad_segid", "")
    // LoadStreamWriter add_segment meet null file writer error
    load_with_injection("LoadStreamWriter.add_segment.null_file_writer", "")
    // LoadStream init failed coz LoadStreamWriter init failed
    load_with_injection("RowsetBuilder.check_tablet_version_count.too_many_version", "")
    // LoadStream add_segment meet unknown segid in request header
    load_with_injection("TabletStream.add_segment.unknown_segid", "")
    // LoadStream append_data meet unknown index id in request header
    load_with_injection("TabletStream._append_data.unknown_indexid", "")
    // LoadStream dispatch meet unknown load id
    load_with_injection("LoadStream._dispatch.unknown_loadid", "")
    // LoadStream dispatch meet unknown src id
    load_with_injection("LoadStream._dispatch.unknown_srcid", "")

    // LoadStream meets StreamRPC idle timeout
    try {
        load_with_injection2("LoadStreamStub._send_with_retry.delay_before_send", "PInternalServiceImpl.open_load_stream.set_idle_timeout", "")
    } catch(Exception e) {
        logger.info(e.getMessage())
    }
}

