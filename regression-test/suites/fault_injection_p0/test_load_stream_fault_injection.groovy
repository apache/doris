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

    def load_with_injection = { injection, expect_errmsg ->
        try {
            GetDebugPoint().enableDebugPointForAllBEs(injection)
            sql "insert into test select * from baseall where k1 <= 3"
        } catch(Exception e) {
            // assertTrue(e.getMessage().contains("Process has no memory available"))  // the msg should contain the root cause
            logger.info(e.getMessage())
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(injection)
        }
    }

    // LoadStreamWriter create file failed
    load_with_injection("LocalFileSystem.create_file_impl.open_file_failed", "")
    // LoadStreamWriter append_data meet null file writer error
    load_with_injection("LoadStreamWriter.append_data.null_file_writer", "")
    // LoadStreamWriter append_data meet bytes_appended and real file size not match error
    load_with_injection("FileWriter.bytes_appended.zero_bytes_appended", "")
    // LoadStreamWriter close_segment meet not inited error
    load_with_injection("LoadStreamWriter.close_segment.uninited_writer", "")
    // LoadStreamWriter close_segment meet not bad segid error
    load_with_injection("LoadStreamWriter.close_segment.bad_segid", "")
    // LoadStreamWriter close_segment meet null file writer error
    load_with_injection("LoadStreamWriter.close_segment.null_file_writer", "")
    // LoadStreamWriter close_segment meet file writer failed to close error
    load_with_injection("LocalFileWriter.close.failed", "")
    // LoadStreamWriter close_segment meet bytes_appended and real file size not match error
    load_with_injection("FileWriter.close_segment.zero_bytes_appended", "")
    // LoadStreamWriter add_segment meet not inited error
    load_with_injection("LoadStreamWriter.add_segment.uninited_writer", "")
    // LoadStreamWriter add_segment meet not bad segid error
    load_with_injection("LoadStreamWriter.add_segment.bad_segid", "")
    // LoadStreamWriter add_segment meet null file writer error
    load_with_injection("LoadStreamWriter.add_segment.null_file_writer", "")
    // LoadStreamWriter add_segment meet bytes_appended and real file size not match error
    load_with_injection("FileWriter.add_segment.zero_bytes_appended", "")
    // LoadStreamWriter close meet not inited error
    load_with_injection("LoadStreamWriter.close.uninited_writer", "")
    // LoadStream init failed coz LoadStreamWriter init failed
    load_with_injection("RowsetBuilder.check_tablet_version_count.too_many_version", "")
    // LoadStream add_segment meet unknown segid in request header
    load_with_injection("TabletStream.add_segment.unknown_segid", "")
    // LoadStream append_data meet unknown index id in request header
    load_with_injection("abletStream.add_segment.unknown_indexid", "")
    // LoadStream dispatch meet unknown load id
    load_with_injection("LoadStream._dispatch.unknown_loadid", "")
    // LoadStream dispatch meet unknown src id
    load_with_injection("LoadStream._dispatch.unknown_srcid", "")
}

