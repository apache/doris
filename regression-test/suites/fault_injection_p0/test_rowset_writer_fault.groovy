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

suite("test_rowset_writer_fault", "nonConcurrent") {
    sql """ DROP TABLE IF EXISTS `baseall` """
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
            `k8` double null comment "",
            `k9` float null comment "",
            `k12` string null comment "",
            `k13` largeint(40) null comment ""
        ) engine=olap
        UNIQUE KEY (k0)
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1")
        """
        
    GetDebugPoint().clearDebugPointsForAllBEs()
    def injection = "BaseBetaRowsetWriter::_build_tmp.create_rowset_failed"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(injection)
        streamLoad {
            table "baseall"
            db "regression_test_fault_injection_p0"
            set 'column_separator', ','
            file "baseall.txt"
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
            }
        }
    } catch(Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains(error_msg))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(injection)
    }
    sql """ DROP TABLE IF EXISTS `baseall` """
}