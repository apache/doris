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

suite("test_calc_crc", "nonConcurrent") {
    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
    }
    def calc_file_crc_on_tablet_with_start = { ip, port, tablet, start->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s&start_version=%s", ip, port, tablet, start))
    }
    def calc_file_crc_on_tablet_with_end = { ip, port, tablet, end->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s&end_version=%s", ip, port, tablet, end))
    }
    def calc_file_crc_on_tablet_with_start_end = { ip, port, tablet, start, end->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s&start_version=%s&end_version=%s", ip, port, tablet, start, end))
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def tableName = "test_clac_crc"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", 99); """
    sql """ INSERT INTO ${tableName} VALUES (2, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "bason", 99); """
    sql """ INSERT INTO ${tableName} VALUES (3, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (3, "bason", 99); """

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)
    def (code_0, out_0, err_0) = calc_file_crc_on_tablet(ip, port, tablet_id)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_0 + ", out=" + out_0 + ", err=" + err_0)
    assertTrue(code_0 == 0)
    assertTrue(out_0.contains("crc_value"))
    assertTrue(out_0.contains("used_time_ms"))
    assertEquals("0", parseJson(out_0.trim()).start_version)
    assertEquals("7", parseJson(out_0.trim()).end_version)
    assertEquals("7", parseJson(out_0.trim()).rowset_count)
    assertEquals("18", parseJson(out_0.trim()).file_count)

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::BetaRowset::calc_local_file_crc")
        def (code_1, out_1, err_1) = calc_file_crc_on_tablet(ip, port, tablet_id)
        logger.info("Run calc_file_crc_on_tablet: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
        assertTrue(out_1.contains("fault_inject calc_local_file_crc error"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::BetaRowset::calc_local_file_crc")
    }

    def (code_2, out_2, err_2) = calc_file_crc_on_tablet_with_start(ip, port, tablet_id, 0)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_2 + ", out=" + out_2 + ", err=" + err_2)
    assertTrue(code_2 == 0)
    assertEquals("0", parseJson(out_2.trim()).start_version)
    assertEquals("7", parseJson(out_2.trim()).end_version)
    assertEquals("7", parseJson(out_2.trim()).rowset_count)
    assertEquals("18", parseJson(out_2.trim()).file_count)
    assertTrue(parseJson(out_0.trim()).crc_value == parseJson(out_2.trim()).crc_value)

    def (code_3, out_3, err_3) = calc_file_crc_on_tablet_with_end(ip, port, tablet_id, 7)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_3 + ", out=" + out_3 + ", err=" + err_3)
    assertTrue(code_3 == 0)
    assertEquals("0", parseJson(out_3.trim()).start_version)
    assertEquals("7", parseJson(out_3.trim()).end_version)
    assertEquals("7", parseJson(out_3.trim()).rowset_count)
    assertEquals("18", parseJson(out_3.trim()).file_count)
    assertTrue(parseJson(out_2.trim()).crc_value == parseJson(out_3.trim()).crc_value)

    def (code_4, out_4, err_4) = calc_file_crc_on_tablet_with_start_end(ip, port, tablet_id, 3, 6)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_4 + ", out=" + out_3 + ", err=" + err_4)
    assertTrue(out_4.contains("crc_value"))
    assertTrue(out_4.contains("used_time_ms"))
    assertEquals("3", parseJson(out_4.trim()).start_version)
    assertEquals("6", parseJson(out_4.trim()).end_version)
    assertEquals("4", parseJson(out_4.trim()).rowset_count)
    assertEquals("12", parseJson(out_4.trim()).file_count)

    def (code_5, out_5, err_5) = calc_file_crc_on_tablet_with_start_end(ip, port, tablet_id, 5, 9)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_5 + ", out=" + out_5 + ", err=" + err_5)
    assertTrue(out_5.contains("crc_value"))
    assertTrue(out_5.contains("used_time_ms"))
    assertEquals("5", parseJson(out_5.trim()).start_version)
    assertEquals("7", parseJson(out_5.trim()).end_version)
    assertEquals("3", parseJson(out_5.trim()).rowset_count)
    assertEquals("9", parseJson(out_5.trim()).file_count)

    def (code_6, out_6, err_6) = calc_file_crc_on_tablet(ip, port, 123)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_6 + ", out=" + out_6 + ", err=" + err_6)
    assertTrue(out_6.contains("Tablet not found."))

    sql "DROP TABLE IF EXISTS ${tableName}"
}
