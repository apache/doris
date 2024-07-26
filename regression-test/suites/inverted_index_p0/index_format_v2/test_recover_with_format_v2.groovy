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

suite("test_recover_with_format_v2", "inverted_index_format_v2"){
    def tableName = "test_recover_with_format_v2"

    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def check_index_file = { ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        for (int i = 0; i < tablets.size(); i++) {
            String tablet_id = tablets[i].TabletId
            String backend_id = tablets[i].BackendId
            String ip = backendId_to_backendIP.get(backend_id)
            String port = backendId_to_backendHttpPort.get(backend_id)
            def (code, out, err) = calc_file_crc_on_tablet(ip, port, tablet_id)
            logger.info("Run calc_file_crc_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(code == 0)
            assertTrue(out.contains("crc_value"))
            assertTrue(out.contains("used_time_ms"))
            assertEquals("0", parseJson(out.trim()).start_version)
            assertEquals("3", parseJson(out.trim()).end_version)
            assertEquals("3", parseJson(out.trim()).rowset_count)
            assertEquals("4", parseJson(out.trim()).file_count)
        }
    }

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
        PARTITION BY LIST(`id`)
        (
            PARTITION p1 VALUES IN ("1"),
            PARTITION p2 VALUES IN ("2"),
            PARTITION p3 VALUES IN ("3")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V2",
            "disable_auto_compaction" = "true"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", 99); """
    sql """ INSERT INTO ${tableName} VALUES (2, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "bason", 99); """
    sql """ INSERT INTO ${tableName} VALUES (3, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (3, "bason", 99); """

    check_index_file()

    // drop table and recover
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "RECOVER TABLE ${tableName}"
    check_index_file()

    // drop partition and recover
    sql "ALTER TABLE ${tableName} DROP PARTITION p1"
    sql "RECOVER PARTITION p1 from ${tableName}"
    check_index_file()
}
