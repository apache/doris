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

suite("test_drop_column_with_format_v2", "inverted_index_format_v2"){
    // cloud mode is light schema change, tablet meta will not be updated after alter table
    // so we can't get the latest tablet meta
    if (isCloudMode()) {
        return
    }
    def tableName = "test_drop_column_with_format_v2"

    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
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
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V2",
            "disable_auto_compaction" = "true",
            "light_schema_change" = "false"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", 99); """
    sql """ INSERT INTO ${tableName} VALUES (2, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "bason", 99); """
    sql """ INSERT INTO ${tableName} VALUES (3, "andy", 100); """
    sql """ INSERT INTO ${tableName} VALUES (3, "bason", 99); """

    qt_sql "SELECT * FROM $tableName WHERE name match 'andy' order by id, name, score;"

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)
    def (code, out, err) = calc_file_crc_on_tablet(ip, port, tablet_id)
    logger.info("Run calc_file_crc_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(code == 0)
    assertTrue(out.contains("crc_value"))
    assertTrue(out.contains("used_time_ms"))
    assertEquals("0", parseJson(out.trim()).start_version)
    assertEquals("7", parseJson(out.trim()).end_version)
    assertEquals("7", parseJson(out.trim()).rowset_count)
    assertEquals("12", parseJson(out.trim()).file_count)

    // drop column
    sql """ ALTER TABLE ${tableName} DROP COLUMN score; """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // select to sync rowset meta in cloud mode
    sql """ select * from ${tableName} limit 1; """

    tablets = sql_return_maparray """ show tablets from ${tableName}; """
    tablet_id = tablets[0].TabletId
    (code, out, err) = calc_file_crc_on_tablet(ip, port, tablet_id)
    logger.info("Run calc_file_crc_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(code == 0)
    assertTrue(out.contains("crc_value"))
    assertTrue(out.contains("used_time_ms"))
    assertEquals("0", parseJson(out.trim()).start_version)
    assertEquals("7", parseJson(out.trim()).end_version)
    assertEquals("7", parseJson(out.trim()).rowset_count)
    assertEquals("12", parseJson(out.trim()).file_count)

    sql """ ALTER TABLE ${tableName} DROP COLUMN name; """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // select to sync rowset meta in cloud mode
    sql """ select * from ${tableName} limit 1; """
    
    tablets = sql_return_maparray """ show tablets from ${tableName}; """
    tablet_id = tablets[0].TabletId
    (code, out, err) = calc_file_crc_on_tablet(ip, port, tablet_id)
    logger.info("Run calc_file_crc_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
    assertTrue(code == 0)
    assertTrue(out.contains("crc_value"))
    assertTrue(out.contains("used_time_ms"))
    assertEquals("0", parseJson(out.trim()).start_version)
    assertEquals("7", parseJson(out.trim()).end_version)
    assertEquals("7", parseJson(out.trim()).rowset_count)
    assertEquals("6", parseJson(out.trim()).file_count)
}
