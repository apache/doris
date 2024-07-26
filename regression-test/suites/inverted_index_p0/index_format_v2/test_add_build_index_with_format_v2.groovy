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

suite("test_add_build_index_with_format_v2", "inverted_index_format_v2"){
    def tableName = "test_add_build_index_with_format_v2"

    def check_nested_index_file = { ip, port, tablet_id, expected_rowsets_count, expected_indices_count, format -> 
        def (code, out, err) = http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet_id))
        logger.info("Run show_nested_index_file_on_tablet: code=" + code + ", out=" + out + ", err=" + err)
        if (code == 500) {
            assertEquals("E-6003", parseJson(out.trim()).status)
            assertTrue(parseJson(out.trim()).msg.contains("not found"))
            return
        }
        assertTrue(code == 0)
        assertEquals(tablet_id, parseJson(out.trim()).tablet_id.toString())
        def rowsets_count = parseJson(out.trim()).rowsets.size();
        assertEquals(expected_rowsets_count, rowsets_count)
        def index_files_count = 0
        def segment_files_count = 0
        for (def rowset in parseJson(out.trim()).rowsets) {
            assertEquals(format, rowset.index_storage_format)
            for (int i = 0; i < rowset.segments.size(); i++) {
                def segment = rowset.segments[i]
                assertEquals(i, segment.segment_id)
                def indices_count = segment.indices.size()
                assertEquals(expected_indices_count, indices_count)
                if (format == "V1") {
                    index_files_count += indices_count
                } else {
                    index_files_count++
                }
            }
            segment_files_count += rowset.segments.size()
        }
        if (format == "V1") {
            assertEquals(index_files_count, segment_files_count * expected_indices_count)
        } else {
            assertEquals(index_files_count, segment_files_count)
        }
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
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
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

    // add index
    sql """
        ALTER TABLE ${tableName}
        ADD INDEX idx_name (name) using inverted;
    """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    
    sql """
        ALTER TABLE ${tableName}
        ADD INDEX idx_score (score) using inverted;
    """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // show index after add index
    def show_result = sql_return_maparray "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result[0].Key_name, "idx_name")
    assertEquals(show_result[1].Key_name, "idx_score")

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)

    // cloud mode is directly schema change, local mode is light schema change.
    // cloud mode is 12, local mode is 6
    if (isCloudMode()) {
        check_nested_index_file(ip, port, tablet_id, 7, 2, "V2")
        qt_sql "SELECT * FROM $tableName WHERE name match 'andy' order by id, name, score;"
        return
    } else {
        check_nested_index_file(ip, port, tablet_id, 7, 0, "V2")
    }

    // build index 
    sql """
        BUILD INDEX idx_name ON ${tableName};
    """
    wait_for_build_index_on_partition_finish(tableName, timeout)

    check_nested_index_file(ip, port, tablet_id, 7, 1, "V2")

    // build index 
    sql """
        BUILD INDEX idx_score ON ${tableName};
    """
    wait_for_build_index_on_partition_finish(tableName, timeout)

    check_nested_index_file(ip, port, tablet_id, 7, 2, "V2")

    qt_sql "SELECT * FROM $tableName WHERE name match 'andy' order by id, name, score;"
}
