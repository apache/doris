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

suite("test_rename_column_with_format_v2", "inverted_index_format_v2"){
    def tableName = "test_rename_column_with_format_v2"

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

    sql "DROP TABLE IF EXISTS ${tableName}"
    
    sql """
	    CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted
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

    qt_sql "SELECT * FROM $tableName WHERE name match 'andy' order by id, name, score;"

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)
    check_nested_index_file(ip, port, tablet_id, 7, 1, "V2")

    // rename column
    sql """ ALTER TABLE ${tableName} RENAME COLUMN name name_new; """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    qt_sql "SELECT * FROM $tableName WHERE name_new match 'andy' order by id, name_new, score;"

    check_nested_index_file(ip, port, tablet_id, 7, 1, "V2")

    // drop column
    sql """ ALTER TABLE ${tableName} DROP COLUMN name_new; """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    // when drop column, the index files will not be deleted, so the index files count is still 1
    // when all index columns are dropped, the index files will be deleted by GC later
    check_nested_index_file(ip, port, tablet_id, 7, 1, "V2")
}
