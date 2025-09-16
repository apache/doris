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

suite("test_show_nested_index_file_http_action") {
    def show_nested_index_file_on_tablet = { ip, port, tablet ->
        return http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet))
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def run_test = { format ->
        def tableName = "test_show_nested_index_file_http_action_" + format

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
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "${format}"
            );
        """
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "bason", 99); """
        sql """ INSERT INTO ${tableName} VALUES (2, "andy", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "bason", 99); """
        sql """ INSERT INTO ${tableName} VALUES (3, "andy", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "bason", 99); """

        // select to sync meta in cloud mode
        sql """ select * from ${tableName}; """

        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        String tablet_id = tablets[0].TabletId
        String backend_id = tablets[0].BackendId
        String ip = backendId_to_backendIP.get(backend_id)
        String port = backendId_to_backendHttpPort.get(backend_id)
        def (code, out, err) = show_nested_index_file_on_tablet(ip, port, tablet_id)
        logger.info("Run show_nested_index_file_on_tablet: code=" + code + ", out=" + out + ", err=" + err)

        assertTrue(code == 0)
        assertEquals(tablet_id, parseJson(out.trim()).tablet_id.toString())
        def rowset_count = parseJson(out.trim()).rowsets.size();
        assertEquals(7, rowset_count)
        def index_files_count = 0
        def segment_files_count = 0
        for (def rowset in parseJson(out.trim()).rowsets) {
            assertEquals(format, rowset.index_storage_format)
            for (int i = 0; i < rowset.segments.size(); i++) {
                def segment = rowset.segments[i]
                assertEquals(i, segment.segment_id)
                def indices_count = segment.indices.size()
                assertEquals(2, indices_count)
                if (format == "V1") {
                    index_files_count += indices_count
                } else {
                    index_files_count++
                }
            }
            segment_files_count += rowset.segments.size()
        }
        if (format == "V1") {
            int indices_count = 2
            assertEquals(index_files_count, segment_files_count * indices_count)
        } else {
            assertEquals(index_files_count, segment_files_count)
        }
    }

    run_test("V1")
    run_test("V2")
}
