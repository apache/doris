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

suite("test_show_nested_index_file_http_action_with_variant", "nonConcurrent,p0") {
    def show_nested_index_file_on_tablet = { ip, port, tablet ->
        return http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet))
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->
        String backend_id;
        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    def load_json_data = {tableName, file_name ->
        // load the json data
        streamLoad {
            table "${tableName}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    set_be_config.call("memory_limitation_per_thread_for_schema_change_bytes", "6294967296")
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1")
    def run_test = { format ->
        def tableName = "test_show_nested_index_file_http_action_with_variant_" + format

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """ set disable_inverted_index_v1_for_variant = false """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k bigint,
                v variant,
                INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "${format}");
        """
        sql """ set disable_inverted_index_v1_for_variant = true """
        load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
        load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")

        // select to sync meta in cloud mode
        sql """ select * from ${tableName} limit 10; """

        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        String tablet_id = tablets[0].TabletId
        String backend_id = tablets[0].BackendId
        String ip = backendId_to_backendIP.get(backend_id)
        String port = backendId_to_backendHttpPort.get(backend_id)
        def (code, out, err) = show_nested_index_file_on_tablet(ip, port, tablet_id)
        logger.info("Run show_nested_index_file_on_tablet: code=" + code + ", err=" + err)

        assertTrue(code == 0)
        assertEquals(tablet_id, parseJson(out.trim()).tablet_id.toString())
        def rowset_count = parseJson(out.trim()).rowsets.size();
        assertEquals(3, rowset_count)
        def index_files_count = 0
        def segment_files_count = 0
        def indices_count = 0
        for (def rowset in parseJson(out.trim()).rowsets) {
            assertEquals(format, rowset.index_storage_format)
            for (int i = 0; i < rowset.segments.size(); i++) {
                def segment = rowset.segments[i]
                assertEquals(i, segment.segment_id)
                indices_count += segment.indices.size()
                if (format == "V1") {
                    index_files_count += segment.indices.size()
                } else {
                    index_files_count++
                }
            }
            segment_files_count += rowset.segments.size()
        }
        if (format == "V1") {
            assertEquals(1203, indices_count)
            assertEquals(1203, index_files_count)
            assertEquals(2, segment_files_count)
        } else {
            assertEquals(1203, indices_count)
            assertEquals(2, index_files_count)
            assertEquals(2, segment_files_count)
        }

        qt_sql """select cast(v["payload"]["pull_request"]["additions"] as int)  from ${tableName} where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1;"""
        qt_sql """select count() from ${tableName} where  cast(v["repo"]["name"] as string) = 'xpressengine/xe-core'"""
    }

    run_test("V1")
    run_test("V2")

    set_be_config.call("memory_limitation_per_thread_for_schema_change_bytes", "2147483648")
}
