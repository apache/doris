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

suite("test_stream_load_with_inverted_index", "p2") {
    def tableName = "test_stream_load_with_inverted_index"

    def set_be_config = { key, value ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
    }

    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            set 'memtable_on_sink_node', 'true'
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

    boolean disableAutoCompaction = true
    boolean has_update_be_config = false
    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))

        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }
        set_be_config.call("disable_auto_compaction", "true")
        has_update_be_config = true

        def test = { format ->
            sql """ DROP TABLE IF EXISTS ${tableName}; """
            sql """
                CREATE TABLE ${tableName} (
                    k bigint,
                    v variant,
                    INDEX idx_v (`v`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
                ) ENGINE=OLAP
                DUPLICATE KEY(`k`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES ( "replication_num" = "2", "inverted_index_storage_format" = ${format});
            """

            def tablets = sql_return_maparray """ show tablets from ${tableName}; """

            String first_backend_id;
            List<String> other_backend_id = new ArrayList<>()
        
            String tablet_id = tablets[0].TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet: " + tablet_info)
            for (def tablet in tablets) {
                first_backend_id = tablet.BackendId
                other_backend_id.add(tablet.BackendId)
            }
            other_backend_id.remove(first_backend_id)

            def checkTabletFileCrc = {
                def (first_code, first_out, first_err) = calc_file_crc_on_tablet(backendId_to_backendIP[first_backend_id], backendId_to_backendHttpPort[first_backend_id], tablet_id)
                logger.info("Run calc_file_crc_on_tablet: ip=" + backendId_to_backendIP[first_backend_id] + " code=" + first_code + ", out=" + first_out + ", err=" + first_err)

                for (String backend: other_backend_id) {
                    def (other_code, other_out, other_err) = calc_file_crc_on_tablet(backendId_to_backendIP[backend], backendId_to_backendHttpPort[backend], tablet_id)
                    logger.info("Run calc_file_crc_on_tablet: ip=" + backendId_to_backendIP[backend] + " code=" + other_code + ", out=" + other_out + ", err=" + other_err)
                    assertTrue(parseJson(first_out.trim()).crc_value == parseJson(other_out.trim()).crc_value)
                    assertTrue(parseJson(first_out.trim()).start_version == parseJson(other_out.trim()).start_version)
                    assertTrue(parseJson(first_out.trim()).end_version == parseJson(other_out.trim()).end_version)
                    assertTrue(parseJson(first_out.trim()).file_count == parseJson(other_out.trim()).file_count)
                    assertTrue(parseJson(first_out.trim()).rowset_count == parseJson(other_out.trim()).rowset_count)
                }
            }

            load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
            load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
            load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
            load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
            load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")


            // check 
            checkTabletFileCrc.call()

            qt_sql_1 """
            select cast(v["repo"]["name"] as string) from ${tableName} where cast(v["repo"]["name"] as string) match "davesbingrewardsbot";
            """

            sql """ DROP TABLE IF EXISTS ${tableName}; """
            sql """
                CREATE TABLE ${tableName} (
                    k bigint,
                    v variant,
                    INDEX idx_v (`v`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
                ) ENGINE=OLAP
                DUPLICATE KEY(`k`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES ( "replication_num" = "2", "inverted_index_storage_format" = ${format});
            """

            sql """insert into ${tableName} values(1, '{"a" : 123, "b" : "xxxyyy", "c" : 111999111}')"""
            sql """insert into ${tableName} values(2, '{"a" : 18811, "b" : "hello world", "c" : 1181111}')"""
            sql """insert into ${tableName} values(3, '{"a" : 18811, "b" : "hello wworld", "c" : 11111}')"""
            sql """insert into ${tableName} values(4, '{"a" : 1234, "b" : "hello xxx world", "c" : 8181111}')"""
            qt_sql_2 """select * from ${tableName} where cast(v["a"] as smallint) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
            sql """insert into ${tableName} values(5, '{"a" : 123456789, "b" : 123456, "c" : 8181111}')"""
            qt_sql_3 """select * from ${tableName} where cast(v["a"] as int) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 11111 order by k"""

            // check 
            checkTabletFileCrc.call()
            sql """ DROP TABLE IF EXISTS ${tableName}; """
        }
        set_be_config("inverted_index_ram_dir_enable", "true")
        // test.call("V1")
        test.call("V2")
        set_be_config("inverted_index_ram_dir_enable", "false")
        // test.call("V1")
        test.call("V2")
        set_be_config("inverted_index_ram_dir_enable", "true")
    } finally {
        if (has_update_be_config) {
            set_be_config.call("disable_auto_compaction", disableAutoCompaction.toString())
        }
    }
}