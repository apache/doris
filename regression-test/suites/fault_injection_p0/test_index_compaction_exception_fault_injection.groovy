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

suite("test_index_compaction_exception_fault_injection", "nonConcurrent") {
    def isCloudMode = isCloudMode()
    def tableName = "test_index_compaction_exception_fault_injection_dups"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def changed_variables = sql "show variables where Changed = 1"
    logger.info("changed variables: " + changed_variables.toString())
    // sql "UNSET GLOBAL VARIABLE ALL;"
    sql "SET global enable_match_without_inverted_index = false"

    boolean disableAutoCompaction = false
  
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def trigger_full_compaction_on_tablets = { tablets ->
        for (def tablet : tablets) {
            String tablet_id = tablet.TabletId
            String backend_id = tablet.BackendId
            int times = 1

            String compactionStatus;
            do{
                def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(2000)
                compactionStatus = parseJson(out.trim()).status.toLowerCase();
            } while (compactionStatus!="success" && times<=10 && compactionStatus!="e-6010")


            if (compactionStatus == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction && compactionStatus!="e-6010") {
                assertEquals("success", compactionStatus)
            }
        }
    }

    def wait_full_compaction_done = { tablets ->
        for (def tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet.TabletId
                String backend_id = tablet.BackendId
                def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }

    def get_rowset_count = { tablets ->
        int rowsetCount = 0
        for (def tablet in tablets) {
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        return rowsetCount
    }

    def check_config = { String key, String value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
            logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            assert configList instanceof List
            for (Object ele in (List) configList) {
                assert ele instanceof List<String>
                if (((List<String>) ele)[0] == key) {
                    assertEquals(value, ((List<String>) ele)[2])
                }
            }
        }
    }

    def insert_data = { -> 
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", 10, [89, 80, 98], ["football", "basketball"], "andy is good at sports", ["andy has a good heart", "andy is so nice"]); """
        sql """ INSERT INTO ${tableName} VALUES (1, "bason", 11, [79, 85, 97], ["singing", "dancing"], "bason is good at singing", ["bason is very clever", "bason is very healthy"]); """
        sql """ INSERT INTO ${tableName} VALUES (2, "andy", 10, [89, 80, 98], ["football", "basketball"], "andy is good at sports", ["andy has a good heart", "andy is so nice"]); """
        sql """ INSERT INTO ${tableName} VALUES (2, "bason", 11, [79, 85, 97], ["singing", "dancing"], "bason is good at singing", ["bason is very clever", "bason is very healthy"]); """
        sql """ INSERT INTO ${tableName} VALUES (3, "andy", 10, [89, 80, 98], ["football", "basketball"], "andy is good at sports", ["andy has a good heart", "andy is so nice"]); """
        sql """ INSERT INTO ${tableName} VALUES (3, "bason", 11, [79, 85, 97], ["singing", "dancing"], "bason is good at singing", ["bason is very clever", "bason is very healthy"]); """
    }

    def run_sql = { -> 
        def result = sql_return_maparray "SELECT * FROM ${tableName} WHERE name MATCH 'bason'"
        assertEquals(3, result.size())
        assertEquals(1, result[0]['id'])
        assertEquals("bason", result[0]['name'])
        assertEquals(2, result[1]['id'])
        assertEquals("bason", result[1]['name'])
        assertEquals(3, result[2]['id'])
        assertEquals("bason", result[2]['name'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE age = 11"
        assertEquals(3, result.size())
        assertEquals(1, result[0]['id'])
        assertEquals("bason", result[0]['name'])
        assertEquals(2, result[1]['id'])
        assertEquals("bason", result[1]['name'])
        assertEquals(3, result[2]['id'])
        assertEquals("bason", result[2]['name'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE description MATCH 'singing'"
        assertEquals(3, result.size())
        assertEquals("bason", result[0]['name'])
        assertEquals("bason is good at singing", result[0]['description'])
        assertEquals("bason", result[1]['name'])
        assertEquals("bason is good at singing", result[1]['description'])
        assertEquals("bason", result[2]['name'])
        assertEquals("bason is good at singing", result[2]['description'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(scores, 79)"
        assertEquals(3, result.size())
        assertEquals("bason", result[0]['name'])
        assertEquals("[79, 85, 97]", result[0]['scores'])
        assertEquals("bason", result[1]['name'])
        assertEquals("[79, 85, 97]", result[1]['scores'])
        assertEquals("bason", result[2]['name'])
        assertEquals("[79, 85, 97]", result[2]['scores'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(hobbies, 'dancing')"
        assertEquals(3, result.size())
        assertEquals("bason", result[0]['name'])
        assertEquals('["singing", "dancing"]', result[0]['hobbies'])
        assertEquals("bason", result[1]['name'])
        assertEquals('["singing", "dancing"]', result[1]['hobbies'])
        assertEquals("bason", result[2]['name'])
        assertEquals('["singing", "dancing"]', result[2]['hobbies'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(evaluation, 'bason is very clever')"
        assertEquals(3, result.size())
        assertEquals("bason", result[0]['name'])
        assertEquals('["bason is very clever", "bason is very healthy"]', result[0]['evaluation'])
        assertEquals("bason", result[1]['name'])
        assertEquals('["bason is very clever", "bason is very healthy"]', result[1]['evaluation'])
        assertEquals("bason", result[2]['name'])
        assertEquals('["bason is very clever", "bason is very healthy"]', result[2]['evaluation'])
    }

    // define debug points array
    def debug_points_abnormal_compaction = [
        "compact_column_getDirectory_error",
        "compact_column_create_index_writer_error",
        "compact_column_indexCompaction_error",
        "compact_column_index_writer_close_error",
        "compact_column_src_index_dirs_close_error",
        "Compaction::do_inverted_index_compaction_find_rowset_error",
        "Compaction::do_inverted_index_compaction_get_fs_error",
        "Compaction::do_inverted_index_compaction_index_file_reader_init_error",
        // "Compaction::do_inverted_index_compaction_file_size_status_not_ok", // v2 do not do index compaction
        "Compaction::do_inverted_index_compaction_can_not_find_index_meta",
        "Compaction::do_inverted_index_compaction_index_properties_different",
        "Compaction::do_inverted_index_compaction_index_file_writer_close_not_ok",
        "Compaction::construct_skip_inverted_index_index_reader_close_error"
    ]

    def debug_points_normal_compaction = [
        "compact_column_local_tmp_dir_delete_error",
        // "Compaction::do_inverted_index_compaction_dest_segment_num_is_zero", // query result not match without inverted index
        "Compaction::do_inverted_index_compaction_index_file_reader_init_not_found",
        "Compaction::construct_skip_inverted_index_is_skip_index_compaction",
        "Compaction::construct_skip_inverted_index_get_fs_error",
        "Compaction::construct_skip_inverted_index_index_meta_nullptr",
        "Compaction::construct_skip_inverted_index_seg_path_nullptr",
        "Compaction::construct_skip_inverted_index_index_file_reader_init_status_not_ok",
        "Compaction::construct_skip_inverted_index_index_file_reader_exist_status_not_ok",
        "Compaction::construct_skip_inverted_index_index_file_reader_exist_false",
        "Compaction::construct_skip_inverted_index_index_file_reader_open_error",
        "Compaction::construct_skip_inverted_index_index_files_count"
    ]

    def run_test = { tablets, debug_point, abnormal ->
        insert_data.call()

        run_sql.call()

        int replicaNum = 1
        def dedup_tablets = deduplicate_tablets(tablets)
        if (dedup_tablets.size() > 0) {
            replicaNum = Math.round(tablets.size() / dedup_tablets.size())
            if (replicaNum != 1 && replicaNum != 3) {
                assert(false)
            }
        }

        // before full compaction, there are 7 rowsets.
        int rowsetCount = get_rowset_count.call(tablets);
        assert (rowsetCount == 7 * replicaNum)

        // debug point, enable it, triger full compaction, wait full compaction done, and disable the debug point
        try {
            GetDebugPoint().enableDebugPointForAllBEs(debug_point)
            logger.info("trigger_full_compaction_on_tablets with fault injection: ${debug_point}")
            trigger_full_compaction_on_tablets.call(tablets)
            wait_full_compaction_done.call(tablets)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(debug_point)
        }

        if (abnormal) {
            // after fault injection, there are still 7 rowsets.
            rowsetCount = get_rowset_count.call(tablets);
            assert (rowsetCount == 7 * replicaNum)

            logger.info("trigger_full_compaction_on_tablets normally")
            // trigger full compactions for all tablets in ${tableName}
            // this time, index compaction of some columns will be skipped because of the fault injection
            trigger_full_compaction_on_tablets.call(tablets)

            // wait for full compaction done
            wait_full_compaction_done.call(tablets)
        }

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }

        run_sql.call()
    }

    def create_and_test_table = { table_name, key_type, debug_points, is_abnormal ->
        debug_points.each { debug_point ->
            sql """ DROP TABLE IF EXISTS ${table_name}; """
            sql """
                CREATE TABLE ${table_name} (
                    `id` int(11) NULL,
                    `name` varchar(255) NULL,
                    `age` int(11) NULL,
                    `scores` array<int> NULL,
                    `hobbies` array<text> NULL,
                    `description` text NULL,
                    `evaluation` array<text> NULL,
                    index index_name (name) using inverted,
                    index index_age (age) using inverted,
                    index index_scores (scores) using inverted,
                    index index_hobbies (hobbies) using inverted,
                    index index_description (description) using inverted properties("parser" = "english"),
                    index index_evaluation (evaluation) using inverted
                ) ENGINE=OLAP
                ${key_type} KEY(`id`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "inverted_index_storage_format" = "V1",
                "disable_auto_compaction" = "true"
                );
            """

            def tablets = sql_return_maparray """ show tablets from ${table_name}; """
            run_test.call(tablets, debug_point, is_abnormal)
        }
    }

    boolean invertedIndexCompactionEnable = false
    boolean has_update_be_config = false
    try {
        String backend_id;
        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "inverted_index_compaction_enable") {
                invertedIndexCompactionEnable = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("inverted_index_compaction_enable: ${((List<String>) ele)[2]}")
            }
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("disable_auto_compaction: ${((List<String>) ele)[2]}")
            }
        }
        set_be_config.call("inverted_index_compaction_enable", "true")
        has_update_be_config = true
        // check updated config
        check_config.call("inverted_index_compaction_enable", "true");

        // duplicated key table
        create_and_test_table.call(tableName, "DUPLICATE", debug_points_abnormal_compaction, true)
        create_and_test_table.call(tableName, "DUPLICATE", debug_points_normal_compaction, false)

        // unique key table
        tableName = "test_index_compaction_exception_fault_injection_unique"
        create_and_test_table.call(tableName, "UNIQUE", debug_points_abnormal_compaction, true)
        create_and_test_table.call(tableName, "UNIQUE", debug_points_normal_compaction, false)
       
    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
        }
        sql "SET global enable_match_without_inverted_index = true"
    }

}
