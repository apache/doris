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

suite("test_build_index_exception_fault_injection", "nonConcurrent,p2") {
    if (isCloudMode()) {
        return
    }
    def tableNamePrefix = "test_build_index_exception_fault_injection"

    def changed_variables = sql "show variables where Changed = 1"
    logger.info("changed variables: " + changed_variables.toString())
    // sql "UNSET GLOBAL VARIABLE ALL;"
    sql "SET global enable_match_without_inverted_index = false"

    // prepare test table
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

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "FINISHED" || last_job_state == "CANCELLED") {
                    logger.info(table_name + " last index job finished, state: " + last_job_state + ", detail: " + alter_res[alter_res.size()-1])
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_finish debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_finish timeout")
        return "wait_timeout"
    }
    
    def creata_table = { String tableName, String format -> 
        sql "DROP TABLE IF EXISTS ${tableName}"

        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `age` int(11) NULL,
                `scores` array<int> NULL,
                `hobbies` array<text> NULL,
                `description` text NULL,
                `evaluation` array<text> NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "${format}",
            "disable_auto_compaction" = "true"
            );
        """
    }

    def create_index = { String tableName ->
        sql "CREATE INDEX idx_name ON ${tableName} (name) USING INVERTED"
        wait_for_latest_op_on_table_finish("${tableName}", timeout)
        sql "CREATE INDEX idx_age ON ${tableName} (age) USING INVERTED"
        wait_for_latest_op_on_table_finish("${tableName}", timeout)
        sql "CREATE INDEX idx_scores ON ${tableName} (scores) USING INVERTED"
        wait_for_latest_op_on_table_finish("${tableName}", timeout)
        sql "CREATE INDEX idx_hobbies ON ${tableName} (hobbies) USING INVERTED"
        wait_for_latest_op_on_table_finish("${tableName}", timeout)
        sql "CREATE INDEX idx_description ON ${tableName} (description) USING INVERTED properties(\"parser\" = \"english\")"
        wait_for_latest_op_on_table_finish("${tableName}", timeout)
        sql "CREATE INDEX idx_evaluation ON ${tableName} (evaluation) USING INVERTED"
        wait_for_latest_op_on_table_finish("${tableName}", timeout)
    }

    def build_index = { String tableName ->
        sql "BUILD INDEX idx_name ON ${tableName}"
        wait_for_last_build_index_on_table_finish("${tableName}", timeout)
        sql "BUILD INDEX idx_age ON ${tableName}"
        wait_for_last_build_index_on_table_finish("${tableName}", timeout)
        sql "BUILD INDEX idx_scores ON ${tableName}"
        wait_for_last_build_index_on_table_finish("${tableName}", timeout)
        sql "BUILD INDEX idx_hobbies ON ${tableName}"
        wait_for_last_build_index_on_table_finish("${tableName}", timeout)
        sql "BUILD INDEX idx_description ON ${tableName}"
        wait_for_last_build_index_on_table_finish("${tableName}", timeout)
        sql "BUILD INDEX idx_evaluation ON ${tableName}"
        wait_for_last_build_index_on_table_finish("${tableName}", timeout)
    }
    
    def run_insert = { String tableName ->
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", 10, [89, 80, 98], ["football", "basketball"], "andy is good at sports", ["andy has a good heart", "andy is so nice"]); """
        sql """ INSERT INTO ${tableName} VALUES (1, "bason", 11, [79, 85, 97], ["singing", "dancing"], "bason is good at singing", ["bason is very clever", "bason is very healthy"]); """
        sql """ INSERT INTO ${tableName} VALUES (2, "andy", 10, [89, 80, 98], ["football", "basketball"], "andy is good at sports", ["andy has a good heart", "andy is so nice"]); """
        sql """ INSERT INTO ${tableName} VALUES (2, "bason", 11, [79, 85, 97], ["singing", "dancing"], "bason is good at singing", ["bason is very clever", "bason is very healthy"]); """
        sql """ INSERT INTO ${tableName} VALUES (3, "andy", 10, [89, 80, 98], ["football", "basketball"], "andy is good at sports", ["andy has a good heart", "andy is so nice"]); """
        sql """ INSERT INTO ${tableName} VALUES (3, "bason", 11, [79, 85, 97], ["singing", "dancing"], "bason is good at singing", ["bason is very clever", "bason is very healthy"]); """
    }

    def check_count = { String tableName, int count ->
        def result = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(count, result[0][0])
    }

    def run_select = { String tableName, boolean normal ->
        def result

        if (normal) {
            result = sql_return_maparray "SELECT * FROM ${tableName} WHERE name MATCH 'andy'"
            assertEquals(3, result.size())
            assertEquals(1, result[0]['id'])
            assertEquals("andy", result[0]['name'])
            assertEquals(2, result[1]['id'])
            assertEquals("andy", result[1]['name'])
            assertEquals(3, result[2]['id'])
            assertEquals("andy", result[2]['name'])

            result = sql_return_maparray "SELECT * FROM ${tableName} WHERE description MATCH 'sports'"
            assertEquals(3, result.size())
            assertEquals("andy", result[0]['name'])
            assertEquals("andy is good at sports", result[0]['description'])
            assertEquals("andy", result[1]['name'])
            assertEquals("andy is good at sports", result[1]['description'])
            assertEquals("andy", result[2]['name'])
            assertEquals("andy is good at sports", result[2]['description'])
        } else {
            try {
                result = sql_return_maparray "SELECT * FROM ${tableName} WHERE name MATCH 'andy'"
                assertTrue(0, result.size())
            } catch (Exception e) {
                log.error("Caught exception: ${e}")
                assertContains(e.toString(), "[E-6001]match_any not support execute_match")
            }
            try {
                result = sql_return_maparray "SELECT * FROM ${tableName} WHERE description MATCH 'sports'"
                assertTrue(0, result.size())
            } catch (Exception e) {
                log.error("Caught exception: ${e}")
                assertContains(e.toString(), "[E-6001]match_any not support execute_match")
            }
        }

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE age < 11"
        assertEquals(3, result.size())
        assertEquals("andy", result[0]['name'])
        assertEquals(2, result[1]['id'])
        assertEquals("andy", result[1]['name'])
        assertEquals(3, result[2]['id'])
        assertEquals("andy", result[2]['name'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(scores, 79)"
        assertEquals(3, result.size())
        assertEquals("bason", result[0]['name'])
        assertEquals("[79, 85, 97]", result[0]['scores'])
        assertEquals("bason", result[1]['name'])
        assertEquals("[79, 85, 97]", result[1]['scores'])
        assertEquals("bason", result[2]['name'])
        assertEquals("[79, 85, 97]", result[2]['scores'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(hobbies, 'football')"
        assertEquals(3, result.size())
        assertEquals("andy", result[0]['name'])
        assertEquals('["football", "basketball"]', result[0]['hobbies'])
        assertEquals("andy", result[1]['name'])
        assertEquals('["football", "basketball"]', result[1]['hobbies'])
        assertEquals("andy", result[2]['name'])
        assertEquals('["football", "basketball"]', result[2]['hobbies'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(evaluation, 'andy is so nice')"
        assertEquals(3, result.size())
        assertEquals("andy", result[0]['name'])
        assertEquals('["andy has a good heart", "andy is so nice"]', result[0]['evaluation'])
        assertEquals("andy", result[1]['name'])
        assertEquals('["andy has a good heart", "andy is so nice"]', result[1]['evaluation'])
        assertEquals("andy", result[2]['name'])
        assertEquals('["andy has a good heart", "andy is so nice"]', result[2]['evaluation'])
    }

    // define debug points array
    def debug_points = [
        "IndexBuilder::update_inverted_index_info_is_local_rowset",
        "IndexBuilder::update_inverted_index_info_size_st_not_ok",
        "IndexBuilder::update_inverted_index_info_index_file_reader_init_not_ok",
        "IndexBuilder::handle_single_rowset_is_local_rowset",
        "IndexBuilder::handle_single_rowset_can_not_find_reader_drop_op",
        "IndexBuilder::handle_single_rowset_can_not_find_reader",
        "IndexBuilder::handle_single_rowset_support_inverted_index",
        "IndexBuilder::handle_single_rowset_index_column_writer_create_error",
        "IndexBuilder::handle_single_rowset_create_iterator_error",
        "IndexBuilder::handle_single_rowset",
        "IndexBuilder::handle_single_rowset_iterator_next_batch_error",
        "IndexBuilder::handle_single_rowset_write_inverted_index_data_error",
        "IndexBuilder::handle_single_rowset_index_build_finish_error",
        "IndexBuilder::handle_single_rowset_file_writer_close_error",
        // "IndexBuilder::_write_inverted_index_data_column_idx_is_negative" // skip build index
        "IndexBuilder::_write_inverted_index_data_convert_column_data_error",
        "IndexBuilder::_add_nullable_add_array_values_error",
        "IndexBuilder::_add_nullable_throw_exception",
        "IndexBuilder::_add_data_throw_exception",
        "IndexBuilder::do_build_inverted_index_alter_inverted_indexes_empty",
        "IndexBuilder::do_build_inverted_index_modify_rowsets_status_error",
        "IndexBuilder::gc_output_rowset_is_local_rowset"
    ]

    def inverted_index_storage_format = ["v1", "v2"]
    inverted_index_storage_format.each { format ->
        def tableName = "${tableNamePrefix}_${format}"

        // for each debug point, enable it, run the insert, check the count, and disable the debug point
        // catch any exceptions and disable the debug point
        debug_points.each { debug_point ->
            try {
                GetDebugPoint().enableDebugPointForAllBEs(debug_point)
                creata_table("${tableName}", format)
                run_insert("${tableName}")
                create_index("${tableName}")
                build_index("${tableName}")
                check_count("${tableName}", 6)
                run_select("${tableName}", false)
            } catch (Exception e) {
                log.error("Caught exception: ${e}")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(debug_point)
            }
        }
    }

    sql "SET global enable_match_without_inverted_index = true"
}
