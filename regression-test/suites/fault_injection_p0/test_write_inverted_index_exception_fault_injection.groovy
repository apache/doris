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

suite("test_write_inverted_index_exception_fault_injection", "nonConcurrent") {
    def tableNamePrefix = "test_write_inverted_index_exception_fault_injection"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
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

    def changed_variables = sql "show variables where Changed = 1"
    logger.info("changed variables: " + changed_variables.toString())
    // sql "UNSET GLOBAL VARIABLE ALL;"

    sql "SET global enable_match_without_inverted_index = false"
    boolean inverted_index_ram_dir_enable = true
    boolean has_update_be_config = false
    
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
                `evaluation` array<text> NULL,
                index index_name (name) using inverted,
                index index_age (age) using inverted,
                index index_scores (scores) using inverted,
                index index_hobbies (hobbies) using inverted,
                index index_description (description) using inverted properties("parser" = "english"),
                index index_evaluation (evaluation) using inverted
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
        def result = sql_return_maparray "SELECT * FROM ${tableName} WHERE name MATCH 'andy'"
        assertEquals(3, result.size())
        assertEquals(1, result[0]['id'])
        assertEquals("andy", result[0]['name'])
        assertEquals(2, result[1]['id'])
        assertEquals("andy", result[1]['name'])
        assertEquals(3, result[2]['id'])
        assertEquals("andy", result[2]['name'])

        result = sql_return_maparray "SELECT * FROM ${tableName} WHERE age < 11"
        assertEquals(3, result.size())
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

        if (normal) {
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
        } else {
            result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(scores, 79)"
            assertEquals(0, result.size())

            result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(hobbies, 'football')"
            assertEquals(0, result.size())

            result = sql_return_maparray "SELECT * FROM ${tableName} WHERE array_contains(evaluation, 'andy is so nice')"
            assertEquals(0, result.size())
        }
    }

    // define debug points array
    def debug_points = [
        "inverted_index_parser.get_parser_stopwords_from_properties",
        "CharFilterFactory::create_return_nullptr",
        "InvertedIndexFileWriter::open_local_fs_exists_error",
        "InvertedIndexFileWriter::open_local_fs_exists_true",
        "InvertedIndexFileWriter::delete_index_index_meta_nullptr",
        "InvertedIndexFileWriter::delete_index_indices_dirs_reach_end",
        "InvertedIndexFileWriter::copyFile_openInput_error",
        "InvertedIndexFileWriter::copyFile_remainder_is_not_zero",
        "InvertedIndexFileWriter::copyFile_diff_not_equals_length",
        "InvertedIndexFileWriter::write_v1_ram_output_is_nullptr",
        "InvertedIndexFileWriter::write_v1_out_dir_createOutput_nullptr",
        "FSIndexInput::~SharedHandle_reader_close_error",
        "DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error",
        "DorisFSDirectory::FSIndexInput::readInternal_bytes_read_error",
        "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init",
        "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor",
        "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer",
        "DorisFSDirectory::FSIndexOutput::flushBuffer_writer_is_nullptr",
        "DorisFSDirectory::FSIndexOutput::flushBuffer_b_is_nullptr",
        "DorisFSDirectory::FSIndexOutput.set_writer_nullptr",
        "DorisFSDirectory::FSIndexOutput._set_writer_close_status_error",
        "DorisFSDirectory::FSIndexOutputV2::flushBuffer_file_writer_is_nullptr",
        "DorisFSDirectory::FSIndexOutputV2::flushBuffer_b_is_nullptr",
        "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_close",
        "DorisFSDirectory::list_status_is_not_ok",
        "DorisFSDirectory::list_directory_not_exists",
        "DorisFSDirectory::fileExists_status_is_not_ok",
        "DorisFSDirectory::touchFile_status_is_not_ok",
        "DorisFSDirectory::fileLength_status_is_not_ok",
        //"DorisFSDirectory::close_close_with_error", // will block the process, off now
        "DorisFSDirectory::doDeleteFile_status_is_not_ok",
        "DorisFSDirectory::deleteDirectory_throw_is_not_directory",
        "DorisFSDirectory::renameFile_exists_status_is_not_ok",
        "DorisFSDirectory::renameFile_delete_status_is_not_ok",
        "DorisFSDirectory::renameFile_rename_status_is_not_ok",
        "DorisFSDirectory::createOutput_exists_status_is_not_ok",
        "DorisFSDirectory::createOutput_delete_status_is_not_ok",
        "DorisFSDirectory::createOutput_exists_after_delete_status_is_not_ok",
        "DorisFSDirectory::createOutput_exists_after_delete_error",
        "DorisRAMFSDirectory::fileModified_file_not_found",
        "DorisRAMFSDirectory::touchFile_file_not_found",
        "DorisRAMFSDirectory::fileLength_file_not_found",
        "DorisRAMFSDirectory::openInput_file_not_found",
        "DorisRAMFSDirectory::close_close_with_error",
        "DorisRAMFSDirectory::createOutput_itr_filesMap_end",
        "DorisFSDirectoryFactory::getDirectory_file_is_nullptr",
        "DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok",
        "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_not_ok",
        "InvertedIndexColumnWriter::init_field_type_not_supported",
        "InvertedIndexColumnWriter::init_inverted_index_writer_init_error",
        "InvertedIndexColumnWriter::close_on_error_throw_exception",
        "InvertedIndexColumnWriter::init_bkd_index_throw_error",
        "InvertedIndexColumnWriter::create_chinese_analyzer_throw_error",
        "InvertedIndexColumnWriter::open_index_directory_error",
        "InvertedIndexColumnWriter::create_index_writer_setRAMBufferSizeMB_error",
        "InvertedIndexColumnWriter::create_index_writer_setMaxBufferedDocs_error",
        "InvertedIndexColumnWriter::create_index_writer_setMergeFactor_error",
        "InvertedIndexColumnWriterImpl::add_document_throw_error",
        "InvertedIndexColumnWriterImpl::add_null_document_throw_error",
        "InvertedIndexColumnWriterImpl::add_nulls_field_nullptr",
        "InvertedIndexColumnWriterImpl::add_nulls_index_writer_nullptr",
        "InvertedIndexColumnWriterImpl::new_char_token_stream__char_string_reader_init_error",
        "InvertedIndexColumnWriterImpl::add_values_field_is_nullptr",
        "InvertedIndexColumnWriterImpl::add_values_index_writer_is_nullptr",
        "InvertedIndexColumnWriterImpl::add_array_values_count_is_zero",
        "InvertedIndexColumnWriterImpl::add_array_values_index_writer_is_nullptr",
        "InvertedIndexColumnWriterImpl::add_array_values_create_field_error",
        "InvertedIndexColumnWriterImpl::add_array_values_create_field_error_2",
        "InvertedIndexColumnWriterImpl::add_array_values_field_is_nullptr",
        "InvertedIndexWriter._throw_clucene_error_in_fulltext_writer_close",
        "InvertedIndexColumnWriter::create_array_typeinfo_is_nullptr",
        "InvertedIndexColumnWriter::create_unsupported_type_for_inverted_index"
    ]

    def inverted_index_storage_format = ["v1", "v2"]

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
            if (((List<String>) ele)[0] == "inverted_index_ram_dir_enable") {
                invertedIndexCompactionEnable = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("inverted_index_ram_dir_enable: ${((List<String>) ele)[2]}")
            }
        }
        set_be_config.call("inverted_index_ram_dir_enable", "false")
        has_update_be_config = true
        // check updated config
        check_config.call("inverted_index_ram_dir_enable", "false");
        inverted_index_storage_format.each { format ->
            def tableName = "${tableNamePrefix}_${format}"
            creata_table("${tableName}", format)

            // for each debug point, enable it, run the insert, check the count, and disable the debug point
            // catch any exceptions and disable the debug point
            debug_points.each { debug_point ->
                try {
                    GetDebugPoint().enableDebugPointForAllBEs(debug_point)
                    run_insert("${tableName}")
                    check_count("${tableName}", 6)
                    // if debug_point equals InvertedIndexColumnWriterImpl::add_array_values_count_is_zero, run_select(false)
                    // else run_select(true)
                    if (debug_point == "InvertedIndexColumnWriterImpl::add_array_values_count_is_zero") {
                        run_select("${tableName}", false)
                    } else {
                        run_select("${tableName}", true)
                    }
                    sql "TRUNCATE TABLE ${tableName}"
                } catch (Exception e) {
                    log.error("Caught exception: ${e}")
                    check_count("${tableName}", 0)
                } finally {
                    GetDebugPoint().disableDebugPointForAllBEs(debug_point)
                }
            }
        }
    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_ram_dir_enable", inverted_index_ram_dir_enable.toString())
        }
        sql "SET global enable_match_without_inverted_index = true"
    }

}
