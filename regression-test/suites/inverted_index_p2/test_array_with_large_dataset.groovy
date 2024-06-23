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

suite("test_array_with_large_dataset", "p2"){

    def StreamLoad = {tableName, fileName ->
        streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'max_filter_ratio', '0.3'
            set 'compress_type', 'GZ'

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file fileName
            time 300000
            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    // create table 
    sql """ DROP TABLE IF EXISTS bai;"""
    sql """
            CREATE TABLE `bai` (
              `id` BIGINT NULL,
              `asl` ARRAY<INT> NULL,
              `ash` ARRAY<INT> NULL,
              INDEX index_inverted_ail (`asl`) USING INVERTED COMMENT '''''',
              INDEX index_inverted_aih (`ash`) USING INVERTED COMMENT ''''''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 16
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
    );
    """
    
    def array_files = ["array_int_1.tar.gz", "array_int_500001.tar.gz", "array_int_1000001.tar.gz", "array_int_1500001.tar.gz", "array_int_2000001.tar.gz"]
    for (f in array_files) {
        def file_name = "${getS3Url()}/regression/array_index/" + f
        StreamLoad.call("bai", file_name)
    }
    sql """sync"""

    // check result
    qt_select "SELECT count(*) FROM bai;"

    // download tar.gz file for
}
