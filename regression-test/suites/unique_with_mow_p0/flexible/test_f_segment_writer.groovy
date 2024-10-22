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
import org.apache.doris.regression.suite.ClusterOptions

suite('test_f_segment_writer', 'docker') {
    def options = new ClusterOptions()
    options.beConfigs += [
        'enable_vertical_segment_writer=false'
    ]
    docker(options) {
        def tableName = "test_flexible_partial_update_segment_writer"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true"); """

        def show_res = sql "show create table ${tableName}"
        assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))
        sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
        qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test1.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[NOT_IMPLEMENTED_ERROR]SegmentWriter doesn't support flexible partial update, please set enable_vertical_segment_writer=true in be.conf on all BEs to use VerticalSegmentWriter."));
            }
        }
        qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"
    }
}