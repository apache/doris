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

suite("dotted_key_escape_stream_load", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    sql "drop table if exists dotted_key_escape_stream_load"
    sql """create table dotted_key_escape_stream_load(id int,v variant<properties(
        "variant_enable_nested_group"="false",
        "variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="1",
        "variant_max_sparse_column_statistics_size"="10000")>)
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
    streamLoad {
        table "dotted_key_escape_stream_load"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        set 'columns', 'id,raw,v=parse_to_variant(raw)'
        set 'jsonpaths', '["$.id","$.v"]'
        file 'dotted_key_escape_ingest.json'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def loaded = parseJson(result)
            assertEquals("success", loaded.Status.toLowerCase())
            assertEquals(2, loaded.NumberLoadedRows)
            assertEquals(0, loaded.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_loaded "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'] from dotted_key_escape_stream_load"
    sql """insert into dotted_key_escape_stream_load values
        (2,parse_to_variant('{"a.b":102,"a":{"b":202},"a%2Eb":302,"50%":402}'))"""
    setBeConfigTemporary([enable_vertical_compaction:true,enable_vertical_compact_variant_subcolumns:true,
            vertical_compaction_num_columns_per_group:1]) {
        trigger_and_wait_compaction("dotted_key_escape_stream_load", "full")
    }
    order_qt_loaded_compacted "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'] from dotted_key_escape_stream_load"
}
