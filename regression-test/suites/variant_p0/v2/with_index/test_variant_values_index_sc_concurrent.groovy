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

import org.awaitility.Awaitility
import java.util.concurrent.TimeUnit
suite("test_variant_values_index_sc_concurrent", "p0,nonConcurrent") {
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=10"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "DROP TABLE IF EXISTS vvi_sc_concurrent"
    sql """CREATE TABLE vvi_sc_concurrent(id INT,aux INT,v VARIANT<'tag*':STRING>,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"),
        INDEX tag_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="unicode"))
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","enable_unique_key_merge_on_write"="true",
                   "inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_sc_concurrent VALUES(1,1,parse_to_variant('{"s":"old","tagA":"alpha"}')),
        (2,2,parse_to_variant('{"s":"retained","tagA":"alpha"}'))"""
    def point = isCloudMode() ? "CloudSchemaChangeJob::_convert_historical_rowsets.block" : "SchemaChangeJob::_convert_historical_rowsets.block"
    GetDebugPoint().enableDebugPointForAllBEs(point)
    try {
        sql "ALTER TABLE vvi_sc_concurrent MODIFY COLUMN aux BIGINT"
        Awaitility.await().atMost(120,TimeUnit.SECONDS).pollInterval(200,TimeUnit.MILLISECONDS).until {
            sql_return_maparray("SHOW ALTER TABLE COLUMN WHERE TableName='vvi_sc_concurrent' ORDER BY CreateTime DESC LIMIT 1")[0].State == "RUNNING"
        }
        sql """INSERT INTO vvi_sc_concurrent VALUES(1,11,parse_to_variant('{"s":"replacement","tagA":"delta"}')),
            (3,33,parse_to_variant('{"s":"new","tagB":"gamma"}'))"""
        // MOW DELETE uses partial update and is explicitly rejected during schema change.
        test {
            sql "DELETE FROM vvi_sc_concurrent WHERE id=2"
            exception "Can't do partial update when table is doing schema change"
        }
        assertEquals("RUNNING",sql_return_maparray("SHOW ALTER TABLE COLUMN WHERE TableName='vvi_sc_concurrent' ORDER BY CreateTime DESC LIMIT 1")[0].State)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(point)
    }
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_sc_concurrent' ORDER BY CreateTime DESC LIMIT 1"
        time 180
    }
    sql "DELETE FROM vvi_sc_concurrent WHERE id=2"
    sql "SET enable_match_without_inverted_index=false"
    for (String phase : ["switched","compacted"]) {
        if (phase == "compacted") { trigger_and_wait_compaction("vvi_sc_concurrent", "full", 1800) }
        quickTest("${phase}_values", "SELECT id,aux,v FROM vvi_sc_concurrent ORDER BY id", true)
        for (String term : ["replacement","new","old","retained"]) {
            quickTest("${phase}_${term}", "SELECT id FROM vvi_sc_concurrent WHERE v MATCH_ANY '${term}' ORDER BY id", true)
        }
        quickTest("${phase}_path", "SELECT id FROM vvi_sc_concurrent WHERE v['s'] MATCH_ANY 'replacement' ORDER BY id", true)
        quickTest("${phase}_template", "SELECT id FROM vvi_sc_concurrent WHERE v['tagA'] MATCH_ANY 'delta' ORDER BY id", true)
        quickTest("${phase}_removed_template", "SELECT id FROM vvi_sc_concurrent WHERE v['tagA'] MATCH_ANY 'alpha' ORDER BY id", true)
    }
}
