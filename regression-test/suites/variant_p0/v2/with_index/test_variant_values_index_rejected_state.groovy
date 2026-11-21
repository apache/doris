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

suite("test_variant_values_index_rejected_state", "p0,nonConcurrent") {
    sql "SET enable_add_index_for_new_data=true"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_max_subcolumns_count=10"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET enable_match_without_inverted_index=false"
    sql "DROP TABLE IF EXISTS vvi_rejected_state"
    sql """CREATE TABLE vvi_rejected_state(id INT,aux INT,v VARIANT<'tag*':STRING>,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"),
        INDEX tag_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="unicode"))
        DUPLICATE KEY(id) PARTITION BY RANGE(id)(PARTITION p1 VALUES LESS THAN(100))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII",
                   "bloom_filter_columns"="aux")"""
    sql """INSERT INTO vvi_rejected_state VALUES(1,11,parse_to_variant('{"s":"hello world","tagA":"alpha"}'))"""
    def database = sql("SELECT DATABASE()")[0][0]
    def dbId = sql("SHOW PROC '/dbs'").find { it[1] == database }[0]
    def state = {
        def tablet = sql_return_maparray("SHOW TABLETS FROM vvi_rejected_state")[0]
        def (code, out, err) = curl("GET", tablet.MetaUrl + "?byte_to_base64=true")
        assertEquals(0, code, err)
        def meta = parseJson(out)
        [sql("SHOW CREATE TABLE vvi_rejected_state"), sql("DESC vvi_rejected_state"),
         sql("SHOW INDEX FROM vvi_rejected_state"),
         sql("SHOW ALTER TABLE COLUMN WHERE TableName='vvi_rejected_state'"),
         sql("SHOW BUILD INDEX WHERE TableName='vvi_rejected_state'"), meta.schema.schema_version,
         sql("SHOW PROC '/dbs/${dbId}/${meta.table_id}/index_schema'")]
    }
    def rejected = [
        ['ALTER TABLE vvi_rejected_state ADD COLUMN extra INT, ADD INDEX new_values(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")', 'ADD VARIANT values index can not be combined with other schema changes'],
        ['CREATE INDEX new_values ON vvi_rejected_state(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")', 'VARIANT column supports only one values index'],
        ['ALTER TABLE vvi_rejected_state ADD INDEX new_values(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")', 'VARIANT column supports only one values index'],
        ['DROP INDEX values_idx ON vvi_rejected_state PARTITION(p1)', 'DROP INDEX is not supported for a VARIANT values index'],
        [isCloudMode() ? 'BUILD INDEX ON vvi_rejected_state' : 'BUILD INDEX path_idx ON vvi_rejected_state', 'BUILD INDEX is not supported on a table with a VARIANT values index'],
        ['ALTER TABLE vvi_rejected_state ADD INDEX new_template(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="chinese")', 'Can not create index with field pattern'],
        ['DROP INDEX tag_idx ON vvi_rejected_state', 'Can not drop index with field pattern'],
        ['ALTER TABLE vvi_rejected_state MODIFY COLUMN v VARIANT<\'tag*\':INT>', 'Can not change variant schema templates'],
        ['ALTER TABLE vvi_rejected_state MODIFY COLUMN v VARIANT<PROPERTIES("variant_enable_doc_mode"="true")>', 'Can not change variant max subcolumns count']
    ]
    rejected.eachWithIndex { attempt, i ->
        def before = state()
        test { sql attempt[0]; exception attempt[1] }
        assertEquals(before, state())
        sql """INSERT INTO vvi_rejected_state VALUES(${i+2},${i+12},parse_to_variant('{"s":"hello world","tagA":"alpha"}'))"""
        quickTest("rejected_${i}_root", "SELECT id FROM vvi_rejected_state WHERE v MATCH_ALL 'hello world' ORDER BY id", true)
        quickTest("rejected_${i}_path", "SELECT id FROM vvi_rejected_state WHERE v['s'] MATCH_PHRASE 'hello world' ORDER BY id", true)
        quickTest("rejected_${i}_template", "SELECT id FROM vvi_rejected_state WHERE v['tagA'] MATCH_ANY 'alpha' ORDER BY id", true)
    }
    qt_final_values "SELECT id,aux,v FROM vvi_rejected_state ORDER BY id"
    // S9: partition DROP keeps the table binding; it only rewrites the chosen partition.
    if (isCloudMode()) {
        for (String format : ["V2","V3"]) {
            def before = state()
            test {
                sql "ALTER TABLE vvi_rejected_state SET (\"partition.inverted_index_storage_format\"=\"${format}\")"
                exception "SNII"
            }
            assertEquals(before,state())
        }
    }
    sql "ALTER TABLE vvi_rejected_state ADD PARTITION p2 VALUES LESS THAN(200)"
    sql """INSERT INTO vvi_rejected_state VALUES(101,101,parse_to_variant('{"s":"hello world","tagB":"beta"}'))"""
    def retainedDDL = sql("SHOW CREATE TABLE vvi_rejected_state")
    def retainedIndexes = sql("SHOW INDEX FROM vvi_rejected_state")
    for (String index : ["path_idx","tag_idx"]) {
        def beforeJobs = sql_return_maparray("SHOW BUILD INDEX WHERE TableName='vvi_rejected_state'")
        sql "DROP INDEX ${index} ON vvi_rejected_state PARTITION(p1)"
        wait_for_last_build_index_finish("vvi_rejected_state",180000)
        def jobs = sql_return_maparray("SHOW BUILD INDEX WHERE TableName='vvi_rejected_state'")
        assertTrue(jobs.size() > beforeJobs.size())
        assertEquals(retainedDDL,sql("SHOW CREATE TABLE vvi_rejected_state"))
        assertEquals(retainedIndexes,sql("SHOW INDEX FROM vvi_rejected_state"))
        quickTest("${index}_partition_root", "SELECT id FROM vvi_rejected_state WHERE v MATCH_ALL 'hello world' ORDER BY id", true)
        quickTest("${index}_other_partition_path", "SELECT id FROM vvi_rejected_state PARTITION(p2) WHERE v['s'] MATCH_PHRASE 'hello world' ORDER BY id", true)
        quickTest("${index}_other_partition_template", "SELECT id FROM vvi_rejected_state PARTITION(p2) WHERE v['tagB'] MATCH_ANY 'beta' ORDER BY id", true)
        sql "SET enable_match_without_inverted_index=true"
        quickTest("${index}_dropped_partition_path", "SELECT id FROM vvi_rejected_state PARTITION(p1) WHERE v['s'] MATCH_ANY 'hello' ORDER BY id", true)
        quickTest("${index}_dropped_partition_template", "SELECT id FROM vvi_rejected_state PARTITION(p1) WHERE v['tagA'] MATCH_ANY 'alpha' ORDER BY id", true)
        sql "SET enable_match_without_inverted_index=false"
    }

}
