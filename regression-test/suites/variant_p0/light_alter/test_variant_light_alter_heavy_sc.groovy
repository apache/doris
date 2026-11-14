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

suite("test_variant_light_alter_heavy_sc", "p0,nonConcurrent") {
    def compareWithControl = { String tag, String cols, String where = "" ->
        // Compare independent extraction from the whole row, not another subpath read.
        def inconsistent = ['p'].collect { path ->
            "NOT (CAST(v['${path}'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.${path}'))"
        }.join(' OR ')
        "order_qt_${tag}_whole_path_invariant"("SELECT id FROM variant_light_alter_heavy_sc WHERE ${inconsistent}")
        def query = { String table, String projection -> "SELECT ${projection} FROM ${table} ${where}" }
        "order_qt_${tag}"(query("variant_light_alter_heavy_sc", cols))
        "order_qt_${tag}_control"(query("variant_light_alter_heavy_sc_control", cols))
        def normalized = cols.split(", ").toList().withIndex().collect { col, i -> "CAST(${col} AS STRING) AS c${i}" }.join(", ")
        def actual = query("variant_light_alter_heavy_sc", normalized), expected = query("variant_light_alter_heavy_sc_control", normalized)
        "order_qt_${tag}_difference"("""SELECT 'altered_only', delta.* FROM (${actual} EXCEPT ${expected}) delta
            UNION ALL SELECT 'control_only', delta.* FROM (${expected} EXCEPT ${actual}) delta""")
    }
    def alterAndWait = { String change ->
        sql "ALTER TABLE variant_light_alter_heavy_sc ${change}"
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_alter_heavy_sc' ORDER BY CreateTime DESC LIMIT 1"
            time 60
        }
    }
    sql "DROP TABLE IF EXISTS variant_light_alter_heavy_sc"
    sql """CREATE TABLE variant_light_alter_heavy_sc (id INT, n INT, v VARIANT<PROPERTIES('variant_max_subcolumns_count'='2')>) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql "DROP TABLE IF EXISTS variant_light_alter_heavy_sc_control"
    sql """CREATE TABLE variant_light_alter_heavy_sc_control (id INT, n BIGINT, v VARIANT<MATCH_NAME 'p':INT, PROPERTIES('variant_max_subcolumns_count'='2')>) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    def rows = ["""(1,1,parse_to_variant('{"p":"x7","a":1}'))""",
        """(2,2,parse_to_variant('{"p":"001","b":2}'))""",
        """(3,3,parse_to_variant('{"p":5,"c":3}'))"""]
    sql "INSERT INTO variant_light_alter_heavy_sc VALUES ${rows.take(2).join(', ')}"
    alterAndWait("MODIFY COLUMN v VARIANT<MATCH_NAME 'p':INT, PROPERTIES('variant_max_subcolumns_count'='2')>")
    order_qt_mixed "SELECT id, n, v['p'], v FROM variant_light_alter_heavy_sc"
    alterAndWait("MODIFY COLUMN n BIGINT")
    order_qt_rewritten "SELECT id, n, v['p'], v FROM variant_light_alter_heavy_sc"
    sql "INSERT INTO variant_light_alter_heavy_sc VALUES ${rows[2]}"
    sql "INSERT INTO variant_light_alter_heavy_sc_control VALUES ${rows.join(', ')}"
    trigger_and_wait_compaction("variant_light_alter_heavy_sc", "full")
    compareWithControl("compacted", "id, n, v['p'], CAST(v['p'] AS INT), v")
    compareWithControl("predicate", "id", "WHERE v['p'] IS NULL")
}
