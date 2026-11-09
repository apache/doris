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

suite("test_variant_light_alter_rowstore", "p0,nonConcurrent") {
    def compareWithControl = { String tag, String cols, String where = "" ->
        // Compare independent extraction from the whole row, not another subpath read.
        def inconsistent = ['a', 'b', 'c', 'd', 'e', 'f'].collect { path ->
            "NOT (CAST(v['${path}'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.${path}'))"
        }.join(' OR ')
        "order_qt_${tag}_whole_path_invariant"("SELECT id FROM variant_light_alter_rowstore WHERE ${inconsistent}")
        def query = { String table, String projection -> "SELECT ${projection} FROM ${table} ${where}" }
        "order_qt_${tag}"(query("variant_light_alter_rowstore", cols))
        "order_qt_${tag}_control"(query("variant_light_alter_rowstore_control", cols))
        def normalized = cols.split(", ").toList().withIndex().collect { col, i -> "CAST(${col} AS STRING) AS c${i}" }.join(", ")
        def actual = query("variant_light_alter_rowstore", normalized), expected = query("variant_light_alter_rowstore_control", normalized)
        "order_qt_${tag}_difference"("""SELECT 'altered_only', delta.* FROM (${actual} EXCEPT ${expected}) delta
            UNION ALL SELECT 'control_only', delta.* FROM (${expected} EXCEPT ${actual}) delta""")
    }
    def alterAndWait = { String change ->
        sql "ALTER TABLE variant_light_alter_rowstore ${change}"
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_alter_rowstore' ORDER BY CreateTime DESC LIMIT 1"
            time 60
        }
    }
    sql "DROP TABLE IF EXISTS variant_light_alter_rowstore"
    sql """CREATE TABLE variant_light_alter_rowstore (id INT, v VARIANT<PROPERTIES('variant_max_subcolumns_count'='3')>) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true", "enable_unique_key_merge_on_write"="true", "store_row_column"="true")"""
    sql "DROP TABLE IF EXISTS variant_light_alter_rowstore_control"
    sql """CREATE TABLE variant_light_alter_rowstore_control (id INT, v VARIANT<PROPERTIES('variant_max_subcolumns_count'='1')>) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true", "enable_unique_key_merge_on_write"="true", "store_row_column"="true")"""
    def rows = ["""(1,parse_to_variant('{"a":1,"b":2,"c":3}'))""",
        """(2,parse_to_variant('{"a":4,"d":5,"e":6}'))""",
        """(3,parse_to_variant('{"a":7,"b":8,"f":9}'))"""]
    sql "INSERT INTO variant_light_alter_rowstore VALUES ${rows.take(2).join(', ')}"
    test {
        sql "ALTER TABLE variant_light_alter_rowstore MODIFY COLUMN v VARIANT<MATCH_NAME 'a':INT, PROPERTIES('variant_max_subcolumns_count'='3')>"
        exception "Can not change variant schema templates on a row-store table"
    }
    alterAndWait("MODIFY COLUMN v VARIANT<PROPERTIES('variant_max_subcolumns_count'='1')>")
    sql "INSERT INTO variant_light_alter_rowstore VALUES ${rows[2]}"
    [1,2,3].each { id -> "order_qt_mixed_point_${id}"("SELECT * FROM variant_light_alter_rowstore WHERE id=${id}") }
    order_qt_mixed_scan "SELECT id, v FROM variant_light_alter_rowstore"
    sql "INSERT INTO variant_light_alter_rowstore_control VALUES ${rows.join(', ')}"
    trigger_and_wait_compaction("variant_light_alter_rowstore", "full")
    [1,2,3].each { id -> compareWithControl("point_${id}", "id, v", "WHERE id=${id}") }
    compareWithControl("scan", "id, v, v['a'], v['b'], v['f']")
}
