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

suite("test_variant_light_field_pattern_index", "p0,nonConcurrent") {
    def rows = [
        """(1, parse_to_variant('{"msg":"hello world","n":1,"extra":"old"}'))""",
        """(2, parse_to_variant('{"msg":"goodbye world","n":5,"extra":"keep"}'))""",
        """(3, parse_to_variant('{"msg":"hello again","n":5,"extra":"new"}'))""",
        """(4, parse_to_variant('{"msg":"other text","n":9,"extra":"new"}'))""",
        """(5, parse_to_variant('{"msg":"hello last","n":5,"extra":"new"}'))"""
    ]
    def compareWithControl = { String tag, String cols, String where = "" ->
        // Compare independent extraction from the whole row, not another subpath read.
        def inconsistent = ['msg', 'n', 'extra'].collect { path ->
            "NOT (CAST(v['${path}'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.${path}'))"
        }.join(' OR ')
        "order_qt_${tag}_whole_path_invariant"("SELECT id FROM variant_light_field_pattern_index WHERE ${inconsistent}")
        def query = { String table, String projection -> "SELECT ${projection} FROM ${table} ${where}" }
        "order_qt_${tag}"(query("variant_light_field_pattern_index", cols))
        "order_qt_${tag}_control"(query("variant_light_field_pattern_index_control", cols))
        def normalized = cols.split(", ").toList().withIndex().collect { col, i -> "CAST(${col} AS STRING) AS c${i}" }.join(", ")
        def actual = query("variant_light_field_pattern_index", normalized), expected = query("variant_light_field_pattern_index_control", normalized)
        "order_qt_${tag}_difference"("""SELECT 'altered_only', delta.* FROM (${actual} EXCEPT ${expected}) delta
            UNION ALL SELECT 'control_only', delta.* FROM (${expected} EXCEPT ${actual}) delta""")
    }
    def reloadControl = { int count ->
        // Template coercion is batch-wide: load all current rows in one INSERT.
        sql "TRUNCATE TABLE variant_light_field_pattern_index_control"
        sql "INSERT INTO variant_light_field_pattern_index_control VALUES ${rows.take(count).join(', ')}"
    }
    def alterAndWait = { String change, boolean checkImmediately = false ->
        sql "ALTER TABLE variant_light_field_pattern_index ${change}"
        def job = "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_field_pattern_index' ORDER BY CreateTime DESC LIMIT 1"
        if (checkImmediately) { assertEquals("FINISHED", sql(job)[0][9]) }
        waitForSchemaChangeDone { sql job; time 30 }
    }
    sql "DROP TABLE IF EXISTS variant_light_field_pattern_index"
    sql """CREATE TABLE variant_light_field_pattern_index (id INT, v VARIANT<'msg':STRING, 'n':INT>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql "DROP TABLE IF EXISTS variant_light_field_pattern_index_control"
    sql """CREATE TABLE variant_light_field_pattern_index_control (id INT, v VARIANT<'msg':STRING, 'n':INT, 'extra':STRING>, INDEX idx_extra(v) USING INVERTED PROPERTIES('field_pattern'='extra', 'parser'='english'))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    def queries = [match: ["COUNT(*)", "WHERE v['msg'] MATCH_ANY 'hello'"],
        msg_eq: ["COUNT(*)", "WHERE v['msg'] = 'hello world'"], msg_range: ["COUNT(*)", "WHERE v['msg'] > 'hello'"],
        n_eq: ["COUNT(*)", "WHERE v['n'] = 5"], n_range: ["COUNT(*)", "WHERE v['n'] > 3"],
        rows: ["id, v['msg'], v['n'], v['extra'], v", ""]]
    def checkQueries = { String stage, boolean compacted ->
        [false, true].each { enabled ->
            sql "set enable_inverted_index_query=${enabled}"
            queries.each { tag, q ->
                def name = "${stage}_${enabled}_${tag}"
                if (compacted) { compareWithControl(name, q[0], q[1]) }
                else { "order_qt_${name}"("SELECT ${q[0]} FROM variant_light_field_pattern_index ${q[1]}") }
            }
        }
    }
    sql "INSERT INTO variant_light_field_pattern_index VALUES ${rows.take(2).join(', ')}"
    alterAndWait("ADD INDEX idx_msg(v) USING INVERTED PROPERTIES('field_pattern'='msg', 'parser'='english')", true)
    alterAndWait("ADD INDEX idx_n(v) USING INVERTED PROPERTIES('field_pattern'='n')")
    sql "INSERT INTO variant_light_field_pattern_index VALUES ${rows[2..3].join(', ')}"
    checkQueries("mixed", false)
    reloadControl(4)
    trigger_and_wait_compaction("variant_light_field_pattern_index", "full")
    checkQueries("compacted", true)
    alterAndWait("DROP INDEX idx_msg")
    alterAndWait("DROP INDEX idx_n")
    checkQueries("dropped", false)
    reloadControl(4)
    trigger_and_wait_compaction("variant_light_field_pattern_index", "full")
    checkQueries("dropped_compacted", true)
    alterAndWait("MODIFY COLUMN v VARIANT<'msg':STRING, 'n':INT, 'extra':STRING>")
    alterAndWait("ADD INDEX idx_extra(v) USING INVERTED PROPERTIES('field_pattern'='extra', 'parser'='english')")
    sql "INSERT INTO variant_light_field_pattern_index VALUES ${rows[4]}"
    queries.extra = ["COUNT(*)", "WHERE v['extra'] MATCH_ANY 'new'"]
    checkQueries("appended", false)
    reloadControl(5)
    trigger_and_wait_compaction("variant_light_field_pattern_index", "full")
    checkQueries("appended_compacted", true)
}
