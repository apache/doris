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

suite("test_variant_light_template_add", "p0,nonConcurrent") {
    // Stored BOOLEAN -> STRING compaction casts true to "1", while a mixed fresh load can
    // shred it as "true". Use a string here; c=10 still covers conversion to the new template.
    def rows = [
        """(1, parse_to_variant('{"a":5,"c":10,"other":"keep1"}'))""", """(2, parse_to_variant('{"a":2147483648,"c":"old","other":"keep2"}'))""",
        """(3, parse_to_variant('{"a":"001","c":"true","other":"keep3"}'))""", """(4, parse_to_variant('{"a":"x","other":"keep4"}'))""",
        """(5, parse_to_variant('{"a":{"nested":1},"other":"keep5"}'))""", """(6, parse_to_variant('{"other":"keep6"}'))""",
        """(7, parse_to_variant('{"b":1,"d":2,"e":3,"a":5,"other":"keep7"}'))""", """(8, parse_to_variant('{"b":1,"d":2,"e":3}'))""",
        """(9, parse_to_variant('{"b":1,"d":2,"e":3}'))""", """(10, parse_to_variant('{"b":1,"d":2,"e":3}'))""",
        """(11, parse_to_variant('{"a":5,"c":"new","other":"keep11"}'))""", """(12, parse_to_variant('{"a":"7","c":"new","other":"keep12"}'))""",
        """(13, parse_to_variant('{"a":9,"c":"last","d":"42","other":"keep13"}'))"""
    ]
    def compareWithControl = { String tag, String cols, String where = "" ->
        // Compare independent extraction from the whole row, not another subpath read.
        def inconsistent = ['a', 'b', 'c', 'd', 'e', 'other'].collect { path ->
            "NOT (CAST(v['${path}'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.${path}'))"
        }.join(' OR ')
        "order_qt_${tag}_whole_path_invariant"("SELECT id FROM variant_light_template_add WHERE ${inconsistent}")
        def query = { String table, String projection -> "SELECT ${projection} FROM ${table} ${where}" }
        "order_qt_${tag}"(query("variant_light_template_add", cols))
        "order_qt_${tag}_control"(query("variant_light_template_add_control", cols))
        def normalized = cols.split(", ").toList().withIndex().collect { col, i -> "CAST(${col} AS STRING) AS c${i}" }.join(", ")
        def actual = query("variant_light_template_add", normalized), expected = query("variant_light_template_add_control", normalized)
        "order_qt_${tag}_difference"("""SELECT 'altered_only', delta.* FROM (${actual} EXCEPT ${expected}) delta
            UNION ALL SELECT 'control_only', delta.* FROM (${expected} EXCEPT ${actual}) delta""")
    }
    def reloadControl = { int count ->
        // Template coercion is batch-wide: load all current rows in one INSERT.
        sql "TRUNCATE TABLE variant_light_template_add_control"
        sql "INSERT INTO variant_light_template_add_control VALUES ${rows.take(count).join(', ')}"
    }
    def alterAndWait = { String change, boolean checkImmediately = false ->
        sql "ALTER TABLE variant_light_template_add ${change}"
        def job = "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_template_add' ORDER BY CreateTime DESC LIMIT 1"
        if (checkImmediately) { assertEquals("FINISHED", sql(job)[0][9]) }
        waitForSchemaChangeDone { sql job; time 30 }
    }
    sql "DROP TABLE IF EXISTS variant_light_template_add"
    sql """CREATE TABLE variant_light_template_add (id INT, v VARIANT<PROPERTIES("variant_max_subcolumns_count"="3")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql "DROP TABLE IF EXISTS variant_light_template_add_control"
    sql """CREATE TABLE variant_light_template_add_control (id INT, v VARIANT<'a':INT, 'c':STRING, 'd':BIGINT, PROPERTIES("variant_max_subcolumns_count"="3")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    def queries = [a: ["id, v['a']", ""], cast_a: ["id, CAST(v['a'] AS INT)", ""],
        eq: ["id", "WHERE v['a'] = 5"], range: ["id", "WHERE v['a'] > 3"],
        null: ["id", "WHERE v['a'] IS NULL"], c: ["id, v['c']", ""],
        whole: ["id, v", ""], other: ["id, v['other']", ""]]
    // Keep the old scalar/object batches separate; frequent b/d/e leave rare a sparse.
    [[0, 1], [2, 3], [4, 5], [6, 9]].each { lo, hi ->
        sql "INSERT INTO variant_light_template_add VALUES ${rows[lo..hi].join(', ')}"
    }
    alterAndWait("MODIFY COLUMN v VARIANT<'a':INT, 'c':STRING, PROPERTIES('variant_max_subcolumns_count'='3')>", true)
    sql "INSERT INTO variant_light_template_add VALUES ${rows[10..11].join(', ')}"
    queries.each { tag, q -> "order_qt_mixed_${tag}"("SELECT ${q[0]} FROM variant_light_template_add ${q[1]}") }
    reloadControl(12)
    trigger_and_wait_compaction("variant_light_template_add", "full")
    queries.each { tag, q -> compareWithControl("compacted_${tag}", q[0], q[1]) }
    alterAndWait("MODIFY COLUMN v VARIANT<'a':INT, 'c':STRING, 'd':BIGINT, PROPERTIES('variant_max_subcolumns_count'='3')>")
    sql "INSERT INTO variant_light_template_add VALUES ${rows[12]}"
    order_qt_second_mixed "SELECT id, v['a'], v['c'], v['d'], v, v['other'] FROM variant_light_template_add"
    reloadControl(13)
    trigger_and_wait_compaction("variant_light_template_add", "full")
    compareWithControl("second_compacted", "id, v['a'], v['c'], v['d'], v, v['other']")
    // Three policy changes before any compaction, then one converging rewrite.
    sql "DROP TABLE IF EXISTS variant_light_pending_alters"
    sql """CREATE TABLE variant_light_pending_alters(id INT,
        v VARIANT<PROPERTIES('variant_max_subcolumns_count'='1')>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
    sql "DROP TABLE IF EXISTS variant_light_pending_alters_control"
    sql """CREATE TABLE variant_light_pending_alters_control(id INT,
        v VARIANT<MATCH_NAME 'a':INT, MATCH_NAME 'b':STRING,
            PROPERTIES('variant_max_subcolumns_count'='2')>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
    def pendingRows = [
        '''(1,parse_to_variant('{"a":"001","b":10,"keep":"k1"}'))''',
        '''(2,parse_to_variant('{"a":"bad","b":20,"keep":"k2"}'))''',
        '''(3,parse_to_variant('{"a":2,"b":"30","keep":"k3"}'))'''
    ]
    sql "INSERT INTO variant_light_pending_alters VALUES ${pendingRows.take(2).join(', ')}"
    ["VARIANT<MATCH_NAME 'a':INT, PROPERTIES('variant_max_subcolumns_count'='1')>",
     "VARIANT<MATCH_NAME 'a':INT, MATCH_NAME 'b':STRING, PROPERTIES('variant_max_subcolumns_count'='1')>",
     "VARIANT<MATCH_NAME 'a':INT, MATCH_NAME 'b':STRING, PROPERTIES('variant_max_subcolumns_count'='2')>"].each { type ->
        sql "ALTER TABLE variant_light_pending_alters MODIFY COLUMN v ${type}"
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_pending_alters' ORDER BY CreateTime DESC LIMIT 1"
            time 30
        }
    }
    order_qt_pending_mixed "SELECT id, v FROM variant_light_pending_alters"
    sql "INSERT INTO variant_light_pending_alters VALUES ${pendingRows[2]}"
    sql "INSERT INTO variant_light_pending_alters_control VALUES ${pendingRows.join(', ')}"
    trigger_and_wait_compaction('variant_light_pending_alters', 'full')
    order_qt_pending_compacted "SELECT id, v['a'], v['b'], v FROM variant_light_pending_alters"
    order_qt_pending_compacted_control "SELECT id, v['a'], v['b'], v FROM variant_light_pending_alters_control"
    order_qt_pending_difference """SELECT 'altered_only', x.* FROM (
        SELECT id, CAST(v AS STRING) FROM variant_light_pending_alters
        EXCEPT SELECT id, CAST(v AS STRING) FROM variant_light_pending_alters_control) x
        UNION ALL SELECT 'control_only', x.* FROM (
        SELECT id, CAST(v AS STRING) FROM variant_light_pending_alters_control
        EXCEPT SELECT id, CAST(v AS STRING) FROM variant_light_pending_alters) x"""
    order_qt_pending_whole_path_invariant """SELECT id FROM variant_light_pending_alters
        WHERE NOT (CAST(v['a'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.a'))
           OR NOT (CAST(v['b'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.b'))"""

}
