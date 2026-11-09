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

suite("test_variant_light_max_subcolumns", "p0,nonConcurrent") {
    def rows = [
        """(1, parse_to_variant('{"a":1,"b":2,"c":3,"d":4}'))""",
        """(2, parse_to_variant('{"a":5,"b":6,"c":7,"d":8}'))""",
        """(3, parse_to_variant('{"a":1,"b":2,"c":3,"d":4}'))""",
        """(4, parse_to_variant('{"a":5,"b":6,"c":7,"d":8}'))""",
        """(5, parse_to_variant('{"a":1,"b":2,"c":3,"d":4}'))""",
        """(6, parse_to_variant('{"a":5,"b":6,"c":7,"d":8}'))""",
        """(7, parse_to_variant('{"a":1,"b":2,"c":3,"d":4}'))""",
        """(8, parse_to_variant('{"a":5,"b":6,"c":7,"d":8}'))"""
    ]
    def compareWithControl = { String tag, String cols, String where = "" ->
        // Compare independent extraction from the whole row, not another subpath read.
        def inconsistent = ['a', 'b', 'c', 'd'].collect { path ->
            "NOT (CAST(v['${path}'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.${path}'))"
        }.join(' OR ')
        "order_qt_${tag}_whole_path_invariant"("SELECT id FROM variant_light_max_subcolumns WHERE ${inconsistent}")
        def query = { String table, String projection -> "SELECT ${projection} FROM ${table} ${where}" }
        "order_qt_${tag}"(query("variant_light_max_subcolumns", cols))
        "order_qt_${tag}_control"(query("variant_light_max_subcolumns_control", cols))
        def normalized = cols.split(", ").toList().withIndex().collect { col, i -> "CAST(${col} AS STRING) AS c${i}" }.join(", ")
        def actual = query("variant_light_max_subcolumns", normalized), expected = query("variant_light_max_subcolumns_control", normalized)
        "order_qt_${tag}_difference"("""SELECT 'altered_only', delta.* FROM (${actual} EXCEPT ${expected}) delta
            UNION ALL SELECT 'control_only', delta.* FROM (${expected} EXCEPT ${actual}) delta""")
    }
    def reloadControl = { int count ->
        // Template coercion is batch-wide: load all current rows in one INSERT.
        sql "TRUNCATE TABLE variant_light_max_subcolumns_control"
        sql "INSERT INTO variant_light_max_subcolumns_control VALUES ${rows.take(count).join(', ')}"
    }
    def alterAndWait = { String change, boolean checkImmediately = false ->
        sql "ALTER TABLE variant_light_max_subcolumns ${change}"
        def job = "SHOW ALTER TABLE COLUMN WHERE TableName='variant_light_max_subcolumns' ORDER BY CreateTime DESC LIMIT 1"
        if (checkImmediately) { assertEquals("FINISHED", sql(job)[0][9]) }
        waitForSchemaChangeDone { sql job; time 30 }
    }
    sql "DROP TABLE IF EXISTS variant_light_max_subcolumns"
    sql """CREATE TABLE variant_light_max_subcolumns (id INT, v VARIANT<PROPERTIES("variant_max_subcolumns_count"="2")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql "DROP TABLE IF EXISTS variant_light_max_subcolumns_control"
    sql """CREATE TABLE variant_light_max_subcolumns_control (id INT, v VARIANT<PROPERTIES("variant_max_subcolumns_count"="1")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    def queries = [whole: ["id, v", ""], paths: ["id, v['a'], v['b'], v['c'], v['d']", ""],
        materialized: ["id", "WHERE v['a'] = 5"], sparse: ["id", "WHERE v['d'] > 5"]]
    sql "INSERT INTO variant_light_max_subcolumns VALUES ${rows.take(2).join(', ')}"
    [10, 0, 1].eachWithIndex { budget, round ->
        alterAndWait("MODIFY COLUMN v VARIANT<PROPERTIES('variant_max_subcolumns_count'='${budget}')>", round == 0)
        sql "INSERT INTO variant_light_max_subcolumns VALUES ${rows[(2 + 2 * round)..(3 + 2 * round)].join(', ')}"
        queries.each { tag, q ->
            "order_qt_budget_${budget}_mixed_${tag}"("SELECT ${q[0]} FROM variant_light_max_subcolumns ${q[1]}")
        }
        reloadControl(4 + 2 * round)
        trigger_and_wait_compaction("variant_light_max_subcolumns", "full")
        queries.each { tag, q -> compareWithControl("budget_${budget}_compacted_${tag}", q[0], q[1]) }
    }
}
