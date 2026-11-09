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

suite("test_variant_light_max_subcolumns_matrix", "p0,nonConcurrent") {
    def rows = []
    def createTables = { int count, int stats = 10000 ->
        sql "DROP TABLE IF EXISTS variant_light_budget_matrix"
        sql """CREATE TABLE variant_light_budget_matrix (id INT,
            v VARIANT<PROPERTIES('variant_max_subcolumns_count'='${count}',
                'variant_max_sparse_column_statistics_size'='${stats}')>)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
        sql "DROP TABLE IF EXISTS variant_light_budget_matrix_control"
        sql """CREATE TABLE variant_light_budget_matrix_control (id INT,
            v VARIANT<PROPERTIES('variant_max_subcolumns_count'='${count}',
                'variant_max_sparse_column_statistics_size'='${stats}')>)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true')"""
        rows.clear()
    }
    def alterAndWait = { String table, int count, int stats = 10000 ->
        sql """ALTER TABLE ${table} MODIFY COLUMN v VARIANT<PROPERTIES(
            'variant_max_subcolumns_count'='${count}', 'variant_max_sparse_column_statistics_size'='${stats}')>"""
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='${table}' ORDER BY CreateTime DESC LIMIT 1"
            time 30
        }
    }
    def row = { int id, int paths ->
        def json = (0..<paths).collect { p -> "\"p${p}\":${id * 100 + p}" }.join(',')
        "(${id},parse_to_variant('{${json}}'))"
    }
    def insertRows = { List batch ->
        rows.addAll(batch)
        sql "INSERT INTO variant_light_budget_matrix VALUES ${batch.join(', ')}"
    }
    def reloadControl = { int count, int stats = 10000 ->
        alterAndWait('variant_light_budget_matrix_control', count, stats)
        sql "TRUNCATE TABLE variant_light_budget_matrix_control"
        sql "INSERT INTO variant_light_budget_matrix_control VALUES ${rows.join(', ')}"
    }
    def compareWithControl = { String tag, String cols, boolean randomPart = false, long seed = 0 ->
        def inconsistent = (0..31).collect { path ->
            "NOT (CAST(v['p${path}'] AS STRING) <=> get_json_string(CAST(v AS STRING), '\$.p${path}'))"
        }.join(' OR ')
        // Even for a random walk, the expected result is deterministically empty.
        "order_qt_${tag}_whole_path_invariant"("SELECT id FROM variant_light_budget_matrix WHERE ${inconsistent}")
        def normalized = cols.split(', ').toList().withIndex().collect { col, i -> "CAST(${col} AS STRING) AS c${i}" }.join(', ')
        def actual = "SELECT ${normalized} FROM variant_light_budget_matrix"
        def expected = "SELECT ${normalized} FROM variant_light_budget_matrix_control"
        def diff = """SELECT 'altered_only', delta.* FROM (${actual} EXCEPT ${expected}) delta
            UNION ALL SELECT 'control_only', delta.* FROM (${expected} EXCEPT ${actual}) delta"""
        if (randomPart) {
            // Random data and step choices never enter the golden output.
            assertTrue(sql(diff).isEmpty(), "seed=${seed}, step=${tag}")
        } else {
            "order_qt_${tag}"("SELECT ${cols} FROM variant_light_budget_matrix")
            "order_qt_${tag}_control"("SELECT ${cols} FROM variant_light_budget_matrix_control")
            "order_qt_${tag}_difference"(diff)
        }
    }
    createTables(0)
    insertRows([row(1,20), row(2,20)])
    alterAndWait('variant_light_budget_matrix', 3)
    insertRows([row(3,22)])
    order_qt_unlimited_mixed "SELECT id, v FROM variant_light_budget_matrix"
    reloadControl(3)
    trigger_and_wait_compaction('variant_light_budget_matrix', 'full')
    compareWithControl('unlimited_to_three', "id, v, v['p19'], v['p21']")

    def seed = System.currentTimeMillis()
    logger.info("seed=${seed}")
    def random = new Random(seed)
    def budgets = [0,1,2,5,50]
    int previous = 3
    (0..<8).each { step ->
        def choices = budgets.findAll { it != previous }
        int count = choices[random.nextInt(choices.size())]
        alterAndWait('variant_light_budget_matrix', count)
        // Six independent rowsets exceed the default cumulative_compaction_min_deltas=5.
        // Every row retains old paths and adds paths specific to this step.
        (0..<6).each { offset -> insertRows([row(step * 6 + offset + 4, 24 + step + offset)]) }
        reloadControl(count)
        trigger_and_wait_compaction('variant_light_budget_matrix', step % 2 == 0 ? 'cumulative' : 'full')
        compareWithControl("walk_${step}", "id, v, v['p0'], v['p${23 + step}']", true, seed)
        previous = count
    }

    // More sparse paths than statistics slots: omitted dictionary entries must survive.
    createTables(2,4)
    insertRows([row(1,30), row(2,30)])
    alterAndWait('variant_light_budget_matrix', 0,4)
    insertRows([row(3,32)])
    order_qt_truncated_mixed "SELECT id, v FROM variant_light_budget_matrix"
    reloadControl(0,4)
    trigger_and_wait_compaction('variant_light_budget_matrix', 'full')
    compareWithControl('truncated', "id, v, v['p7'], v['p19'], v['p29']")

    // An old-only cumulative compaction can link old segments; its values must remain intact.
    createTables(3)
    (1..6).each { id -> insertRows([row(id,20)]) }
    order_qt_old_only_before "SELECT id, v FROM variant_light_budget_matrix"
    alterAndWait('variant_light_budget_matrix', 0)
    trigger_and_wait_compaction('variant_light_budget_matrix', 'cumulative')
    order_qt_old_only_after "SELECT id, v FROM variant_light_budget_matrix"
    insertRows([row(7,22)])
    reloadControl(0)
    trigger_and_wait_compaction('variant_light_budget_matrix', 'full')
    compareWithControl('old_only_converged', "id, v, v['p19'], v['p21']")
}
