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

suite("topn_runtime_filter_strict_paths") {
    sql "DROP TABLE IF EXISTS topn_strict_probe"
    sql """CREATE TABLE topn_strict_probe (id INT NOT NULL, k INT NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_strict_probe VALUES (1,1),(2,1)"
    sql "DROP TABLE IF EXISTS topn_strict_build"
    sql """CREATE TABLE topn_strict_build (id INT NULL, k INT NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql "INSERT INTO topn_strict_build VALUES (1,1)"

    // Two probe rows are the minimum for LIMIT 1 to encounter another boundary tie.
    // A single build row suffices for the hash/nested-loop cases below. Use a nullable
    // schema so IN/NOT IN also exercise the null-aware join implementations.
    // Prevent rewrites from moving TopN below the operator whose safety is under test.
    sql "SET disable_join_reorder=true"
    sql "SET disable_nereids_rules='PUSH_DOWN_TOP_N_THROUGH_JOIN,PUSH_DOWN_TOP_N_THROUGH_UNION'"
    sql "SET force_sort_algorithm='heap'"
    sql "SET parallel_pipeline_task_num=1"
    sql "SET batch_size=1"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"

    // Pin the eligible plan and verify its SQL semantics. With minimal data a scanner
    // can finish before the asynchronous bound arrives, so do not assert a filtered-row
    // count here. FE/BE unit tests separately check strict target IDs and LT/GT equality;
    // topn_runtime_filter_safety covers an already-published bound across a rebuild.
    def checkQuery = { String name, String query, List<String> operators, int targets = 1 ->
        sql "SET topn_filter_ratio=0"
        "order_qt_${name}_disabled"(query)
        sql "SET topn_filter_ratio=1000000"
        explain {
            sql query
            verbose true
            contains "algorithm: heap sort"
            operators.each { contains it }
            multiContains "TOPN OPT:", targets
        }
        "order_qt_${name}_enabled"(query)
    }

    // Do not add id/mark/payload as a second ordering key: that would select inclusive
    // bounds and defeat this test. Instead check that whichever tie wins is a complete,
    // legal row. k=1 and valid_row=true are deterministic even when the winning id is not.
    def checkedTop1 = { String input, String valid ->
        "SELECT k, (${valid}) AS valid_row FROM (${input} ORDER BY k NULLS LAST LIMIT 1) s"
    }

    for (String implementation : ["hash", "nested_loop"]) {
        boolean hash = implementation == "hash"
        String node = hash ? "VHASH JOIN" : "VNESTED LOOP JOIN"
        String bothMatch = hash ? "a.k=b.k" : "a.id>=b.id"
        String oneMatches = hash ? "a.id=b.id" : "a.id<b.id+1"
        String neitherMatches = hash ? "a.id=b.id+2" : "a.id<b.id"

        // Both probes match the same build row. Removing a later probe tie may remove
        // its joined row, but must not change the payload of the retained row.
        checkQuery("${implementation}_inner", checkedTop1("""SELECT a.k,a.id,b.id AS matched
            FROM topn_strict_probe a INNER JOIN topn_strict_build b ON ${bothMatch}""",
                "id IN (1,2) AND matched <=> 1"), [node, "INNER JOIN"])

        // id=1 matches; id=2 is null-extended. A legal Top1 can be either (1,1,1) or
        // (1,2,NULL), never an invented null-extended id=1 or a matched id=2.
        // NULLS LAST is required: existing outer-join pushdown rejects NULLS FIRST.
        checkQuery("${implementation}_left_outer", checkedTop1("""SELECT a.k,a.id,b.id AS matched
            FROM topn_strict_probe a LEFT OUTER JOIN topn_strict_build b ON ${oneMatches}""",
                "(id=1 AND matched <=> 1) OR (id=2 AND matched IS NULL)"),
                [node, "LEFT OUTER JOIN"])

        // A semi join keeps both probes, without multiplying them by build matches.
        // An anti join with no matches also keeps both probes. These are separate
        // whitelist entries; a right-anti plan would have different removal semantics.
        checkQuery("${implementation}_left_semi", """SELECT a.k FROM topn_strict_probe a
            LEFT SEMI JOIN topn_strict_build b ON ${bothMatch}
            ORDER BY a.k NULLS LAST LIMIT 1""", [node, "LEFT SEMI JOIN"])
        checkQuery("${implementation}_left_anti", """SELECT a.k FROM topn_strict_probe a
            LEFT ANTI JOIN topn_strict_build b ON ${neitherMatches}
            ORDER BY a.k NULLS LAST LIMIT 1""", [node, "LEFT ANTI JOIN"])
    }

    // The minimum nonempty Cartesian product with a boundary tie is 2 x 1 rows.
    checkQuery("cross", checkedTop1("""SELECT a.k,a.id,b.id AS matched
        FROM topn_strict_probe a CROSS JOIN topn_strict_build b""",
            "id IN (1,2) AND matched <=> 1"), ["VNESTED LOOP JOIN", "CROSS JOIN"])

    // Local exchanges are added AFTER ordinary EXPLAIN's plan is built. Two pipeline
    // tasks and a shuffle join put one below TopN, without requiring extra input rows.
    // Disable the small-LIMIT serial-read shortcut only for this query. Inspect the
    // distributed plan so a single-task execution cannot silently miss this path.
    String localExchangeQuery = """SELECT /*+ SET_VAR(parallel_pipeline_task_num=2,
        force_to_local_shuffle=true,enable_adaptive_pipeline_task_serial_read_on_limit=false) */ a.k
        FROM topn_strict_probe a JOIN [shuffle] topn_strict_build b ON a.k=b.k
        ORDER BY a.k NULLS LAST LIMIT 1"""
    checkQuery("local_exchange", localExchangeQuery, ["INNER JOIN(PARTITIONED)"])
    explain {
        sql "distributed plan ${localExchangeQuery}"
        contains "VLOCAL-EXCHANGE"
        contains "TOPN OPT:"
    }

    // EXISTS/NOT EXISTS use ordinary left semi/anti mark joins rather than the
    // filtering variants above. Both probe rows survive, carrying different marks;
    // rejecting a later tie must not change the mark attached to the retained id.
    for (String existence : ["EXISTS", "NOT EXISTS"]) {
        String mark = existence == "EXISTS" ? "id=1" : "id=2"
        String kind = existence == "EXISTS" ? "SEMI" : "ANTI"
        checkQuery("left_${kind.toLowerCase()}_mark", checkedTop1("""SELECT a.k,a.id,
            ${existence} (SELECT 1 FROM topn_strict_build b WHERE a.id=b.id) AS m
            FROM topn_strict_probe a""", "m <=> (${mark})"),
                ["join op: LEFT ${kind} JOIN", "isMarkJoin: true"])
    }

    // Nullable IN emits a NULL AWARE LEFT SEMI mark join. Its true/false mark is part
    // of the retained row and must remain paired with the original probe id.
    // NOT IN in the projection similarly covers the null-aware anti mark variant.
    for (String membership : ["IN", "NOT IN"]) {
        String mark = membership == "IN" ? "id=1" : "id=2"
        String kind = membership == "IN" ? "SEMI" : "ANTI"
        checkQuery("null_aware_${kind.toLowerCase()}_mark", checkedTop1("""SELECT a.k,a.id,
            a.id ${membership} (SELECT id FROM topn_strict_build) AS m
            FROM topn_strict_probe a""", "m <=> (${mark})"),
                ["NULL AWARE LEFT ${kind} JOIN", "isMarkJoin: true"])
    }
    // Both 1s are absent from {2}; the nullable schemas retain the null-aware anti
    // implementation even though this particular data set has no NULL values.
    checkQuery("null_aware_anti", """SELECT a.k FROM topn_strict_probe a
        WHERE a.k NOT IN (SELECT k+1 FROM topn_strict_build)
        ORDER BY a.k NULLS LAST LIMIT 1""", ["NULL AWARE LEFT ANTI JOIN"])

    // Two distinct rows with the same ordering key must survive INTERSECT's set
    // membership check. Reuse the same two stored rows for both inputs. Only the
    // first input is a filter target; filtering the membership side is not permitted.
    checkQuery("intersect", checkedTop1("""SELECT k,id FROM (
        SELECT k,id FROM topn_strict_probe INTERSECT SELECT k,id FROM topn_strict_probe
        ) u""", "id IN (1,2)"), ["VINTERSECT"])

    // Each input row expands to one output with its own payload. Dropping a boundary
    // input may discard its expansion, but cannot change the other input's expansion.
    // Two one-element arrays are enough; no large exploded array is needed.
    checkQuery("table_function", checkedTop1("""SELECT k,id,e FROM topn_strict_probe
        LATERAL VIEW explode(array(id)) x AS e""", "e <=> id"), ["VTABLE FUNCTION NODE"])

    // Lazy materialization must retain the row id belonging to the chosen key. This
    // tests the lazy scan target; the top materialization node is ABOVE its source TopN.
    checkQuery("lazy_scan", checkedTop1("SELECT k,id FROM topn_strict_probe", "id IN (1,2)"),
            ["VMaterializeNode"])
    // Repeated ORDER BY expressions collapse to one effective key and remain eligible.
    checkQuery("duplicate_order_key", """SELECT k FROM topn_strict_probe
        ORDER BY k,k LIMIT 1""", ["VTOP-N"])

    // Keep the unlimited inner SortNode. This also puts remote Exchange nodes BETWEEN
    // the source TopN and its targets: FE must traverse their sending fragments.
    // Unlike an inner LIMIT, sorting all rows does not make a discarded tie affect
    // membership. Both UNION inputs read the same two-row fixture.
    sql """SET disable_nereids_rules='PUSH_DOWN_TOP_N_THROUGH_JOIN,PUSH_DOWN_TOP_N_THROUGH_UNION,
        ELIMINATE_SORT,PUSH_DOWN_FILTER_THROUGH_SET_OPERATION'"""
    checkQuery("sort_exchange_union", checkedTop1("""SELECT k,id FROM (
        (SELECT k,id FROM topn_strict_probe ORDER BY id DESC)
        UNION ALL SELECT k,id FROM topn_strict_probe) u""", "id IN (1,2)"),
            ["VSORT", "VEXCHANGE", "VUNION"], 2)
    // Disabling filter pushdown leaves an actual SelectNode above UNION. id>0 keeps
    // both boundary candidates; the strict path must pass through this selection.
    checkQuery("select", checkedTop1("""SELECT k,id FROM (
        SELECT k,id FROM topn_strict_probe UNION ALL SELECT k,id FROM topn_strict_probe
        ) u WHERE id>0""", "id IN (1,2)"), ["VSELECT", "VUNION"], 2)

    // ASOF uses timestamps, not integer match conditions. Two probes with equal k
    // and two build timestamps are the minimum that also distinguish the nearest
    // match from an older match. For probe id=2, the correct build id is 2, not 1.
    sql "INSERT INTO topn_strict_build VALUES (2,1)"
    String asofCondition = """MATCH_CONDITION(
        date_add(CAST('2020-01-01' AS DATE), INTERVAL a.id DAY) >=
        date_add(CAST('2020-01-01' AS DATE), INTERVAL b.id DAY)) ON a.k=b.k"""
    checkQuery("asof_inner", checkedTop1("""SELECT a.k,a.id,b.id AS matched
        FROM topn_strict_probe a ASOF INNER JOIN topn_strict_build b ${asofCondition}""",
            "matched <=> id"), ["ASOF_LEFT_INNER_JOIN"])
    // Replace the build side with its single id=2 row. Probe 1 has no earlier match,
    // while probe 2 matches exactly: dropping a later tie must not switch either result.
    sql "TRUNCATE TABLE topn_strict_build"
    sql "INSERT INTO topn_strict_build VALUES (2,1)"
    checkQuery("asof_left", checkedTop1("""SELECT a.k,a.id,b.id AS matched
        FROM topn_strict_probe a ASOF LEFT JOIN topn_strict_build b ${asofCondition}""",
            "(id=1 AND matched IS NULL) OR (id=2 AND matched <=> 2)"), ["ASOF_LEFT_OUTER_JOIN"])

    // A single NULL build value is sufficient to make every nonmatching IN mark
    // UNKNOWN. Check the complete mark rather than only the projected ordering key.
    sql "TRUNCATE TABLE topn_strict_build"
    sql "INSERT INTO topn_strict_build VALUES (NULL,NULL)"
    for (String membership : ["IN", "NOT IN"]) {
        String kind = membership == "IN" ? "SEMI" : "ANTI"
        checkQuery("null_aware_${kind.toLowerCase()}_unknown", checkedTop1("""SELECT a.k,a.id,
            a.id ${membership} (SELECT id FROM topn_strict_build) AS m
            FROM topn_strict_probe a""", "m IS NULL"),
                ["NULL AWARE LEFT ${kind} JOIN", "isMarkJoin: true"])
    }

    // EXCEPT needs three distinct input rows: removing one set member still leaves
    // two boundary ties. Reusing that table for the membership side avoids extra data.
    sql "INSERT INTO topn_strict_probe VALUES (3,1)"
    checkQuery("except", checkedTop1("""SELECT k,id FROM (
        SELECT k,id FROM topn_strict_probe EXCEPT
        SELECT k,id FROM topn_strict_probe WHERE id=1) u""", "id IN (2,3)"), ["VEXCEPT"])
    // LIMIT 1 OFFSET 1 needs two heap candidates plus one later tie: exactly three
    // rows. A prematurely published strict boundary must not leave too few candidates.
    checkQuery("offset", """SELECT k FROM topn_strict_probe
        ORDER BY k NULLS LAST LIMIT 1 OFFSET 1""", ["VTOP-N"])
}
