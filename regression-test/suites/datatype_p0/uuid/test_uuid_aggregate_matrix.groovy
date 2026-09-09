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

suite("test_uuid_aggregate_matrix", "p0") {
    sql "DROP TABLE IF EXISTS uuid_matrix_aggregate"
    sql """CREATE TABLE uuid_matrix_aggregate (${uuidMatrixSchema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_aggregate VALUES ${uuidMatrixValues()}"

    for (int phase : [1,2]) {
        sql "SET agg_phase=${phase}"
        uuidRunMatrix("unary_p${phase}", 'uuid_matrix_aggregate', ['u'], { u ->
            [minimum: "MIN(${u})", maximum: "MAX(${u})", count_value: "COUNT(${u})",
             distinct_count: "COUNT(DISTINCT ${u})", multi_distinct: "MULTI_DISTINCT_COUNT(${u})", ndv_value: "NDV(${u})",
             collected: "ARRAY_SORT(COLLECT_LIST(${u}))", collected_set: "ARRAY_SORT(COLLECT_SET(${u}))",
             array_agg_value: "ARRAY_SORT(ARRAY_AGG(${u}))", histogram_value: "HISTOGRAM(${u})", histogram_param: "HISTOGRAM(${u},16)",
             topn_values: "ARRAY_SORT(TOPN_ARRAY(${u},100))", topn_expanded: "ARRAY_SORT(TOPN_ARRAY(${u},100,200))",
             collect_limited: "ARRAY_SORT(COLLECT_LIST(${u},16))", set_limited: "ARRAY_SORT(COLLECT_SET(${u},16))",
             list_zero: "COLLECT_LIST(${u},0)", set_zero: "COLLECT_SET(${u},0)",
             list_unlimited: "ARRAY_SORT(COLLECT_LIST(${u},-1))", set_unlimited: "ARRAY_SORT(COLLECT_SET(${u},-1))",
             list_null_limit: "COLLECT_LIST(${u},NULL)", set_null_limit: "COLLECT_SET(${u},NULL)",
             histogram_null: "HISTOGRAM(${u},NULL)", topn_null: "TOPN_ARRAY(${u},NULL)",
             topn_zero_capacity: "TOPN_ARRAY(${u},100,0)", topn_null_capacity: "TOPN_ARRAY(${u},100,NULL)",
             any_member: "IF(COUNT(${u})=0,ANY_VALUE(${u}) IS NULL,ARRAY_CONTAINS(COLLECT_SET(${u}),ANY_VALUE(${u})))"]
        }, [aggregate:true])
        uuidRunMatrix("binary_p${phase}", 'uuid_matrix_aggregate', ['u','v'], { u,v ->
            // Encode pairs only in the oracle; MIN_BY/MAX_BY still consume native UUIDs.
            String pairs = "COLLECT_LIST(CONCAT(IFNULL(CAST(${u} AS STRING),'NULL'),':',IFNULL(CAST(${v} AS STRING),'NULL')))"
            String minimum = "CONCAT(IFNULL(CAST(MIN_BY(${u},${v}) AS STRING),'NULL'),':',CAST(MIN(${v}) AS STRING))"
            String maximum = "CONCAT(IFNULL(CAST(MAX_BY(${u},${v}) AS STRING),'NULL'),':',CAST(MAX(${v}) AS STRING))"
            [distinct_pair: "COUNT(DISTINCT ${u},${v})",
             min_by_pair: "IF(COUNT(${v})=0,MIN_BY(${u},${v}) IS NULL,ARRAY_CONTAINS(${pairs},${minimum}))",
             max_by_pair: "IF(COUNT(${v})=0,MAX_BY(${u},${v}) IS NULL,ARRAY_CONTAINS(${pairs},${maximum}))"]
        }, [aggregate:true])
        // A constant key and several different values have an unspecified winning value.
        // Match one input row for mixed masks; all-column input has unique UUID keys.
        uuidRunMatrix("map_p${phase}", 'uuid_matrix_aggregate', ['u','v'], { u,v ->
            String pairs = "ARRAY_SORT(ARRAY_MAP(e -> CONCAT(IFNULL(CAST(e[1] AS STRING),'NULL'),':'," +
                           "IFNULL(CAST(e[2] AS STRING),'NULL')),MAP_ENTRIES(MAP_AGG(${u},${v}))))"
            [entries: pairs, keys: "ARRAY_SORT(MAP_KEYS(MAP_AGG(${u},${v})))", vals: "ARRAY_SORT(MAP_VALUES(MAP_AGG(${u},${v})))",
             map_v2_keys: "ARRAY_SORT(MAP_KEYS(MAP_AGG_V2(${u},${v})))",
             map_v2_vals: "ARRAY_SORT(MAP_VALUES(MAP_AGG_V2(${u},${v})))"]
        }, [aggregate:true,aligned:true])
        uuidRunMatrix("arrays_p${phase}", 'uuid_matrix_aggregate', ['a'], { a ->
            [union_values: "ARRAY_SORT(GROUP_ARRAY_UNION(${a}))", intersection: "ARRAY_SORT(GROUP_ARRAY_INTERSECT(${a}))"]
        }, [aggregate:true])
    }
    // Unlike COLLECT_LIST, COLLECT_SET accepts a column limit. Keep it constant within
    // each group and verify cardinality plus membership without fixing arbitrary winners.
    uuidRunMatrix('set_limit', 'uuid_matrix_aggregate', ['u','num'], { u,n ->
        [size_matches: "SIZE(COLLECT_SET(${u},${n})) = LEAST(IFNULL(${n},0),COUNT(DISTINCT ${u}))",
         members_match: "ARRAY_CONTAINS_ALL(COLLECT_SET(${u}),COLLECT_SET(${u},${n}))"]
    }, [aggregate:true,groupBy:'num'])
    sql "SET enable_agg_state=true"
    // Keep the aggregate-state signature nullable while its constant child folds.
    // Raw non-nullable constant states hit a pre-existing generic MERGE rewrite bug,
    // also reproducible with INT; that optimizer fix is outside the UUID change.
    uuidRunMatrix('state', 'uuid_matrix_aggregate', ['u'], { u ->
        u = "NULLABLE(${u})"
        [max_merge: "MAX_MERGE(MAX_STATE(${u}))", min_merge: "MIN_MERGE(MIN_STATE(${u}))",
         count_merge: "COUNT_MERGE(COUNT_STATE(${u}))"]
    }, [aggregate:true])
    uuidRunMatrix('map_combinator', 'uuid_matrix_aggregate', ['m'], { m ->
        [min_keys: "ARRAY_SORT(MAP_KEYS(MIN_MAP(${m})))", min_vals: "ARRAY_SORT(MAP_VALUES(MIN_MAP(${m})))",
         max_keys: "ARRAY_SORT(MAP_KEYS(MAX_MAP(${m})))", max_vals: "ARRAY_SORT(MAP_VALUES(MAX_MAP(${m})))",
         count_keys: "ARRAY_SORT(MAP_KEYS(COUNT_MAP(${m})))", count_vals: "ARRAY_SORT(MAP_VALUES(COUNT_MAP(${m})))"]
    }, [aggregate:true])
    for (String predicate : ['id<0', 'u IS NULL']) {
        qt_empty_null """SELECT MIN(u),MAX(u),COUNT(u),COUNT(DISTINCT u),NDV(u),
                         COLLECT_LIST(u),COLLECT_SET(u),ARRAY_AGG(u),HISTOGRAM(u),MAP_AGG(u,v)
                         FROM uuid_matrix_aggregate WHERE ${predicate}"""
    }
    // A subquery carries the state type through slots before UNION/COMBINE -> MERGE.
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        List<String> inputs = uuidMatrixRows().collect { "NULLABLE(${it.u})" } + ['u']
        for (String input : inputs) {
            qt_state_union """SELECT MAX_MERGE(hi),MIN_MERGE(lo),COUNT_MERGE(n) FROM (
                SELECT MAX_UNION(MAX_STATE(${input})) hi,MIN_UNION(MIN_STATE(${input})) lo,
                       COUNT_UNION(COUNT_STATE(${input})) n
                FROM uuid_matrix_aggregate GROUP BY id%2) states"""
            qt_state_combine """SELECT MAX_MERGE(hi),MIN_MERGE(lo),COUNT_MERGE(n) FROM (
                SELECT MAX_COMBINE(${input}) hi,MIN_COMBINE(${input}) lo,COUNT_COMBINE(${input}) n
                FROM uuid_matrix_aggregate GROUP BY id%2) states"""
        }
    }
    // Parameter positions required by the signature to be constants are not column candidates.
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        for (int buckets : [1,3]) {
            qt_histogram "SELECT HISTOGRAM(u,${buckets}),HIST(u,${buckets}) FROM uuid_matrix_aggregate"
        }
        for (String query : ['HISTOGRAM(u,0)', 'HISTOGRAM(u,-1)', 'TOPN_ARRAY(u,0)', 'TOPN_ARRAY(u,-1)']) {
            test {
                sql "SELECT ${query} FROM uuid_matrix_aggregate"
                exception query.startsWith('HISTOGRAM') ? 'Invalid max_num_buckets' : 'constant positive integer'
            }
        }
        qt_foreach "SELECT MIN_FOREACH(a),MAX_FOREACH(a),COUNT_FOREACH(a) FROM uuid_matrix_aggregate"
        // FOREACH currently accepts stored array slots only, even for numeric element types.
        for (String input : uuidMatrixRows().collect { it.a }) {
            for (String function : ['MIN_FOREACH','MAX_FOREACH','COUNT_FOREACH']) {
                test {
                    sql "SELECT ${function}(${input}) FROM uuid_matrix_aggregate"
                    exception 'Can not build foreach nested function'
                }
            }
        }
        // Group per UUID: TOPN ties cannot make the expected array order arbitrary.
        qt_topn """SELECT u,TOPN_ARRAY(u,1),TOPN_ARRAY(u,2,4),COLLECT_LIST(u,2),COLLECT_SET(u,2)
                   FROM uuid_matrix_aggregate GROUP BY u ORDER BY u NULLS FIRST"""
        for (String query : ['HISTOGRAM(u,num)', 'TOPN_ARRAY(u,num)', 'COLLECT_LIST(u,num)']) {
            test {
                sql "SELECT ${query} FROM uuid_matrix_aggregate"
                exception "constant"
            }
        }
    }
}
