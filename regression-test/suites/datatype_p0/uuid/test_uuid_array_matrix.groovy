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

// All constant/column masks, with FE folding, BE folding and folding disabled.
suite("test_uuid_array_matrix", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "DROP TABLE IF EXISTS uuid_matrix_array"
    sql """CREATE TABLE uuid_matrix_array (${matrix.schema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_array VALUES ${matrix.values()}"

    matrix.run(delegate, 'unary', 'uuid_matrix_array', ['a'], { a ->
        [sorted: "ARRAY_SORT(${a})", reverse_sorted: "ARRAY_REVERSE_SORT(${a})",
         distinct_values: "ARRAY_SORT(ARRAY_DISTINCT(${a}))", compact_values: "ARRAY_COMPACT(${a})",
         minimum: "ARRAY_MIN(${a})", maximum: "ARRAY_MAX(${a})", pop_back: "ARRAY_POPBACK(${a})",
         pop_front: "ARRAY_POPFRONT(${a})", reversed: "REVERSE(${a})", size_value: "SIZE(${a})",
         cardinality_value: "CARDINALITY(${a})", enumerate_value: "ARRAY_ENUMERATE(${a})",
         enumerate_uniq: "ARRAY_ENUMERATE_UNIQ(${a})", json_value: "TO_JSON(${a})",
         mapped: "ARRAY_MAP(x -> UUID_VERSION(x),${a})", filtered: "ARRAY_FILTER(x -> x IS NOT NULL,${a})",
         first_value: "ARRAY_FIRST(x -> x IS NOT NULL,${a})", last_value: "ARRAY_LAST(x -> x IS NOT NULL,${a})",
         first_index: "ARRAY_FIRST_INDEX(x -> x IS NOT NULL,${a})", last_index: "ARRAY_LAST_INDEX(x -> x IS NOT NULL,${a})",
         shuffled_members: "ARRAY_SORT(ARRAY_SHUFFLE(${a}))",
         count_matches: "ARRAY_COUNT(x -> x IS NOT NULL,${a})", exists_match: "ARRAY_EXISTS(x -> x IS NOT NULL,${a})",
         any_match: "ARRAY_MATCH_ANY(x -> x IS NOT NULL,${a})", all_match: "ARRAY_MATCH_ALL(x -> x IS NOT NULL,${a})",
         flattened: "ARRAY_FLATTEN(ARRAY(${a},${a}))"]
    })
    matrix.run(delegate, 'sets', 'uuid_matrix_array', ['a','a2'], { a,b ->
        [union_values: "ARRAY_SORT(ARRAY_UNION(${a},${b}))", intersection: "ARRAY_SORT(ARRAY_INTERSECT(${a},${b}))",
         except_values: "ARRAY_SORT(ARRAY_EXCEPT(${a},${b}))", except_all: "ARRAY_SORT(ARRAY_EXCEPT_ALL(${a},${b}))",
         overlap_value: "ARRAYS_OVERLAP(${a},${b})", contains_all: "ARRAY_CONTAINS_ALL(${a},${b})",
         concatenated: "ARRAY_CONCAT(${a},${b})"]
    })
    matrix.run(delegate, 'seed', 'uuid_matrix_array', ['a','num'], { a,n ->
        [shuffled_members: "ARRAY_SORT(ARRAY_SHUFFLE(${a},${n}))"]
    })
    matrix.run(delegate, 'aligned', 'uuid_matrix_array', ['a','a2'], { a,b ->
        [sort_by: "ARRAY_SORTBY(${a},${b})", zipped: "ARRAY_ZIP(${a},${b})",
         enumerate_uniq: "ARRAY_ENUMERATE_UNIQ(${a},${b})",
         mapped: "ARRAY_MAP((x,y) -> COALESCE(x,y),${a},${b})"]
    }, [aligned: true])
    matrix.run(delegate, 'split', 'uuid_matrix_array', ['a','flags'], { a,b ->
        [split_values: "ARRAY_SPLIT(${a},${b})", reverse_split: "ARRAY_REVERSE_SPLIT(${a},${b})",
         filtered: "ARRAY_FILTER(${a},${b})"]
    }, [aligned: true])
    for (String function : ['EXPLODE','EXPLODE_OUTER']) {
        matrix.run(delegate, function.toLowerCase(), 'uuid_matrix_array', ['a'], { a -> [element: 'e'] },
            [lateral: { a -> "LATERAL VIEW ${function}(${a}) generated AS e" }, orderBy: 'id,e'])
    }
    for (String function : ['POSEXPLODE','POSEXPLODE_OUTER']) {
        matrix.run(delegate, function.toLowerCase(), 'uuid_matrix_array', ['a'], { a -> [position: 'pos', element: 'e'] },
            [lateral: { a -> "LATERAL VIEW ${function}(${a}) generated AS pos,e" }, orderBy: 'id,pos'])
    }

}
