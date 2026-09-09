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
    matrix.run(delegate, 'element', 'uuid_matrix_array', ['a','u'], { a,u ->
        [contains_value: "ARRAY_CONTAINS(${a},${u})", position_value: "ARRAY_POSITION(${a},${u})",
         removed: "ARRAY_REMOVE(${a},${u})", pushed_back: "ARRAY_PUSHBACK(${a},${u})",
         pushed_front: "ARRAY_PUSHFRONT(${a},${u})", appended: "ARRAY_APPEND(${a},${u})", count_equal: "COUNTEQUAL(${a},${u})"]
    })
    matrix.run(delegate, 'capture', 'uuid_matrix_array', ['a','u'], { a,u ->
        [mapped: "ARRAY_MAP(x -> COALESCE(x,${u}),${a})", filtered: "ARRAY_FILTER(x -> x <=> ${u},${a})",
         count_matches: "ARRAY_COUNT(x -> x <=> ${u},${a})", exists_match: "ARRAY_EXISTS(x -> x <=> ${u},${a})",
         any_match: "ARRAY_MATCH_ANY(x -> x <=> ${u},${a})", all_match: "ARRAY_MATCH_ALL(x -> x <=> ${u},${a})",
         first_value: "ARRAY_FIRST(x -> x <=> ${u},${a})", last_value: "ARRAY_LAST(x -> x <=> ${u},${a})",
         first_index: "ARRAY_FIRST_INDEX(x -> x <=> ${u},${a})", last_index: "ARRAY_LAST_INDEX(x -> x <=> ${u},${a})",
         filtered_unknown: "ARRAY_FILTER(x -> x = ${u},${a})", count_unknown: "ARRAY_COUNT(x -> x = ${u},${a})",
         exists_unknown: "ARRAY_EXISTS(x -> x = ${u},${a})", any_unknown: "ARRAY_MATCH_ANY(x -> x = ${u},${a})",
         all_unknown: "ARRAY_MATCH_ALL(x -> x = ${u},${a})"]
    })
    matrix.run(delegate, 'index', 'uuid_matrix_array', ['a','idx'], { a,n ->
        [element_value: "ELEMENT_AT(${a},${n})", subscript_value: "(${a})[${n}]", sliced: "ARRAY_SLICE(${a},${n})"]
    })
    matrix.run(delegate, 'slice', 'uuid_matrix_array', ['a','idx','num'], { a,n,k ->
        [sliced: "ARRAY_SLICE(${a},${n},${k})"]
    })
    matrix.run(delegate, 'repeat', 'uuid_matrix_array', ['u','num'], { u,n ->
        [repeated: "ARRAY_REPEAT(${u},${n})", with_constant: "ARRAY_WITH_CONSTANT(${n},${u})"]
    })
    matrix.run(delegate, 'seed', 'uuid_matrix_array', ['a','num'], { a,n ->
        [shuffled_members: "ARRAY_SORT(ARRAY_SHUFFLE(${a},${n}))"]
    })
    matrix.run(delegate, 'constructor', 'uuid_matrix_array', ['u','v','w'], { u,v,w ->
        [array_value: "ARRAY(${u},${v},${w})"]
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
