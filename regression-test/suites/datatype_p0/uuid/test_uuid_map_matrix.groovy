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
suite("test_uuid_map_matrix", "p0") {
    sql "DROP TABLE IF EXISTS uuid_matrix_map"
    sql """CREATE TABLE uuid_matrix_map (${uuidMatrixSchema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_map VALUES ${uuidMatrixValues()}"

    uuidRunMatrix('unary', 'uuid_matrix_map', ['m'], { m ->
        [keys: "MAP_KEYS(${m})", vals: "MAP_VALUES(${m})", size_value: "MAP_SIZE(${m})",
         deduplicated: "DEDUPLICATE_MAP(${m})", filtered: "MAP_FILTER((k,v) -> v IS NOT NULL,${m})",
         applied: "MAP_APPLY((k,v) -> STRUCT(k,COALESCE(v,k)),${m})",
         exists_match: "MAP_EXISTS((k,v) -> v IS NOT NULL,${m})", all_match: "MAP_ALL((k,v) -> v IS NOT NULL,${m})",
         entries: "MAP_ENTRIES(${m})", from_entries: "MAP_FROM_ENTRIES(MAP_ENTRIES(${m}))"]
    })
    uuidRunMatrix('lookup', 'uuid_matrix_map', ['m','u'], { m,u ->
        [has_key: "MAP_CONTAINS_KEY(${m},${u})", has_value: "MAP_CONTAINS_VALUE(${m},${u})",
         element_value: "ELEMENT_AT(${m},${u})", subscript_value: "(${m})[${u}]"]
    })
    uuidRunMatrix('capture', 'uuid_matrix_map', ['m','u'], { m,u ->
        [applied: "MAP_APPLY((k,v) -> STRUCT(k,COALESCE(v,${u})),${m})",
         filtered: "MAP_FILTER((k,v) -> v <=> ${u},${m})",
         exists_match: "MAP_EXISTS((k,v) -> v <=> ${u},${m})", all_match: "MAP_ALL((k,v) -> v <=> ${u},${m})",
         filtered_unknown: "MAP_FILTER((k,v) -> v = ${u},${m})", exists_unknown: "MAP_EXISTS((k,v) -> v = ${u},${m})",
         all_unknown: "MAP_ALL((k,v) -> v = ${u},${m})"]
    })
    uuidRunMatrix('entry', 'uuid_matrix_map', ['m','u','v'], { m,u,v ->
        [has_entry: "MAP_CONTAINS_ENTRY(${m},${u},${v})"]
    })
    uuidRunMatrix('constructor', 'uuid_matrix_map', ['u','v'], { u,v ->
        [map_value: "MAP(${u},${v})", struct_value: "NAMED_STRUCT('k',${u},'v',${v})"]
    })
    uuidRunMatrix('arrays', 'uuid_matrix_map', ['a','a2'], { a,b ->
        [map_value: "MAP_FROM_ARRAYS(${a},${b})"]
    }, [aligned: true])
    uuidRunMatrix('struct', 'uuid_matrix_map', ['st'], { st ->
        [field_value: "STRUCT_ELEMENT(${st},'k')", nested_field: "STRUCT_ELEMENT(${st},'a')",
         json_value: "TO_JSON(${st})", string_value: "CAST(${st} AS STRING)"]
    })
    for (String function : ['EXPLODE_MAP','EXPLODE_MAP_OUTER']) {
        uuidRunMatrix(function.toLowerCase(), 'uuid_matrix_map', ['m'], { m -> [key_value: 'map_key', value_value: 'map_value'] },
            [lateral: { m -> "LATERAL VIEW ${function}(${m}) generated AS map_key,map_value" }, orderBy: 'id,map_key,map_value'])
    }

}
