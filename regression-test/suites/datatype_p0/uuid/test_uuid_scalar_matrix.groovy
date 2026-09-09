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
suite("test_uuid_scalar_matrix", "p0") {
    sql "DROP TABLE IF EXISTS uuid_matrix_scalar"
    sql """CREATE TABLE uuid_matrix_scalar (${uuidMatrixSchema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_scalar VALUES ${uuidMatrixValues()}"

    sql "DROP TABLE IF EXISTS uuid_matrix_scalar_notnull"
    sql """CREATE TABLE uuid_matrix_scalar_notnull (${
           uuidMatrixSchema().replace('u UUID,','u UUID NOT NULL,')
                             .replace('v UUID,','v UUID NOT NULL,')
                             .replace('w UUID,','w UUID NOT NULL,')})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_matrix_scalar_notnull SELECT id,
           IFNULL(u,CAST(REPEAT('0',32) AS UUID)),IFNULL(v,CAST(REPEAT('0',32) AS UUID)),
           IFNULL(w,CAST(REPEAT('0',32) AS UUID)),flag,idx,num,a,a2,flags,m,st FROM uuid_matrix_scalar"""
    for (boolean nullable : [true,false]) {
        String table = nullable ? 'uuid_matrix_scalar' : 'uuid_matrix_scalar_notnull'
        uuidRunMatrix("unary_${nullable ? 'nullable' : 'notnull'}", table, ['u'], { u ->
            [version: "UUID_VERSION(${u})", is_null: "${u} IS NULL", is_not_null: "${u} IS NOT NULL",
             json_value: "TO_JSON(${u})", string_value: "CAST(${u} AS STRING)"]
        })
        uuidRunMatrix("binary_${nullable ? 'nullable' : 'notnull'}", table, ['u','v'], { u,v ->
            [eq: "${u} = ${v}", ne: "${u} != ${v}", lt: "${u} < ${v}", le: "${u} <= ${v}",
             gt: "${u} > ${v}", ge: "${u} >= ${v}", null_safe_eq: "${u} <=> ${v}",
             null_if: "NULLIF(${u},${v})", if_null: "IFNULL(${u},${v})", nvl_value: "NVL(${u},${v})",
             coalesce_value: "COALESCE(${u},${v})", greatest_value: "GREATEST(${u},${v})", least_value: "LEAST(${u},${v})"]
        })
        uuidRunMatrix("ternary_${nullable ? 'nullable' : 'notnull'}", table, ['u','v','w'], { u,v,w ->
            [in_values: "${u} IN (${v},${w})", not_in_values: "${u} NOT IN (${v},${w})",
             between_values: "${u} BETWEEN ${v} AND ${w}", not_between_values: "${u} NOT BETWEEN ${v} AND ${w}",
             simple_case: "CASE ${u} WHEN ${v} THEN ${w} ELSE ${u} END",
             coalesce_value: "COALESCE(${u},${v},${w})", greatest_value: "GREATEST(${u},${v},${w})",
             least_value: "LEAST(${u},${v},${w})"]
        })
        uuidRunMatrix("conditional_${nullable ? 'nullable' : 'notnull'}", table, ['flag','u','v'], { flag,u,v ->
            [if_value: "IF(${flag},${u},${v})", searched_case: "CASE WHEN ${flag} THEN ${u} ELSE ${v} END",
             and_value: "${flag} AND (${u} = ${v})", or_value: "${flag} OR (${u} = ${v})",
             not_value: "NOT (${flag} AND (${u} = ${v}))"]
        })
    }
    // Observe both the foldable constant child and the surviving column-dependent parent.
    String foldQuery = """SELECT UUID_VERSION(CAST(CONCAT('550E8400','E29B41D4A716446655440000') AS UUID)),
                        COALESCE(u,CAST(CONCAT('550E8400','E29B41D4A716446655440000') AS UUID))
                        FROM uuid_matrix_scalar ORDER BY id"""
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        explain {
            sql "verbose ${foldQuery}"
            contains "coalesce(u["
            if (mode == 'runtime') {
                contains "concat('550E8400'"
                contains "uuid_version("
            } else {
                notContains "concat('550E8400'"
                if (mode == 'be') {
                    notContains "uuid_version("
                } else {
                    contains "uuid_version("
                }
            }
        }
        qt_fold_path foldQuery
    }
}
