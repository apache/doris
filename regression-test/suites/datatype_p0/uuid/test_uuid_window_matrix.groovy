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

suite("test_uuid_window_matrix", "p0") {
    sql "DROP TABLE IF EXISTS uuid_matrix_window"
    sql """CREATE TABLE uuid_matrix_window (${uuidMatrixSchema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_window VALUES ${uuidMatrixValues()}"

    uuidRunMatrix('unary', 'uuid_matrix_window', ['u'], { u ->
        String frame = 'OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)'
        [lag_value: "LAG(${u}) OVER (ORDER BY id)", lead_value: "LEAD(${u}) OVER (ORDER BY id)",
         first_value: "FIRST_VALUE(${u}) ${frame}", last_value: "LAST_VALUE(${u}) ${frame}",
         first_ignore_null: "FIRST_VALUE(${u},TRUE) ${frame}", last_ignore_null: "LAST_VALUE(${u},TRUE) ${frame}",
         nth_value: "NTH_VALUE(${u},2) ${frame}", nth_first: "NTH_VALUE(${u},1) ${frame}",
         nth_outside: "NTH_VALUE(${u},100) ${frame}", minimum: "MIN(${u}) ${frame}", maximum: "MAX(${u}) ${frame}",
         count_value: "COUNT(${u}) ${frame}", rank_value: "RANK() OVER (ORDER BY ${u})",
         dense_rank_value: "DENSE_RANK() OVER (ORDER BY ${u})", row_number_value: "ROW_NUMBER() OVER (ORDER BY ${u},id)",
         percent_rank_value: "PERCENT_RANK() OVER (ORDER BY ${u})", cume_dist_value: "CUME_DIST() OVER (ORDER BY ${u})",
         ntile_value: "NTILE(3) OVER (ORDER BY ${u},id)"]
    })
    uuidRunMatrix('default', 'uuid_matrix_window', ['u','v'], { u,v ->
        [lag_zero: "LAG(${u},0,${v}) OVER (ORDER BY id)", lead_zero: "LEAD(${u},0,${v}) OVER (ORDER BY id)",
         lag_value: "LAG(${u},1,${v}) OVER (ORDER BY id)", lead_value: "LEAD(${u},1,${v}) OVER (ORDER BY id)",
         lag_outside: "LAG(${u},100,${v}) OVER (ORDER BY id)", lead_outside: "LEAD(${u},100,${v}) OVER (ORDER BY id)"]
    })
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        for (String query : ['LAG(u,-1)', 'LEAD(u,-1)', 'LAG(u,NULL)', 'LEAD(u,NULL)',
                             'NTH_VALUE(u,0)', 'NTH_VALUE(u,-1)', 'NTH_VALUE(u,NULL)',
                             'NTILE(0)', 'NTILE(-1)', 'NTILE(NULL)']) {
            test {
                sql "SELECT ${query} OVER (ORDER BY id) FROM uuid_matrix_window"
                exception query == 'NTILE(NULL)' ? 'must be a integer' : 'constant positive integer'
            }
        }
        for (String query : ['FIRST_VALUE(u,NULL)', 'LAST_VALUE(u,NULL)']) {
            test {
                sql "SELECT ${query} OVER (ORDER BY id) FROM uuid_matrix_window"
                exception 'must be true or false'
            }
        }
    }
    for (String query : ['LAG(u,idx)', 'LEAD(u,idx)', 'NTH_VALUE(u,idx)', 'FIRST_VALUE(u,flag)', 'LAST_VALUE(u,flag)']) {
        test {
            sql "SELECT ${query} OVER (ORDER BY id) FROM uuid_matrix_window"
            exception "constant"
        }
    }
}
