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

suite("test_orthogonal_bitmap_expr_calculate") {
    multi_sql """
            drop table if exists test_orthogonal_bitmap_expr_calculate;

            create table test_orthogonal_bitmap_expr_calculate(
                id int,
                tag int,
                user_id bitmap bitmap_union
            )
            aggregate key(id, tag)
            distributed by hash(id) buckets 1
            properties(
                'replication_num'='1'
            );
            
            insert into test_orthogonal_bitmap_expr_calculate values
            (1, 100, bitmap_from_string('1,2,3,4,5')),
            (1, 200, bitmap_from_string('3,4,5,6,7'));

            set enable_fallback_to_original_planner=false;
            """

    test {
        sql """
            select bitmap_to_string(orthogonal_bitmap_expr_calculate(user_id, tag, '(100&200)'))
            from test_orthogonal_bitmap_expr_calculate
            """
        result([['3,4,5']])
    }

    test {
        sql """
            select orthogonal_bitmap_expr_calculate_count(user_id, tag, '(100&200)')
            from test_orthogonal_bitmap_expr_calculate
            """
        result([[3L]])
    }
}
