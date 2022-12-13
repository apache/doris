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

suite("bitmap_agg") {
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // bitmap_union, bitmap_union_count, bitmap_union_int
    test {
        sql """
                select
                  bitmap_union_count(b),
                  bitmap_to_string(bitmap_union(b)),
                  bitmap_union_int(number)
                from
                (
                    select to_bitmap(number) b, number from numbers(number = 10) tmp
                ) t
                """
        result([[10L, "0,1,2,3,4,5,6,7,8,9", 10L]])
    }


    // bitmap_intersect
    def test_bitmap_intersect_table = "test_bitmap_intersect_table"

    sql "drop table if exists ${test_bitmap_intersect_table}"

    sql """
            CREATE TABLE IF NOT EXISTS ${test_bitmap_intersect_table} (
              `id` int,
              `bitmap_string` string
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """

    sql """insert into ${test_bitmap_intersect_table}
                  values
                    (1, '1,2,5'),
                    (2, '2,5,7'),
                    (3, '1,5,9')
                """
    test {
        sql """
                select bitmap_to_string(bitmap_intersect(bitmap_from_string(bitmap_string)))
                from ${test_bitmap_intersect_table}
                """
        result([["5"]])
    }

    test {
        sql """
                select bitmap_to_string(bitmap_intersect(bitmap_from_string(bitmap_string)))
                from ${test_bitmap_intersect_table}
                where id in (1, 2)
                """
        result([["2,5"]])
    }

    // group_bitmap_xor

    def test_group_bitmap_xor_table = "test_group_bitmap_xor_table"

    sql "drop table if exists ${test_group_bitmap_xor_table}"

    sql """
            CREATE TABLE IF NOT EXISTS ${test_group_bitmap_xor_table} (
              `id` int,
              `bitmap_string` string
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """

    sql """insert into ${test_group_bitmap_xor_table}
                  values
                    (1, '4,7,8'),
                    (1, '1,3,6,15'),
                    (1, '4,7')
                """

    test {
        sql """
                select
                    id,
                    bitmap_to_string(group_bitmap_xor(bitmap_from_string(bitmap_string)))
                from ${test_group_bitmap_xor_table}
                group by id
                order by id
                """
        result([[1, "1,3,6,8,15"]])
    }
}
