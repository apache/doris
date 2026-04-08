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

suite("bucket_shuffle_set_operation") {
    // TODO: open comment when support `enable_local_shuffle_planner` and change to REQUIRE
    return

    multi_sql """
        drop table if exists bucket_shuffle_set_operation1;
        create table bucket_shuffle_set_operation1(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1');
        insert into bucket_shuffle_set_operation1 values(1, 1), (2, 2), (3, 3);
        
        drop table if exists bucket_shuffle_set_operation2;
        create table bucket_shuffle_set_operation2(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1');
        insert into bucket_shuffle_set_operation2 values(1, 1), (2, 2), (3, 3);
        
        drop table if exists bucket_shuffle_set_operation3;
        create table bucket_shuffle_set_operation3(id int, value int) distributed by hash(id) buckets 11 properties('replication_num'='1');
        insert into bucket_shuffle_set_operation3 values(1, 1), (2, 2), (3, 3);
        
        set runtime_filter_mode=off;
        """

    // make bucket shuffle set operation stable
    sql "set parallel_pipeline_task_num=5"

    def checkShapeAndResult = { String tag, String sqlStr ->
        quickTest(tag + "_shape", "explain shape plan " + sqlStr)
        quickTest(tag + "_result", sqlStr, true)
    }

    checkShapeAndResult("bucket_shuffle_union_with_all_column", """
        select *
        from (
            select * from bucket_shuffle_set_operation1
            union all
            select * from bucket_shuffle_set_operation2
        )a
        join[shuffle] (
            select *
            from bucket_shuffle_set_operation1
        )b
        on a.id=b.id""")

    checkShapeAndResult("bucket_shuffle_intersect", """
        select id from bucket_shuffle_set_operation1
        intersect
        select id from bucket_shuffle_set_operation2""")

    checkShapeAndResult("bucket_shuffle_intersect_with_all_column", """
        select * from bucket_shuffle_set_operation1
        intersect
        select * from bucket_shuffle_set_operation2""")

    checkShapeAndResult("no_bucket_shuffle_intersect", """
        select value from bucket_shuffle_set_operation1
        intersect
        select value from bucket_shuffle_set_operation2""")

    checkShapeAndResult("bucket_shuffle_to_left", """
        select id from bucket_shuffle_set_operation3
        intersect
        select id from bucket_shuffle_set_operation1
        """)

    checkShapeAndResult("bucket_shuffle_to_right", """
        select id from bucket_shuffle_set_operation1
        intersect
        select id from bucket_shuffle_set_operation3
        """)

    checkShapeAndResult("bucket_shuffle_except_1", """
        select id from bucket_shuffle_set_operation1 where id=1
        except
        select id from bucket_shuffle_set_operation2
        """)

    checkShapeAndResult("bucket_shuffle_except_2", """
        select id from bucket_shuffle_set_operation1
        except
        select id from bucket_shuffle_set_operation2 where id=1
        """)

    explain {
        sql """
        select id, id as id2 from (select nullable(id) as id from bucket_shuffle_set_operation1)a
        intersect
        (select id, id as id2 from bucket_shuffle_set_operation3)
        """

        check { String e ->
            def index = e.indexOf("VINTERSECT")
            e = e.substring(index)

            // extract following 6 lines of VINTERSECT:
            //
            // VINTERSECT(325)
            //  |  runtime filters: RF000[min_max] <- id[#4](-1/1/1048576), RF001[in_or_bloom] <- id[#4](-1/1/1048576)
            //  |  distribute expr lists: id2[#5]
            //  |  distribute expr lists: id2[#9]
            //  |

            def lines = e.split("\n")
            boolean checked = false
            for (int i = 1; i < Math.min(6, lines.length); ++i) {
                if (lines[i].contains("distribute expr lists")) {
                    def line = lines[i].substring(lines[i].indexOf(":") + 1)

                    // because left shuffle to right, and right only distribute 1 column(id)
                    // so we should ensure left shuffle to right only distribute by 1 column,
                    // not distribute 2 columns
                    assertTrue(line.trim().split(",").length == 1)
                    checked = true
                }
            }
            assertTrue(checked)
        }
    }
}