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

suite("aggregate_group_by_metric_type") {
    def error_msg = "column must use with specific function, and don't support filter"
    sql "DROP TABLE IF EXISTS test_group_by_hll_and_bitmap"

    sql """
        CREATE TABLE IF NOT EXISTS test_group_by_hll_and_bitmap (id int, user_ids bitmap bitmap_union, hll_set hll hll_union) 
        ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """

    sql "insert into test_group_by_hll_and_bitmap values(1, bitmap_hash(1), hll_hash(1))"

    test {
        sql "select distinct user_ids from test_group_by_hll_and_bitmap"
        exception "${error_msg}"
    }

    test {
        sql "select distinct hll_set from test_group_by_hll_and_bitmap"
        exception "${error_msg}"
    }

    test {
        sql "select user_ids from test_group_by_hll_and_bitmap order by user_ids"
        exception "${error_msg}"
    }

    test {
        sql "select hll_set from test_group_by_hll_and_bitmap order by hll_set"
        exception "${error_msg}"
    }

    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_planner=true'

    test {
        sql "select distinct user_ids from test_group_by_hll_and_bitmap"
        exception "${error_msg}"
    }

    test {
        sql "select distinct hll_set from test_group_by_hll_and_bitmap"
        exception "${error_msg}"
    }

    test {
        sql "select user_ids from test_group_by_hll_and_bitmap order by user_ids"
        exception "${error_msg}"
    }

    test {
        sql "select hll_set from test_group_by_hll_and_bitmap order by hll_set"
        exception "${error_msg}"
    }
    sql "DROP TABLE test_group_by_hll_and_bitmap"

    sql "DROP TABLE IF EXISTS test_group_by_array"
    sql """
        CREATE TABLE IF NOT EXISTS test_group_by_array (id int, c_array array<int>) ENGINE=OLAP DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1 properties("replication_num" = "1");
        """
    sql "insert into test_group_by_array values(1, [1,2,3])"

    test {
        sql "select distinct c_array from test_group_by_array"
        exception "${error_msg}"
    }
    test {
        sql "select c_array from test_group_by_array order by c_array"
        exception "${error_msg}"
    }
    test {
        sql "select c_array,count(*) from test_group_by_array group by c_array"
        exception "${error_msg}"
    }

    sql "DROP TABLE test_group_by_array"

    sql "DROP TABLE IF EXISTS test_group_by_struct"

    sql """
        CREATE TABLE IF NOT EXISTS test_group_by_struct (id int, s_struct struct<f1:tinyint, f2:char(5)>) ENGINE=OLAP DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1 properties("replication_num" = "1");
        """

    sql "insert into test_group_by_struct values(1, {1, 'a'})"

    test {
        sql "select distinct s_struct from test_group_by_struct"
        exception "${error_msg}"
    }
    test {
        sql "select s_struct from test_group_by_struct order by s_struct"
        exception "${error_msg}"
    }
    test {
        sql "select s_struct,count(*) from test_group_by_struct group by s_struct"
        exception "${error_msg}"
    }

    sql "DROP TABLE IF EXISTS test_group_by_struct_join"
    sql """
        CREATE TABLE IF NOT EXISTS test_group_by_struct_join (id int, value int)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1 properties("replication_num" = "1");
    """

    sql """
        insert into test_group_by_struct_join values(1, 1), (1, 2), (1, 3), (1, 4);
    """

    qt_select """
        select
            t1.s_struct
            , t2.value
        from
            test_group_by_struct t1 right join test_group_by_struct_join t2 on t1.id = t2.id
        order by t2.value;
    """
}
