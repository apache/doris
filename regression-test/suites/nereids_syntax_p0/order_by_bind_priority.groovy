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

suite("order_by_bind_priority") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    sql "drop table if exists t_order_by_bind_priority"
    sql """create table t_order_by_bind_priority (c1 int, c2 int) distributed by hash(c1) properties("replication_num"="1");"""
    sql "insert into t_order_by_bind_priority values(-2, -2),(1,2),(1,2),(3,3),(-5,5);"
    sql "sync"


    qt_test_bind_order_by_with_aggfun1 "select 2*abs(sum(c1)) as c1, c1,sum(c1)+c1 from t_order_by_bind_priority group by c1 order by sum(c1)+c1 asc;"
    qt_test_bind_order_by_with_aggfun2 "select 2*abs(sum(c1)) as c2, c1,sum(c1)+c2 from t_order_by_bind_priority group by c1,c2 order by sum(c1)+c2 asc;"
    qt_test_bind_order_by_with_aggfun3 "select abs(sum(c1)) as c1, c1,sum(c2) as c2 from t_order_by_bind_priority group by c1 order by sum(c1) asc;"
    qt_test_bind_order_by_in_no_agg_func_output "select abs(c1) xx, sum(c2) from t_order_by_bind_priority group by xx order by min(xx)"
    test {
        sql "select abs(sum(c1)) as c1, c1,sum(c2) as c2 from t_order_by_bind_priority group by c1 order by sum(c1)+c2 asc;"
        exception "c2 should be grouped by."
    }
    sql """drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by58"""
    sql """
        create table table_20_undef_partitions2_keys3_properties4_distributed_by58 (
        pk int,
        col_int_undef_signed int   ,
        col_int_undef_signed2 int
        ) engine=olap
        DUPLICATE KEY(pk, col_int_undef_signed)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by58(pk,col_int_undef_signed,col_int_undef_signed2) values (0,-8777,null),
        (1,-127,30240),(2,null,null),(3,-10008,-54),(4,3618,2881),(5,null,16155),(6,null,null),(7,null,null),(8,-29433,-4654),(9,-13909,29600),(10,10450,8105),
        (11,null,null),(12,88,88),(13,null,null),(14,74,14138),(15,23,63),(16,4418,-24598),(17,-22950,99),(18,null,null),(19,2,null);
    """
    sql "sync;"

    qt_test_multi_slots_in_agg_func_bind_first """
        SELECT
        SIGN( SUM(col_int_undef_signed) ) + 3 AS col_int_undef_signed,
        pk - 5 pk ,
        pk pk ,
        ABS( MIN(col_int_undef_signed) ) AS col_int_undef_signed,
        MAX(col_int_undef_signed2) col_int_undef_signed2,
        col_int_undef_signed2 col_int_undef_signed2
        FROM
        table_20_undef_partitions2_keys3_properties4_distributed_by58 tbl_alias1
        GROUP BY
        pk,
        col_int_undef_signed2
        ORDER BY
        col_int_undef_signed,
        pk - 5,
        pk,
        col_int_undef_signed2 ;
    """

}