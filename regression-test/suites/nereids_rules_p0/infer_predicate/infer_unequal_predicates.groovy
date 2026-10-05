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

suite("infer_unequal_predicates") {
    sql """SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"""
    sql "set runtime_filter_mode = OFF"
    sql "set disable_join_reorder=true "
    sql "drop table if exists infer_unequal_predicates_t1"
    sql """
        CREATE TABLE `infer_unequal_predicates_t1` (
          `a` INT NULL,
          `b` VARCHAR(10) NULL,
          `c` INT NULL,
          `d` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`) 
        partition by list(d) 
        (partition p1 values in (5,6),
        partition p2 values in (7,8))
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql "insert into infer_unequal_predicates_t1 values(1,'d2',3,5);"
    sql "insert into infer_unequal_predicates_t1 values(0,'d2',3,5);"
    sql "insert into infer_unequal_predicates_t1 values(0,'d2',3,7);"

    sql "drop table if exists infer_unequal_predicates_t2"
    sql """
        CREATE TABLE `infer_unequal_predicates_t2` (
          `a` INT NULL,
          `b` VARCHAR(10) NULL,
          `c` INT NULL,
          `d` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`) 
        partition by list(d) 
        (partition p1 values in (5,6),
        partition p2 values in (7,8))
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql "insert into infer_unequal_predicates_t2 values(1,'d2',3,5);"
    sql "insert into infer_unequal_predicates_t2 values(0,'d2',3,5);"
    sql "insert into infer_unequal_predicates_t2 values(0,'d2',3,7);"

    sql "drop table if exists infer_unequal_predicates_t3"
    sql """
    create table infer_unequal_predicates_t3(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2,d_date date, d_datetime datetime) properties('replication_num'='1');
    """
    sql """
    insert into infer_unequal_predicates_t3 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09','2022-08-09','2022-08-09 10:00:00'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11','2022-08-03','2022-08-09 10:00:02');
    """

    // c<a, a<1 -> c<1 should not be inferred
    qt_not_infer_same_table_have_mid_column """
    explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 WHERE t1.c<t1.a AND t1.a<5
    """

    // c<1, 1<a -> c<a should not be inferred
    qt_not_infer_same_table_have_mid_literal """
    explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 WHERE t1.c<1 AND 1<t1.a
    """

    // t1.a<1, 1<t2.b -> t1.a<t2.b should not be inferred
    qt_not_infer_diff_table_have_mid_literal """explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a<1 and 1<t2.a"""

    // t1.c<t2.a, t2.a<1 -> t1.c<1 should be inferred
    qt_infer_diff_table """explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t2.a<1 and t1.c<t2.a"""

    // a<c, c<1 -> a<1 should be inferred
    qt_should_infer_because_a_is_key """
    explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 WHERE t1.a<t1.c AND t1.c<5
    """
    // d<c, c<1 -> d<1 should be inferred
    qt_should_infer_because_d_is_partition_column """
    explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 WHERE t1.d<t1.c AND t1.c<10
    """

    // t1.a<1, t1.a=t2.c -> t2.c<1 should be inferred
    qt_infer_with_equal """explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a<1 and t1.a=t2.c"""

    // t2.c<1, t1.a<t2.a, t2.a<t2.c -> t1.a<1 and t2.a<1 should be inferred
    qt_infer_4_expr """explain shape plan
    SELECT * FROM  infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t2.c<1 and t1.a<t2.a and t2.a <t2.c """

    qt_infer_long_chain_same_table_infer_a_and_d """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 WHERE t1.a<t1.d AND t1.d<t1.c AND t1.c<10
    """
    qt_infer_long_chain_same_table_not_infer_c """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 WHERE t1.a<t1.c AND t1.c<t1.d AND t1.d<10
    """

    qt_remove_useless_input_predicate_c_less_than_10 """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 WHERE t1.a<t1.c AND t1.c<t1.d AND t1.d<10 and t1.c<10
    """
    qt_remove_useless_predicate """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a>1 AND t1.a=t1.c
    """
    qt_infer_long_chain_diff_table """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a<t2.d AND t2.d<t2.c AND t2.c<10
    """

    qt_infer_with_constant_and_columns """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a>1 AND t1.a=t2.c AND t2.c<t2.d
    """

    qt_no_infer """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a<t2.d AND t2.d>t2.c
    """

    qt_no_infer_cyclic_dependency """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a<t2.c AND t2.c<t1.a
    """

    qt_infer_multiple_conditions """
    explain shape plan
    SELECT * FROM infer_unequal_predicates_t1 t1 INNER JOIN infer_unequal_predicates_t2 t2 ON t1.a<t2.a AND t2.a<t2.c AND t2.c<t2.d AND t2.d<10
    """

    qt_infer_cast_int """
    explain shape plan
    select * from infer_unequal_predicates_t3 t1 inner join infer_unequal_predicates_t3 t2 on t1.d_int>t2.d_smallint and t2.d_smallint >1;
    """

    qt_multi_slot_equal """explain shape plan select * from infer_unequal_predicates_t1 where a=c and c=d"""

    qt_no_redundant_predicates """
    explain shape plan
    SELECT t1.a FROM (select * from infer_unequal_predicates_t1 t1 where t1.d<10 and t1.d=t1.c and t1.c<10) t1 inner join
    infer_unequal_predicates_t2 t2 on t1.d=t2.d where t2.d>1
    """

    // TODO
    // Non equivalent transfer relation derivation, expression is not supported temporarily
    qt_expr_unequal_infer_same_table1 """explain shape plan
    select * from infer_unequal_predicates_t1 t1 where abs(t1.d)<abs(t1.c) and abs(t1.d)<1 and abs(t1.c)<1
    """
    qt_expr_unequal_infer_same_table2 """explain shape plan
    select * from infer_unequal_predicates_t1 t1 where abs(t1.d)<abs(t1.c) and abs(t1.c)<1"""
    qt_expr_unequal_infer_diff_table """explain shape plan
    select * from infer_unequal_predicates_t1 t1 ,infer_unequal_predicates_t2 t2 where abs(t1.d)<abs(t2.c) and abs(t2.c)<1"""

    // TODO
    // should only same 2 predicates, equal condition abs(t1.d)=abs(t1.c) is not in join hash condition,
    // so it is not rewritten into slotReference, and expr infer is not supported temporarily
    qt_not_infer_expr1 """explain shape plan
    select * from infer_unequal_predicates_t1 t1 where abs(t1.d)=abs(t1.c) and abs(t1.d)=1 and abs(t1.c)=1"""
    // not infer because of same reason with qt_not_infer_expr1
    qt_not_infer_expr2 """explain shape plan
    select * from infer_unequal_predicates_t1 t1 where abs(t1.d)=abs(t1.c) and abs(t1.d)=1"""
    // abs(t1.d) and abs(t2.c) is computed that from same table, because qualifier is empty
    qt_not_infer_because_is_infer_and_then_remove """explain shape plan
    select * from infer_unequal_predicates_t1 t1 ,infer_unequal_predicates_t2 t2 where abs(t1.d)=abs(t2.c) and abs(t1.d)<1"""
    qt_infer_join_equal_condition """explain shape plan
    select * from infer_unequal_predicates_t1 t1 ,infer_unequal_predicates_t2 t2 where abs(t1.d)=abs(t2.c) and abs(t1.d)=1"""
}