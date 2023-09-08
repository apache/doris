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

suite('nereids_insert_into_values') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    sql 'use nereids_insert_into_table_test'

    def t1 = 'value_t1'
    def t2 = 'value_t2'
    def t3 = 'value_t3'

    sql "drop table if exists ${t1}"
    sql "drop table if exists ${t2}"
    sql "drop table if exists ${t3}"

    sql """
        create table ${t1} (
            id int,
            id1 int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id, id1)
        distributed by hash(id, id1) buckets 13
        properties(
            'replication_num'='1',
            "function_column.sequence_col" = "c4"
        );
    """

    sql """
        create table ${t2} (
            id int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id)
        distributed by hash(id) buckets 13
        properties(
            'replication_num'='1'
        );
    """

    sql """
        create table ${t3} (
            id int
        ) distributed by hash(id) buckets 13
        properties(
            'replication_num'='1'
        );
    """

    sql """
        INSERT INTO ${t1} VALUES
            (1, (1 + 9) * (10 - 9), 1, '1', 1.0, '2000-01-01'),
            (2, 20, 2, '2', 2.0, days_add('2000-01-01', 1)),
            (3, 30, 3, '3', 3.0, makedate(2000, 3));
    """

    sql """
        INSERT INTO ${t2} VALUES
            (1, 10, '10', 10.0, '2000-01-10'),
            (2, 20, '20', 20.0, '2000-01-20'),
            (3, 30, '30', 30.0, '2000-01-30'),
            (4, 4, '4', 4.0, '2000-01-04'),
            (5, 5, '5', 5.0, '2000-01-05');
    """

    sql """
        INSERT INTO ${t3} VALUES
            (1),
            (4),
            (5);
    """

    sql "sync"
    qt_sql_cross_join "select * from ${t1}, ${t2}, ${t3} order by ${t1}.id, ${t1}.id1, ${t2}.id, ${t3}.id"

    sql "drop table if exists agg_have_dup_base_value"

    sql """
        create table agg_have_dup_base_value (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1, k2, k3)
        distributed by hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    createMV("create materialized view k12s3m as select k1, sum(k2), max(k2) from agg_have_dup_base_value group by k1;")

    sql "insert into agg_have_dup_base_value values (-4, -4, -4, 'd')"
    sql "sync"
    qt_mv "select * from agg_have_dup_base_value"
}