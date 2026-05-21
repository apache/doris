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

suite("insert_select_partial_columns_without_column_list", "p0") {
    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;"

    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql "use ${db};"
        sql "set enable_nereids_dml=true;"
        sql "set enable_strict_consistency_dml=true;"
        sql "set enable_unique_key_partial_update=false;"
        sql "sync;"

        sql "drop table if exists insert_select_partial_dup_dst"
        sql "drop table if exists insert_select_partial_dup_src"
        sql """
            create table insert_select_partial_dup_dst (
                k1 int,
                k2 int,
                v1 int default "100",
                v2 string null
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql """
            create table insert_select_partial_dup_src (
                k1 int,
                k2 int
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql "insert into insert_select_partial_dup_src values (1, 10), (2, 20)"
        sql "insert into insert_select_partial_dup_dst select k1, k2 from insert_select_partial_dup_src"
        sql "sync"
        order_qt_dup_partial """
            select * from insert_select_partial_dup_dst order by k1, k2, v1, v2
        """

        sql "insert into insert_select_partial_dup_dst(k1, k2) select k1, k2 from insert_select_partial_dup_src"
        sql "sync"
        order_qt_dup_explicit """
            select * from insert_select_partial_dup_dst order by k1, k2, v1, v2
        """

        test {
            sql """
                insert into insert_select_partial_dup_dst
                select k1, k2, 1, 'x', 999 from insert_select_partial_dup_src
            """
            exception "insert into cols should be corresponding to the query output"
        }

        sql "drop table if exists insert_select_partial_agg_dst"
        sql "drop table if exists insert_select_partial_agg_src"
        sql """
            create table insert_select_partial_agg_dst (
                k1 int,
                v1 int replace default "100"
            )
            aggregate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql """
            create table insert_select_partial_agg_src (
                k1 int
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql "insert into insert_select_partial_agg_src values (1), (2)"
        sql "insert into insert_select_partial_agg_dst select k1 from insert_select_partial_agg_src"
        sql "sync"
        order_qt_agg_partial """
            select * from insert_select_partial_agg_dst order by k1, v1
        """

        sql "drop table if exists insert_select_partial_not_null_dst"
        sql "drop table if exists insert_select_partial_not_null_src"
        sql """
            create table insert_select_partial_not_null_dst (
                k1 int,
                k2 int not null
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql """
            create table insert_select_partial_not_null_src (
                k1 int
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql "insert into insert_select_partial_not_null_src values (1)"
        test {
            sql "insert into insert_select_partial_not_null_dst select k1 from insert_select_partial_not_null_src"
            exception "Column has no default value"
        }

        sql "drop table if exists insert_select_partial_generated_dst"
        sql "drop table if exists insert_select_partial_generated_src"
        sql """
            create table insert_select_partial_generated_dst (
                k1 int,
                v1 int,
                g1 int generated always as (v1 + 1)
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql """
            create table insert_select_partial_generated_src (
                k1 int,
                v1 int
            )
            duplicate key(k1)
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1");
        """
        sql "insert into insert_select_partial_generated_src values (1, 10), (2, 20)"
        sql "insert into insert_select_partial_generated_dst select k1, v1 from insert_select_partial_generated_src"
        sql "sync"
        order_qt_generated_partial """
            select * from insert_select_partial_generated_dst order by k1, v1, g1
        """

        sql "drop table if exists insert_select_partial_unique_dst"
        sql "drop table if exists insert_select_partial_unique_src"
        sql """
            create table insert_select_partial_unique_dst (
                id int,
                score int,
                v1 int default "100",
                v2 string null
            )
            unique key(id)
            distributed by hash(id) buckets 1
            properties(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """
        sql """
            create table insert_select_partial_unique_src (
                id int,
                score int
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1");
        """
        sql "insert into insert_select_partial_unique_dst values (1, 10, 500, 'old')"
        sql "insert into insert_select_partial_unique_src values (1, 11), (2, 20)"
        sql "insert into insert_select_partial_unique_dst select id, score from insert_select_partial_unique_src"
        sql "sync"
        order_qt_unique_partial """
            select * from insert_select_partial_unique_dst order by id, score, v1, v2
        """

        sql "set enable_unique_key_partial_update=true;"
        sql "insert into insert_select_partial_unique_dst select id, score from insert_select_partial_unique_src"
        sql "sync"
        order_qt_unique_partial_session_enabled """
            select * from insert_select_partial_unique_dst order by id, score, v1, v2
        """
        sql "set enable_unique_key_partial_update=false;"

        sql "drop table if exists insert_select_partial_seq_dst"
        sql "drop table if exists insert_select_partial_seq_src"
        sql """
            create table insert_select_partial_seq_dst (
                id int,
                score int,
                update_time datetime not null
            )
            unique key(id)
            distributed by hash(id) buckets 1
            properties(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "update_time"
            );
        """
        sql """
            create table insert_select_partial_seq_src (
                id int,
                score int,
                update_time datetime
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1");
        """
        sql """
            insert into insert_select_partial_seq_src values
                (1, 10, '2024-01-01 00:00:00'),
                (2, 20, '2024-01-02 00:00:00')
        """
        sql "insert into insert_select_partial_seq_dst select id, score, update_time from insert_select_partial_seq_src"
        sql "sync"
        order_qt_seq_with_input """
            select * from insert_select_partial_seq_dst order by id, score, update_time
        """

        sql "delete from insert_select_partial_seq_dst where id is not null"
        test {
            sql "insert into insert_select_partial_seq_dst select id, score from insert_select_partial_seq_src"
            exception "has sequence column, need to specify the sequence column"
        }
    }
}
