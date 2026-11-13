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

suite("test_map_insert_shared_column", "p0") {
    sql "drop table if exists test_map_insert_shared_column_src"
    sql "drop table if exists test_map_insert_shared_column_dst"

    sql """
        create table test_map_insert_shared_column_src (
            id int,
            m map<int, string>
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table test_map_insert_shared_column_dst (
            id int,
            m_short map<int, varchar(3)>,
            m_full map<int, string>
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_map_insert_shared_column_src values
            (401, {1: "abcdef"}),
            (402, {2: "uvwxyz"})
    """

    sql "set enable_insert_strict = false"
    sql """
        insert into test_map_insert_shared_column_dst
        select id, m, m from test_map_insert_shared_column_src
    """
    order_qt_shared_column """
        select id, m_short, m_full
        from test_map_insert_shared_column_dst
        order by id
    """
}
