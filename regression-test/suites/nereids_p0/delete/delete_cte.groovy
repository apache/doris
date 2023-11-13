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

suite('nereids_delete_cte') {
    def t1 = 't1_cte'
    def t2 = 't2_cte'
    def t3 = 't3_cte'

    sql "drop table if exists ${t1}"
    sql """
        create table ${t1} (
            id int,
            id1 int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id, id1)
        distributed by hash(id, id1)
        properties(
            "replication_num"="1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql "drop table if exists ${t2}"
    sql """
        create table ${t2} (
            id int,
            c1 bigint,
            c2 string,
            c3 double,
            c4 date
        ) unique key (id)
        distributed by hash(id)
        properties(
            "replication_num"="1"
        );
    """

    sql "drop table if exists ${t3}"
    sql """
        create table ${t3} (
            id int
        ) distributed by hash(id)
        properties(
            "replication_num"="1"
        );
    """

    sql """
        INSERT INTO ${t1} VALUES
            (1, 10, 1, '1', 1.0, '2000-01-01'),
            (2, 20, 2, '2', 2.0, '2000-01-02'),
            (3, 30, 3, '3', 3.0, '2000-01-03');
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

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_nereids_dml=true"

    sql "insert into ${t1}(id, c1, c2, c3) select id, c1 * 2, c2, c3 from ${t1}"
    sql "insert into ${t2}(id, c1, c2, c3) select id, c1, c2 * 2, c3 from ${t2}"
    sql "insert into ${t2}(c1, c3) select c1 + 1, c3 + 1 from (select id, c1, c3 from ${t1} order by id, c1 limit 10) ${t1}, ${t3}"

    qt_sql "select * from ${t1} order by id, id1"

    sql """
        with cte as (select * from ${t3})
        delete from ${t1}
        using ${t2} join cte on ${t2}.id = cte.id
        where ${t1}.id = ${t2}.id;
    """

    qt_sql "select * from ${t1} order by id, id1"
}