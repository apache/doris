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

suite("bind_priority") {
    sql """
        DROP TABLE IF EXISTS bind_priority_tbl
       """

    sql """CREATE TABLE IF NOT EXISTS bind_priority_tbl (a int not null, b int not null)
        DISTRIBUTED BY HASH(a)
        BUCKETS 1
        PROPERTIES(
            "replication_num"="1"
        )
        """

    sql """
    insert into bind_priority_tbl values(1, 2),(3, 4)
    """

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """sync"""

    qt_select """
        select a * -1 as a from bind_priority_tbl group by a order by a;
    """

    qt_select """
        select coalesce(a, 'all') as a, count(*) as cnt from (select  null as a  union all  select  'a' as a ) t group by grouping sets ((a),()) order by a, cnt;
    """

    qt_select """
        select (a-1) as a from bind_priority_tbl group by a having a=1;
    """

    test {
        sql """
            select sum(a) as v from bind_priority_tbl  group by v;
            """
        exception "Unknown column 'v' in 'table list' in AGGREGATE clause"
    }

    sql "drop table if exists bind_priority_tbl"
    sql """
        CREATE TABLE bind_priority_tbl (
            `a` int(11) NOT NULL,
            `b` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql"insert into bind_priority_tbl values(1,5),(2, 6),(3,4);"

    qt_bind_sort_scan "select a as b from bind_priority_tbl order by bind_priority_tbl.b;"

    qt_bind_sort_alias "select a as b from bind_priority_tbl order by b;"

    qt_bind_sort_aliasAndScan "select a as b from bind_priority_tbl order by bind_priority_tbl.b + b;"

    sql "drop table if exists bind_priority_tbl2"
    sql """
        CREATE TABLE bind_priority_tbl2 (
            `a` int(11) NOT NULL,
            `b` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """
    sql "insert into bind_priority_tbl2 values(3,5),(2, 6),(1,4);"

    qt_bind_order_to_project_alias """
            select bind_priority_tbl.b b, bind_priority_tbl2.b
            from bind_priority_tbl join bind_priority_tbl2 on bind_priority_tbl.a=bind_priority_tbl2.a
            order by b;
        """


    qt_bind_order_to_project_alias """
        select bind_priority_tbl.b, bind_priority_tbl2.b b
        from bind_priority_tbl join bind_priority_tbl2 on bind_priority_tbl.a=bind_priority_tbl2.a
        order by b;
        """

    test{
        sql "SELECT a,2 as a FROM (SELECT '1' as a) b HAVING a=1"
        exception "a is ambiguous: a#1, a#2."
    }

    sql "drop table if exists duplicate_slot";
    sql "create table if not exists duplicate_slot(id int) distributed by hash(id) properties('replication_num'='1');"
    sql "insert into duplicate_slot values(1), (1), (2), (2), (3), (3);"
    test {
        sql("""
                select id, id
                from duplicate_slot
                group by id
                order by id"""
        )

        result([[1, 1], [2, 2], [3, 3]])
    }

    test {
        sql("""
            with tb1 as
            (
              select id
              from
              (
                select 1 id, 'a' name
                union all
                select /*+SET_VAR(query_timeout=60) */ 2 id, 'b' name
                union all
                select 3 id, 'c' name
              ) a
            ), tb2 as
            (
              select * from tb1
            )
            select * from tb2 order by id;
            """)

        result([[1], [2], [3]])
    }

    def testBindHaving = {
        sql "drop table if exists test_bind_having_slots"

        sql """create table test_bind_having_slots
                (id int, age int)
                distributed by hash(id)
                properties('replication_num'='1');
                """
        sql "insert into test_bind_having_slots values(1, 10), (2, 20), (3, 30);"

        order_qt_having_bind_child """
            select id, sum(age)
            from test_bind_having_slots s
            group by id
            having id = 1; -- bind id from group by
            """

        order_qt_having_bind_child2 """
            select id + 1 as id, sum(age)
            from test_bind_having_slots s
            group by id
            having id = 1; -- bind id from group by
            """

        order_qt_having_bind_child3 """
            select id + 1 as id, sum(age)
            from test_bind_having_slots s
            group by id
            having id + 1 = 2; -- bind id from group by
            """

        order_qt_having_bind_project """
            select id + 1 as id, sum(age)
            from test_bind_having_slots s
            group by id + 1
            having id = 2; -- bind id from project
            """

        order_qt_having_bind_project2 """
            select id + 1 as id, sum(age)
            from test_bind_having_slots s
            group by id + 1
            having id + 1 = 2;  -- bind id from project
            """

        order_qt_having_bind_project3 """
            select id + 1 as id, sum(age + 1) as age
            from test_bind_having_slots s
            group by id
            having age = 10; -- bind age from project
            """

        order_qt_having_bind_project4 """
            select id + 1 as id, sum(age + 1) as age
            from test_bind_having_slots s
            group by id
            having age = 11; -- bind age from project
            """

        order_qt_having_bind_child4 """
            select id + 1 as id, sum(age + 1) as age
            from test_bind_having_slots s
            group by id
            having sum(age) = 10; -- bind age from s
            """

        order_qt_having_bind_child5 """
            select id + 1 as id, sum(age + 1) as age
            from test_bind_having_slots s
            group by id
            having sum(age + 1) = 11 -- bind age from s
            """




        sql "drop table if exists test_bind_having_slots2"
        sql """create table test_bind_having_slots2
                (id int)
                distributed by hash(id)
                properties('replication_num'='1');
                """
        sql "insert into test_bind_having_slots2 values(1), (2), (3), (2);"

        order_qt_having_bind_agg_fun """
               select id, abs(sum(id)) as id
                from test_bind_having_slots2
                group by id
                having sum(id) + id  >= 7
                """

        order_qt_having_bind_agg_fun """
               select id, abs(sum(id)) as id
                from test_bind_having_slots2
                group by id
                having sum(id) + id  >= 6
                """





        sql "drop table if exists test_bind_having_slots3"

        sql """CREATE TABLE `test_bind_having_slots3`(pk int, pk2 int)
                    DUPLICATE KEY(`pk`)
                    DISTRIBUTED BY HASH(`pk`) BUCKETS 10
                    properties('replication_num'='1');
                    """
        sql "insert into test_bind_having_slots3 values(1, 1), (2, 2), (2, 2), (3, 3), (3, 3), (3, 3);"

        order_qt_having_bind_group_by """
                SELECT pk + 6 as ps, COUNT(pk )  *  3 as pk
                FROM test_bind_having_slots3  tbl_alias1
                GROUP by pk
                HAVING  pk = 1
                """

        order_qt_having_bind_group_by """
                SELECT pk + 6 as pk, COUNT(pk )  *  3 as pk
                FROM test_bind_having_slots3  tbl_alias1
                GROUP by pk + 6
                HAVING  pk = 7
                """

        order_qt_having_bind_group_by """
                SELECT pk + 6, COUNT(pk )  *  3 as pk
                FROM test_bind_having_slots3  tbl_alias1
                GROUP by pk + 6
                HAVING  pk = 3
                """

        order_qt_having_bind_group_by """
                select pk + 1 as pk, pk + 2 as pk, count(*)
                from test_bind_having_slots3
                group by pk + 1, pk + 2
                having pk = 4;
                """

        order_qt_having_bind_group_by """
                select count(*) pk, pk + 1 as pk
                from test_bind_having_slots3
                group by pk + 1, pk + 2
                having pk = 1;
                """

        order_qt_having_bind_group_by """
                select pk + 1 as pk, count(*) pk
                from test_bind_having_slots3
                group by pk + 1, pk + 2
                having pk = 2;
                """
    }()

    def bindGroupBy = {
        sql "drop table if exists test_bind_groupby_slots"

        sql """create table test_bind_groupby_slots
                (id int, age int)
                distributed by hash(id)
                properties('replication_num'='1');
                """
        sql "insert into test_bind_groupby_slots values(1, 10), (2, 20), (3, 30);"

        order_qt_sql "select MIN (LENGTH (cast(age as varchar))), 1 AS col2 from test_bind_groupby_slots group by 2"
    }()



    def bindOrderBy = {
        sql "drop table if exists test_bind_orderby_slots"

        sql """create table test_bind_orderby_slots
                (id int, age int)
                distributed by hash(id)
                properties('replication_num'='1');
                """
        sql "insert into test_bind_orderby_slots values(1, 10), (2, 20), (3, 30);"

        order_qt_sql "select MIN (LENGTH (cast(age as varchar))), 1 AS col2 from test_bind_orderby_slots order by 2"
    }()
}
