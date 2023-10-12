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
        select coalesce(a, 'all') as a, count(*) as cnt from (select  null as a  union all  select  'a' as a ) t group by grouping sets ((a),()) order by a;
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
        exception "Column 'a' in field list is ambiguous"
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
}
