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

suite("test_join5", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def DBname = "regression_test_join5"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    def tbName1 = "tt3"
    def tbName2 = "tt4"
    def tbName3 = "tt4x"

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"
    sql "DROP TABLE IF EXISTS ${tbName3};"

    sql """create table if not exists ${tbName1} (f1 int, f2 text) DISTRIBUTED BY HASH(f1) properties("replication_num" = "1");"""
    sql """create table if not exists ${tbName2} (f1 int) DISTRIBUTED BY HASH(f1) properties("replication_num" = "1");"""
    sql """create table if not exists ${tbName3} (c1 int, c2 int, c3 int) DISTRIBUTED BY HASH(c1) properties("replication_num" = "1");"""

    sql "insert into ${tbName1} values (1,null);"
    sql "insert into ${tbName1} values (null,null);"
    sql "insert into ${tbName2} values (0),(1),(9999);"
    sql "insert into ${tbName3} values (0,1,9999);"

    qt_join1 """
            SELECT a.f1
            FROM ${tbName2} a
            LEFT JOIN (
                    SELECT b.f1
                    FROM ${tbName1} b LEFT JOIN ${tbName1} c ON (b.f1 = c.f1)
                    WHERE c.f1 IS NULL
            ) AS d ON (a.f1 = d.f1)
            WHERE d.f1 IS NULL
            ORDER BY 1;
            """

    qt_join2 """
            select * from ${tbName3} t1
            where not exists (
              select 1 from ${tbName3} t2
                left join ${tbName3} t3 on t2.c3 = t3.c1
                left join ( select t5.c1 as c1
                            from ${tbName3} t4 left join ${tbName3} t5 on t4.c2 = t5.c1
                          ) a1 on t3.c2 = a1.c1
              where t1.c1 = t2.c2
            )
            ORDER BY 1;
            """

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName3};"

    def tbName4 = "uq1"
    def tbName5 = "uq2"
    def tbName6 = "uq3"
    def tbName7 = "uqv1"

    sql "DROP TABLE IF EXISTS ${tbName4};"
    sql "DROP TABLE IF EXISTS ${tbName5};"
    sql "DROP TABLE IF EXISTS ${tbName6};"


    sql """create table if not exists ${tbName4} (f1 int) UNIQUE KEY (f1) DISTRIBUTED BY HASH(f1) properties("replication_num" = "1");"""
    sql """create table if not exists ${tbName5} (f2 int) UNIQUE KEY (f2) DISTRIBUTED BY HASH(f2) properties("replication_num" = "1");"""
    sql """create table if not exists ${tbName6} (f3 int) UNIQUE KEY (f3) DISTRIBUTED BY HASH(f3) properties("replication_num" = "1");"""

    sql "insert into ${tbName4} values(53);"
    sql "insert into ${tbName5} values(53);"

    qt_join3 """
            select * from
            ${tbName5} left join ${tbName6} on (f2 = f3)
            left join ${tbName4} on (f3 = f1)
            where f2 = 53;
        """

    sql "create view ${tbName7} as select f1 from ${tbName4};"

    qt_join4 """
            select * from
            ${tbName5} left join ${tbName6} on (f2 = f3)
            left join ${tbName7} on (f3 = f1)
            where f2 = 53;
        """

    sql "DROP TABLE IF EXISTS ${tbName4};"
    sql "DROP TABLE IF EXISTS ${tbName5};"
    sql "DROP TABLE IF EXISTS ${tbName6};"


    sql """ create table if not exists a (code char not null) UNIQUE KEY (code) DISTRIBUTED BY HASH(code) properties("replication_num" = "1");"""
    sql """ create table if not exists b (a char not null, num integer not null) UNIQUE KEY (a,num) DISTRIBUTED BY HASH(a) properties("replication_num" = "1");"""
    sql """ create table if not exists c (name char not null, a char) UNIQUE KEY (name) DISTRIBUTED BY HASH(name) properties("replication_num" = "1");""";

    sql " insert into a (code) values ('p');"
    sql " insert into a (code) values ('q');"
    sql " insert into b (a, num) values ('p', 1);"
    sql " insert into b (a, num) values ('p', 2);"
    sql " insert into c (name, a) values ('A', 'p');"
    sql " insert into c (name, a) values ('B', 'q');"
    sql " insert into c (name, a) values ('C', null);"

    qt_join5 """
        select c.name, ss.code, ss.b_cnt, ss.const
        from c left join
          (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
           from a left join
             (select count(1) as cnt, b.a from b group by b.a) as b_grp
             on a.code = b_grp.a
          ) as ss
          on (c.a = ss.code)
        order by c.name;
        """

    qt_join5 """
        SELECT * FROM
            ( SELECT 1 as key1 ) sub1
            LEFT JOIN
            ( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
                ( SELECT 1 as key3 ) sub3
                LEFT JOIN
                ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
                    ( SELECT 1 as key5 ) sub5
                    LEFT JOIN
                    ( SELECT 2 as key6, 42 as value1 ) sub6
                    ON sub5.key5 = sub6.key6
                ) sub4
                ON sub4.key5 = sub3.key3
            ) sub2
            ON sub1.key1 = sub2.key3;
            """

    qt_join6 """
            SELECT * FROM
            ( SELECT 1 as key1 ) sub1
            LEFT JOIN
            ( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
                ( SELECT 1 as key3 ) sub3
                LEFT JOIN
                ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
                    ( SELECT 1 as key5 ) sub5
                    LEFT JOIN
                    ( SELECT 2 as key6, 42 as value1 ) sub6
                    ON sub5.key5 = sub6.key6
                ) sub4
                ON sub4.key5 = sub3.key3
            ) sub2
            ON sub1.key1 = sub2.key3;
            """

    sql "DROP DATABASE IF EXISTS ${DBname};"
}
