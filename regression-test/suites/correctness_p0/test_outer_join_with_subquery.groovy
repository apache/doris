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

suite("test_outer_join_with_subquery") {
    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_A;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_B;
    """
    
    sql """
        create table test_outer_join_with_subquery_outerjoin_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table test_outer_join_with_subquery_outerjoin_B ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_outer_join_with_subquery_outerjoin_A values( 1 );
    """

    sql """
        insert into test_outer_join_with_subquery_outerjoin_B values( 1 );
    """

    qt_select """
        select
        subq_1.c1
        from
        (
            select
            case
                when test_outer_join_with_subquery_outerjoin_A.a is NULL then test_outer_join_with_subquery_outerjoin_A.a
                else test_outer_join_with_subquery_outerjoin_A.a
            end as c1
            from
            test_outer_join_with_subquery_outerjoin_A
        ) as subq_1
        full join (
            select
            case
                when test_outer_join_with_subquery_outerjoin_B.a is NULL then test_outer_join_with_subquery_outerjoin_B.a
                else test_outer_join_with_subquery_outerjoin_B.a
            end as c2
            from
            test_outer_join_with_subquery_outerjoin_B
        ) as subq_2 on (subq_1.c1 = subq_2.c2);
    """

    qt_select"""
        with idm_org_table as (
        SELECT 
            1 a
        ) 
        select 
        a 
        from 
        test_outer_join_with_subquery_outerjoin_A 
        where 
        a in (
            SELECT 
            a 
            from 
            test_outer_join_with_subquery_outerjoin_B 
            where 
            a = (
                select 
                a 
                from 
                idm_org_table
            )
        );
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_A;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_B;
    """

    ////////////////////////////////////

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_A_1;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_B_1;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_C_1;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_D_1;
    """

    sql """
        CREATE TABLE test_outer_join_with_subquery_outerjoin_A_1(user_id int NULL) 
        ENGINE = OLAP DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1 PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """    

    sql """
        CREATE TABLE test_outer_join_with_subquery_outerjoin_B_1(user_id int NULL) 
        ENGINE = OLAP DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1 PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """

    sql """
        CREATE TABLE test_outer_join_with_subquery_outerjoin_C_1(user_id int NULL) 
        ENGINE = OLAP DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1 PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """

    sql """
        CREATE TABLE test_outer_join_with_subquery_outerjoin_D_1(user_id int NULL) 
        ENGINE = OLAP DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1 PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
    """

    qt_select """
        WITH W1 AS (
            SELECT
                t.user_id
            FROM
                test_outer_join_with_subquery_outerjoin_A_1 t
                JOIN test_outer_join_with_subquery_outerjoin_B_1 u ON u.user_id = t.user_id
        ),
        W2 AS (
            SELECT
                t.user_id
            FROM
                test_outer_join_with_subquery_outerjoin_C_1 t
                JOIN test_outer_join_with_subquery_outerjoin_B_1 u ON u.user_id = t.user_id
            GROUP BY
                t.user_id
        ),
        W3 AS (
            SELECT
                t.user_id
            FROM
                test_outer_join_with_subquery_outerjoin_D_1 t
                JOIN test_outer_join_with_subquery_outerjoin_B_1 u ON u.user_id = t.user_id
            GROUP BY
                t.user_id
        )
        SELECT
            COUNT(dataset_30.reg_user_cnt) AS u_18efaea722a8747c_2
        FROM
            (
                SELECT
                    t1.reg_user_cnt,
                    t2.commit_case_user_cnt,
                    t3.watch_live_course_and_commit_case_user_cnt
                FROM
                    (
                        SELECT
                            COUNT(user_id) AS reg_user_cnt,
                            '1' AS flag
                        FROM
                            W1
                    ) t1
                    JOIN (
                        SELECT
                            COUNT(user_id) AS commit_case_user_cnt,
                            '1' AS flag
                        FROM
                            W2
                    ) t2 ON t2.flag = t1.flag
                    JOIN (
                        SELECT
                            COUNT(t1.user_id) AS watch_live_course_and_commit_case_user_cnt,
                            '1' AS flag
                        FROM
                            W2 t1
                            JOIN W3 t2 ON t2.user_id = t1.user_id
                    ) t3 ON t3.flag = t1.flag
            ) dataset_30
        LIMIT
            1000;
    """        

    qt_select """
        select 'aaa' from(  select 'bbb' FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """    

    qt_select """
        select 'aaa' from(  select 'bbb' ,count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """    

    qt_select """
        select count(1) from(  select 'bbb' ,count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1), 'aaa' from(  select 'bbb' ,count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1) from(  select 'bbb' FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1), 'aaa' from(  select count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1), 'aaa' from(  select 'bbb' FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1), 'aaa' from(  select count(1) FROM (  select count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1), 'aaa' from(  select 'bbb' FROM (  select 'bbb' FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1) from(  select count(1) FROM (  select count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1) from(  select 'bbb' FROM (  select 'bbb' FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1) from(  select 'bbb',count(1) FROM (  select 'bbb',count(1) FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1), max(user_id) from(  select user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1), max(user_id) from(  select user_id FROM (  select user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1), max(user_id) from(  select count(1), 100 as user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select count(1), max(user_id) from(  select 100 as user_id FROM (  select max(user_id) FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select count(1), max(user_id) from(  select user_id FROM (  select count(1) as user_id, 'abc' FROM test_outer_join_with_subquery_outerjoin_A_1 )s  )t;
    """

    qt_select """
        select count(1), max(user_id) from(  select max(user_id), 100 as user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

qt_select """
        select max(user_id) from(  select user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select max(user_id) from(  select user_id FROM (  select user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select max(user_id) from(  select count(1), 100 as user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    qt_select """
        select max(user_id) from(  select 100 as user_id FROM (  select max(user_id) FROM test_outer_join_with_subquery_outerjoin_A_1  )s  )t;
    """

    qt_select """
        select max(user_id) from(  select user_id FROM (  select count(1) as user_id, 'abc' FROM test_outer_join_with_subquery_outerjoin_A_1 )s  )t;
    """

    qt_select """
        select max(user_id) from(  select max(user_id), 100 as user_id FROM test_outer_join_with_subquery_outerjoin_A_1  )t;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_A_1;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_B_1;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_C_1;
    """

    sql """
        drop table if exists test_outer_join_with_subquery_outerjoin_D_1;
    """
}

