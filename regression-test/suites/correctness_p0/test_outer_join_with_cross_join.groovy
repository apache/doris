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

suite("test_outer_join_with_cross_join") {
    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_A;
    """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_B;
    """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_C;
    """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_D;
    """
    
    sql """
        create table if not exists test_outer_join_with_cross_join_outerjoin_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists test_outer_join_with_cross_join_outerjoin_B ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists test_outer_join_with_cross_join_outerjoin_C ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists test_outer_join_with_cross_join_outerjoin_D ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_outer_join_with_cross_join_outerjoin_A values( 1 );
    """

    sql """
        insert into test_outer_join_with_cross_join_outerjoin_B values( 1 );
    """

    sql """
        insert into test_outer_join_with_cross_join_outerjoin_C values( 1 );
    """

    sql """
        insert into test_outer_join_with_cross_join_outerjoin_D values( 1 );
    """

    qt_select """
        select test_outer_join_with_cross_join_outerjoin_B.a from test_outer_join_with_cross_join_outerjoin_A left join test_outer_join_with_cross_join_outerjoin_B on test_outer_join_with_cross_join_outerjoin_A.a = test_outer_join_with_cross_join_outerjoin_B.a 
        inner join test_outer_join_with_cross_join_outerjoin_C on true left join test_outer_join_with_cross_join_outerjoin_D on test_outer_join_with_cross_join_outerjoin_B.a = test_outer_join_with_cross_join_outerjoin_D.a;
    """

    qt_select2 """
        select
        subq_0.`c3` as c0
        from
        (
            select
            ref_0.a as c3,
            unhex(cast(version() as varchar)) as c4
            from
            test_outer_join_with_cross_join_outerjoin_A as ref_0
        ) as subq_0
        right join test_outer_join_with_cross_join_outerjoin_B as ref_3 on (subq_0.`c3` = ref_3.a)
        inner join test_outer_join_with_cross_join_outerjoin_C as ref_4 on true;
    """

    qt_select3 """
                WITH a As(
                    select
                        (case
                            when '年' = '年' then DATE_FORMAT(date_sub(concat('2023', '-01-01'), interval 0 year), '%Y')

                            end) as startdate,
                        (case
                            when '年' = '年' then DATE_FORMAT(date_sub(concat('2023', '-01-01'), interval 0 year), '%Y')

                            end) as enddate
                )
                select * from test_outer_join_with_cross_join_outerjoin_A DMR_POTM cross join a
                right join ( select distinct a from test_outer_join_with_cross_join_outerjoin_B ) DD
                on DMR_POTM.a =DD.a;
                """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_A;
    """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_B;
    """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_C;
    """

    sql """
        drop table if exists test_outer_join_with_cross_join_outerjoin_D;
    """
}
