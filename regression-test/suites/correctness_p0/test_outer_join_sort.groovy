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

suite("test_outer_join_sort") {
    sql """
        drop table if exists test_test_outer_join_sort_outerjoin_A;
    """

    sql """
        drop table if exists test_outer_join_sort_outerjoin_B;
    """

    sql """
        drop table if exists outerjoin_C;
    """

    sql """
        create table if not exists test_test_outer_join_sort_outerjoin_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists test_outer_join_sort_outerjoin_B ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists outerjoin_C ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_test_outer_join_sort_outerjoin_A values( 1 );
    """

    sql """
        insert into test_outer_join_sort_outerjoin_B values( 1 );
    """

    sql """
        insert into outerjoin_C values( 1 );
    """

    qt_select """
        select  
        bitand(
        test_test_outer_join_sort_outerjoin_A.`a` ,
        test_test_outer_join_sort_outerjoin_A.`a` ) as c0
        from 
            test_test_outer_join_sort_outerjoin_A

            inner join (select  
                test_outer_join_sort_outerjoin_B.a as c3
                from 
                test_outer_join_sort_outerjoin_B
                    ) as subq_0
            on (test_test_outer_join_sort_outerjoin_A.a = subq_0.c3 )
            right join outerjoin_C as ref_83
            on (subq_0.c3 = ref_83.a )
        order by subq_0.`c3`;
    """

    qt_select """
        select
        case
            when outerjoin_C.a is  NULL then subq_0.`c1`
            else subq_0.`c2`
        end as c0
        from
        (
            select
            1 as c0,
            version() as c1,
            a as c2
            from
            test_test_outer_join_sort_outerjoin_A
        ) as subq_0
        right join outerjoin_C on (subq_0.`c0` = outerjoin_C.a)
        order by
        subq_0.`c1`;
    """

    sql """
        drop table if exists test_test_outer_join_sort_outerjoin_A;
    """

    sql """
        drop table if exists test_outer_join_sort_outerjoin_B;
    """

    sql """
        drop table if exists outerjoin_C;
    """
}
