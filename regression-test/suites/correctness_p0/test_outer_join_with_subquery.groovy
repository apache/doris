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
}
