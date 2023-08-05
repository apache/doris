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

suite("join_order") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """ drop table if exists outerjoin_A_order;"""
    sql """
        create table outerjoin_A_order ( a1 bigint not null, a2 bigint not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_B_order;"""
    sql """
        create table outerjoin_B_order ( b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_C_order;"""
    sql """
        create table outerjoin_C_order ( c int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_D_order;"""
    sql """
        create table outerjoin_D_order ( d1 int not null, d2 int not null, d3 int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(d1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_E_order;"""
    sql """
        create table outerjoin_E_order ( e1 int not null, e2 int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(e1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """insert into outerjoin_A_order values( 1,2 );"""
    sql """insert into outerjoin_B_order values( 1 );"""
    sql """insert into outerjoin_C_order values( 1 );"""
    sql """insert into outerjoin_D_order values( 1,2,3 );"""
    sql """insert into outerjoin_E_order values( 1,2 );"""

    qt_sql"""SELECT count(*)
            FROM outerjoin_A_order t1
            LEFT JOIN outerjoin_D_order dcbc
                ON t1.a1 = dcbc.d1
            LEFT JOIN outerjoin_C_order dcso
                ON dcbc.d2 = dcso.c
            LEFT JOIN outerjoin_B_order dcii
                ON t1.a2 = dcii.b
            LEFT JOIN outerjoin_E_order dcssm
                ON dcii.b = dcssm.e1
                    AND dcbc.d3 = dcssm.e2;
        """

    sql 'set disable_join_reorder=true;'
    explain {
        sql("select * from outerjoin_A_order, outerjoin_B_order, outerjoin_C_order where outerjoin_A_order.a1 = outerjoin_C_order.c and outerjoin_B_order.b = outerjoin_C_order.c;")
        contains "CROSS JOIN"
    }

    sql 'set disable_join_reorder=false;'
    explain {
        sql("select * from outerjoin_A_order, outerjoin_B_order, outerjoin_C_order where outerjoin_A_order.a1 = outerjoin_C_order.c and outerjoin_B_order.b = outerjoin_C_order.c;")
        notContains "CROSS JOIN"
    }

    qt_sql2 """SELECT 
                    subq_0.`c1`,
                    subq_0.`c0`
                FROM 
                    (SELECT ref_1.b AS c0,
                        ref_7.a1 AS c1
                    FROM outerjoin_B_order AS ref_1
                    INNER JOIN outerjoin_A_order AS ref_7
                        ON (true) order by ref_7.a2) AS subq_0 order by 1, 2;"""
}
