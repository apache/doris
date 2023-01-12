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

    sql """ drop table if exists outerjoin_A;"""
    sql """
        create table outerjoin_A ( a1 bigint not null, a2 bigint not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_B;"""
    sql """
        create table outerjoin_B ( b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_C;"""
    sql """
        create table outerjoin_C ( c int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_D;"""
    sql """
        create table outerjoin_D ( d1 int not null, d2 int not null, d3 int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(d1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_E;"""
    sql """
        create table outerjoin_E ( e1 int not null, e2 int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(e1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """insert into outerjoin_A values( 1,2 );"""
    sql """insert into outerjoin_B values( 1 );"""
    sql """insert into outerjoin_C values( 1 );"""
    sql """insert into outerjoin_D values( 1,2,3 );"""
    sql """insert into outerjoin_E values( 1,2 );"""

    qt_sql"""SELECT count(*)
            FROM outerjoin_A t1
            LEFT JOIN outerjoin_D dcbc
                ON t1.a1 = dcbc.d1
            LEFT JOIN outerjoin_C dcso
                ON dcbc.d2 = dcso.c
            LEFT JOIN outerjoin_B dcii
                ON t1.a2 = dcii.b
            LEFT JOIN outerjoin_E dcssm
                ON dcii.b = dcssm.e1
                    AND dcbc.d3 = dcssm.e2;
        """
}
