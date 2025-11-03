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

suite("join_with_alias") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """ drop table if exists table_A_alias;"""
    sql """
        create table table_A_alias ( a bigint not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists table_B_alias;"""
    sql """
        create table table_B_alias ( b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """insert into table_A_alias values( 1 );"""
    sql """insert into table_B_alias values( 1 );"""

    qt_sql"""
            select a1, a2 from ( select a as a1, a as a2 from table_A_alias join table_B_alias on a = b ) t;
        """

    qt_sql2"""
            select table_B_alias.b from table_B_alias where table_B_alias.b in ( select a from table_A_alias );
        """

    sql """ drop table if exists table_A_alias;"""

    sql """ drop table if exists table_B_alias;"""
}
