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

suite("eliminate_nullaware_anti_join") {
    multi_sql """
    SET enable_nereids_planner=true;
    SET enable_fallback_to_original_planner=false;
    set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
    """

    sql """drop table if exists eliminate_nullaware_anti_join_A;"""
    sql """drop table if exists eliminate_nullaware_anti_join_B;"""
    sql """drop table if exists eliminate_nullaware_anti_join_C;"""
    sql """
        create table eliminate_nullaware_anti_join_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table eliminate_nullaware_anti_join_B ( b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table eliminate_nullaware_anti_join_C ( c int )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    explain {
        sql("select * from eliminate_nullaware_anti_join_A where eliminate_nullaware_anti_join_A.a not in (select b from eliminate_nullaware_anti_join_B);")
        contains "LEFT ANTI JOIN"
    }

    explain {
        sql("select * from eliminate_nullaware_anti_join_C where eliminate_nullaware_anti_join_C.c not in (select b from eliminate_nullaware_anti_join_B);")
        contains "NULL AWARE LEFT ANTI JOIN"
    }

    explain {
        sql("select * from eliminate_nullaware_anti_join_A where eliminate_nullaware_anti_join_A.a not in (select c from eliminate_nullaware_anti_join_C);")
        contains "NULL AWARE LEFT ANTI JOIN"
    }
}

