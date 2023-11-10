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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("sql_force_drop") {
    def testTable = "test_force_drop"

    sql "CREATE DATABASE IF NOT EXISTS test_force_drop_database"
    sql """
        CREATE TABLE IF NOT EXISTS test_force_drop_database.table1 (
            `actorid` varchar(128),
            `gameid` varchar(128),
            `eventtime` datetimev2(3)
        )
        engine=olap
        duplicate key(actorid, gameid, eventtime)
        partition by range(eventtime)(
            from ("2000-01-01") to ("2021-01-01") interval 1 year,
            from ("2021-01-01") to ("2022-01-01") interval 1 MONth,
            from ("2022-01-01") to ("2023-01-01") interval 1 WEEK,
            from ("2023-01-01") TO ("2023-02-01") interval 1 DAY
        )
        distributed by hash(actorid) buckets 1
        properties(
            "replication_num"="1",
            "light_schema_change"="true",
            "compression"="zstd"
        );
    """
    sql """
        CREATE TABLE IF NOT EXISTS test_force_drop_database.table2 (
            `actorid` varchar(128),
            `gameid` varchar(128),
            `eventtime` datetimev2(3)
        )
        engine=olap
        duplicate key(actorid, gameid, eventtime)
        partition by range(eventtime)(
            from ("2000-01-01") to ("2021-01-01") interval 1 year,
            from ("2021-01-01") to ("2022-01-01") interval 1 MONth,
            from ("2022-01-01") to ("2023-01-01") interval 1 WEEK,
            from ("2023-01-01") TO ("2023-02-01") interval 1 DAY
        )
        distributed by hash(actorid) buckets 1
        properties(
            "replication_num"="1",
            "light_schema_change"="true",
            "compression"="zstd"
        );
    """

    sql " drop table test_force_drop_database.table2 "
    sql " recover table test_force_drop_database.table2 "
    sql " drop table test_force_drop_database.table2 FORCE"
    sql " drop database test_force_drop_database "
    sql " recover database test_force_drop_database "
    sql " drop database test_force_drop_database FORCE"
}

