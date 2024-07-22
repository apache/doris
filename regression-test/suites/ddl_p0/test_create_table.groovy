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

suite("sql_create_time_range_table") {
    def testTable = "test_time_range_table"

    sql "DROP TABLE IF EXISTS ${testTable}"

        // multi-line sql
    def result1 = sql """
        create table ${testTable} (
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

    // DDL/DML return 1 row and 1 column, the only value is update row count
    assertTrue(result1.size() == 1)
    assertTrue(result1[0].size() == 1)
    assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    sql "drop table if exists varchar_0_char_0"
    sql "create table varchar_0_char_0 (id int, a varchar(0), b char(0)) distributed by hash(id) properties(\"replication_num\"=\"1\")\n"
    def res_show = sql "show create table varchar_0_char_0"
    mustContain(res_show[0][1], "varchar(65533)")
    mustContain(res_show[0][1], "char(1)")
}
