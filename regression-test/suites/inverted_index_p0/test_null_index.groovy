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


suite("test_null_index", "p0"){
    // prepare test table


    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "no_index_test"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
	    `id` int(11) NOT NULL,
	    `str` string NOT NULL,
	    `str_null` string NULL,
            `value` array<text> NOT NULL,
            `value_int` array<int> NOT NULL
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 	    "replication_allocation" = "tag.location.default: 1"
	);
    """
    
    sql "INSERT INTO $indexTblName VALUES (1, 'a', null, [null], [1]), (2, 'b', 'b', ['b'], [2]), (3, 'c', 'c', ['c'], [3]);"
    qt_sql "SELECT * FROM $indexTblName WHERE str match null order by id;"
    qt_sql "SELECT * FROM $indexTblName WHERE str_null match null order by id;"
}
