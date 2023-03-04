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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_aggregate_bit") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 

    def tableName = "group_bit_test"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
	    CREATE TABLE IF NOT EXISTS ${tableName} (
	        flag varchar(10),
	        tint tinyint,
	        sint smallint,
	        `int` int,
	        bint bigint,
	        lint largeint
	    )
	    DISTRIBUTED BY HASH(flag) BUCKETS 1
	    PROPERTIES (
	      "replication_num" = "1"
	    )
    """

    //test without null and without group by
    sql "INSERT INTO ${tableName} values('a',44,44,44,44,44),('a',28,28,28,28,28),('b',15,15,15,15,15),('b',85,85,85,85,85);"

    qt_select "select group_bit_and(tint),group_bit_and(sint),group_bit_and(`int`),group_bit_and(bint),group_bit_and(lint) from ${tableName}"
    qt_select "select group_bit_or(tint),group_bit_or(sint),group_bit_or(`int`),group_bit_or(bint),group_bit_or(lint) from ${tableName}"
    qt_select "select group_bit_xor(tint),group_bit_xor(sint),group_bit_xor(`int`),group_bit_xor(bint),group_bit_xor(lint) from ${tableName}"

    //test without null and with group by
    qt_select "select flag, group_bit_and(tint),group_bit_and(sint),group_bit_and(`int`),group_bit_and(bint),group_bit_and(lint) from ${tableName} group by flag order by flag"
    qt_select "select flag, group_bit_or(tint),group_bit_or(sint),group_bit_or(`int`),group_bit_or(bint),group_bit_or(lint) from ${tableName} group by flag order by flag"
    qt_select "select flag, group_bit_xor(tint),group_bit_xor(sint),group_bit_xor(`int`),group_bit_xor(bint),group_bit_xor(lint) from ${tableName} group by flag order by flag"

    //test with negative number
    sql "truncate table ${tableName}"
    sql "INSERT INTO ${tableName} values('a',-44,-44,-44,-44,-44),('a',28,28,28,28,28),('b',15,15,15,15,15),('b',85,85,85,85,85);"

    qt_select "select group_bit_and(tint),group_bit_and(sint),group_bit_and(`int`),group_bit_and(bint),group_bit_and(lint) from ${tableName}"
    qt_select "select group_bit_or(tint),group_bit_or(sint),group_bit_or(`int`),group_bit_or(bint),group_bit_or(lint) from ${tableName}"
    qt_select "select group_bit_xor(tint),group_bit_xor(sint),group_bit_xor(`int`),group_bit_xor(bint),group_bit_xor(lint) from ${tableName}"

    //test with null
    sql "truncate table ${tableName}"
    sql "INSERT INTO ${tableName} values('a',NULL,NULL,NULL,NULL,NULL),('a',28,28,28,28,28),('b',15,15,15,15,15),('b',85,85,85,85,85);"

    qt_select "select group_bit_and(tint),group_bit_and(sint),group_bit_and(`int`),group_bit_and(bint),group_bit_and(lint) from ${tableName}"
    qt_select "select group_bit_or(tint),group_bit_or(sint),group_bit_or(`int`),group_bit_or(bint),group_bit_or(lint) from ${tableName}"
    qt_select "select group_bit_xor(tint),group_bit_xor(sint),group_bit_xor(`int`),group_bit_xor(bint),group_bit_xor(lint) from ${tableName}"

    sql "DROP TABLE IF EXISTS ${tableName}"
}