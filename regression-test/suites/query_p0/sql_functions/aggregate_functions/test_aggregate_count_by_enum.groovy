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

suite("test_aggregate_count_by_enum") {
    sql "set enable_vectorized_engine = true"

    def tableName = "count_by_enum_test"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
	    CREATE TABLE IF NOT EXISTS ${tableName} (
	        `id` varchar(1024) NULL,
            `f1` text REPLACE_IF_NOT_NULL NULL,
            `f2` text REPLACE_IF_NOT_NULL NULL,
            `f3` text REPLACE_IF_NOT_NULL NULL
	    )
	    AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 3
	    PROPERTIES (
	      "replication_num" = "1"
	    )
    """

    sql "INSERT INTO ${tableName} values(1, \"F\", \"10\", \"China\"),(2, \"F\", \"20\", \"China\"),(3, \"M\", NULL, \"United States\"),(4, \"M\", NULL, \"United States\"),(5, \"M\", NULL, \"England\");"

    qt_select "select count_by_enum(f1) from ${tableName}"
    qt_select "select count_by_enum(f2) from ${tableName}"
    qt_select "select count_by_enum(f1,f2,f3) from ${tableName}"

    sql "DROP TABLE IF EXISTS ${tableName}"
}