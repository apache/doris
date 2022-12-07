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

suite("test_aggregate_histogram") {
    sql "set enable_vectorized_engine = true"

    def tableName = "histogram_test"
    def tableCTAS = "histogram_test_ctas"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableCTAS}"

    sql """
	    CREATE TABLE IF NOT EXISTS ${tableName} (
	        c_int INT,
	        c_string VARCHAR(10),
	        c_date Date,
	        c_decimal DECIMAL(10, 2),
	        c_string_not_null VARCHAR(10) NOT NULL
	    )
	    DISTRIBUTED BY HASH(c_int) BUCKETS 1
	    PROPERTIES (
	        "replication_num" = "1"
	    ) 
    """

    sql "INSERT INTO ${tableName} values(1,'hello','2022-07-04',1.23,'hello'), (2,NULL,NULL,NULL,'hello')"
    sql "INSERT INTO ${tableName} values(1,'hello','2022-07-04',1.23,'hello'), (2,NULL,NULL,NULL,'hello')"

    qt_select "select histogram(c_string),histogram(c_string_not_null) from ${tableName}"
    qt_select "select c_int,histogram(c_string),histogram(c_date),histogram(c_decimal) from ${tableName} group by c_int order by c_int"

    sql """ CREATE TABLE ${tableCTAS} PROPERTIES("replication_num" = "1") AS SELECT 1,histogram(c_int),histogram(c_string),histogram(c_date),histogram(c_decimal),histogram(c_string_not_null) FROM ${tableName} """
    qt_select "SELECT * from ${tableCTAS}"

     sql "DROP TABLE IF EXISTS ${tableName}"
     sql "DROP TABLE IF EXISTS ${tableCTAS}"
}
