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

suite("test_like_with_escape") {
    def db_name = "test_like_with_escape_db"

    sql """DROP DATABASE IF EXISTS ${db_name}"""

    sql """CREATE DATABASE ${db_name}"""
    sql """use ${db_name}"""
	
    sql """  
       CREATE TABLE `test_employees`( `id` tinyint, `name` char(20) ) ENGINE=OLAP DUPLICATE KEY(`id`, `name`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" );
    """
	
    sql "insert into test_employees values (1, 'A_%'),(2, 'B_%'),(3, 'C_D'),(4, 'E_F'),(5, 'F_%');"

	def result = sql "select * from test_employees where name like '%\_\%' escape '\\';"
	assertEquals(3, result.size())
	
	result = sql "select * from test_employees where name like '%|_|%' escape '|';"
	assertEquals(3, result.size())
	
	result = sql "select * from test_employees where name like '%@_@%' escape '@';"
	assertEquals(3, result.size())
	
	result = sql "select * from test_employees where name like '%#_#%' escape '#';"
	assertEquals(3, result.size())

	sql "DROP DATABASE IF EXISTS ${db_name}"
}
