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

suite("test_transaction") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
	
	def db_name = "test_transaction_db"
    sql """DROP DATABASE IF EXISTS ${db_name}"""
    sql """CREATE DATABASE ${db_name}"""
    sql """use ${db_name}"""
	
    sql """  
       CREATE TABLE `test_employees`( `id` tinyint, `name` char(20) ) ENGINE=OLAP DUPLICATE KEY(`id`, `name`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" );
    """
	
	sql """begin;"""
    sql """insert into test_employees values (1, 'A_%');"""
	
	test {
        sql "select 1"
        exception "This is in a transaction, only insert, update, delete, commit, rollback is acceptable."
    }
	
	sql """insert into test_employees values (2, 'B_%');"""
	sql """commit;"""
	def result = sql """select * from test_employees;"""
	assertEquals(2, result.size())
	
	sql """begin;"""
	sql """insert into test_employees values (3, 'C_%');"""
	sql """rollback;"""
	result = sql """select * from test_employees;"""
	assertEquals(2, result.size())
	
	sql """begin;"""
	sql """delete from test_employees where id = 2;"""
	sql """commit;"""
	result = sql """select * from test_employees;"""
	assertEquals(1, result.size())
	
	sql """begin;"""
	sql """insert into test_employees values (2, 'B_%');"""
	sql """commit;"""
	result = sql """select * from test_employees;"""
	assertEquals(2, result.size())
	
	sql """begin;"""
	sql """insert into test_employees values (3, 'C_%');"""
	
	test {
        sql "select 1"
        exception "This is in a transaction, only insert, update, delete, commit, rollback is acceptable."
    }
	
	sql """rollback;"""
	result = sql """select * from test_employees;"""
	assertEquals(2, result.size())
	
	sql "DROP DATABASE IF EXISTS ${db_name}"
}