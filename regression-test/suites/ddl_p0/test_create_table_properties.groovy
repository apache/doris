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

suite("test_create_table_properties") {    
	try {
		sql """
		create table test_create_table_properties (
			`user_id` LARGEINT NOT NULL COMMENT "用户id",
			`start_time` DATETIME,
			`billing_cycle_id` INT
		)
		partition by range(`billing_cycle_id`, `start_time`)(
			PARTITION p202201_otr VALUES [("202201", '2000-01-01 00:00:00'), ("202201", '2022-01-01 00:00:00')),
			PARTITION error_partition VALUES [("999999", '1970-01-01 00:00:00'), ("999999", '1970-01-02 00:00:00'))
		)
		distributed by hash(`user_id`) buckets 1
		properties(
			"replication_num"="1",
			"abc"="false"
		);
			"""
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unknown properties"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }
}
