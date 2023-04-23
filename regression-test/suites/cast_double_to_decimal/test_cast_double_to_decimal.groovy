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

suite("test_cast_double_to_decimal") {
	def tableName = "tbl_test_double_to_decimal"    
	sql "DROP TABLE IF EXISTS ${tableName}"
	sql "CREATE  TABLE if NOT EXISTS ${tableName} (id int, d double) ENGINE=OLAP UNIQUE KEY(id) COMMENT 'OLAP' DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ( 'replication_allocation' = 'tag.location.default: 1' ) "
	sql """insert into ${tableName} values  (1,15.025),
										  	(2,15.024999999999),
										  	(3,15.33),
										  	(4,15.999999995),
											(5,15.9999),
											(6,0.9999),
											(7,15.0245),
											(11,-15.025),
											(12,-15.024999999999),
											(13,-15.33),
											(14,-15.999999995),
											(15,-15.9999),
											(16,-0.9999),
											(17,-15.0245)"""
	
	qt_select "select cast(d as decimal(10,3)), cast(d as decimalv3(10,3)), cast(d as decimal(10,8)), cast(d as decimalv3(10,8)) from ${tableName} order by id"

	sql "DROP TABLE IF EXISTS ${tableName}"
}
