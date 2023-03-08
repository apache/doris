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
    def db = "test_double"
    sql "CREATE DATABASE IF NOT EXISTS ${db}"
    sql "use ${db}"
    sql "drop table if exists test5"
	sql '''CREATE  TABLE test5 (id int, d double) ENGINE=OLAP UNIQUE KEY(id) COMMENT 'OLAP' DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" ) '''
	sql "insert into test5 values(1,15.025)"
	sql "insert into test5 values(2,15.024999999999)"
	sql "insert into test5 values(3,15.33)"
	sql "insert into test5 values(4,15.999999995)"
	sql "insert into test5 values(5,15.9999)"
	sql "insert into test5 values(6,0.9999)"
	sql "insert into test5 values(11,-15.025)"
	sql "insert into test5 values(12,-15.024999999999)"
	sql "insert into test5 values(13,-15.33)"
	sql "insert into test5 values(14,-15.999999995)"
	sql "insert into test5 values(15,-15.9999)"
	sql "insert into test5 values(16,-0.9999)"

	def keyAndExcpectedValues = [
		["1", "15.025", "15.025", "15.02500000","15.02500000"],
		["2", "15.025", "15.025", "15.02500000","15.02500000"],
		["3", "15.330", "15.330", "15.33000000","15.33000000"],
		["4", "16.000", "16.000", "16.00000000","16.00000000"],
		["5", "16.000", "16.000", "15.99990000","15.99990000"],
		["6", "1.000", "1.000", "0.99990000","0.99990000"],
		["11", "-15.025", "-15.025", "-15.02500000","-15.02500000"],
		["12", "-15.025", "-15.025", "-15.02500000","-15.02500000"],
		["13", "-15.330", "-15.330", "-15.33000000","-15.33000000"],
		["14", "-16.000", "-16.000", "-16.00000000","-16.00000000"],
		["15", "-16.000", "-16.000", "-15.99990000","-15.99990000"],
		["16", "-1.000", "-1.000", "-0.99990000","-0.99990000"]
	]
	
	keyAndExcpectedValues.each { keyAndExcpetedValue ->
		println(keyAndExcpetedValue)
		logger.info("convert double to decimal test key: ${keyAndExcpetedValue[0]} -> ${keyAndExcpetedValue[1]} ${keyAndExcpetedValue[2]} ${keyAndExcpetedValue[3]} ${keyAndExcpetedValue[4]}")
		def s = "select cast(d as decimal(10,3)), cast(d as decimalv3(10,3)), cast(d as decimal(10,8)), cast(d as decimalv3(10,8)) from test5 where id=" + keyAndExcpetedValue[0]
		def result = sql s
		logger.info("sql result: ${result[0][0]} ${result[0][1]} ${result[0][2]} ${result[0][3]}")

		for (int i=0 ;i<4 ;i++) {
			//assertEquals(keyAndExcpetedValue[i+1], result[0][i].toString())
			assertEquals(keyAndExcpetedValue[i+1], result[0][i].toString())
		}
	}	

	sql "drop table if exists test5"
	sql "drop database if exists test_double"
}
