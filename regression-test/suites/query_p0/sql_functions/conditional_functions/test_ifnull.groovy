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

suite("test_ifnull") {
	def tbName = "test_ifnull"
	sql "DROP TABLE IF EXISTS ${tbName};"
	sql"""
		CREATE TABLE IF NOT EXISTS ${tbName} (
			id int(11) NULL,
			t_decimal DECIMALV3(26, 9) NULL,
			test_double double NULL
		) ENGINE = OLAP
		DUPLICATE KEY(id)
		DISTRIBUTED BY HASH(id) BUCKETS 1
		PROPERTIES (
		"replication_allocation" = "tag.location.default: 1");
	"""
	sql"""
		INSERT INTO test_ifnull VALUES(1,11111.11111,2222.22222);
	"""

	qt_sql "select id,t_decimal,test_double,ifnull(t_decimal,test_double) as if_dou,ifnull(test_double,t_decimal) as if_dei from test_ifnull;"

	test{
		sql"""select ifnull(kstr) from fn_test"""
		check {result, exception, startTime, endTime ->
			assertTrue(exception != null)
			logger.info(exception.message)
		}
	}

	sql "DROP TABLE ${tbName};"
}

