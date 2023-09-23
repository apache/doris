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


suite("test_truncate_table") {
    def testTable = "test_truncate_table"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE ${testTable}
		(
			k1 DATE,
			k2 DECIMAL(10, 2) DEFAULT "10.5",
			k3 CHAR(10) COMMENT "string column",
			k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
		)
		PARTITION BY RANGE(k1)
		(
			PARTITION p1 VALUES LESS THAN ("2020-02-01"),
			PARTITION p2 VALUES LESS THAN ("2020-03-01"),
			PARTITION p3 VALUES LESS THAN ("2020-04-01")
		)
		DISTRIBUTED BY HASH(k2) BUCKETS 32
		PROPERTIES (
			"replication_num" = "1"
		);
		"""
    List<List<Object>> result = sql "show partitions from ${testTable}"
    logger.info("${result}")
    assertEquals(result.size(), 3)
    assertEquals(result.get(0).get(1), "p1")	

    sql """truncate table ${testTable};"""
    result = sql "show partitions from ${testTable}"
    logger.info("${result}")
    assertEquals(result.size(), 3)
    assertEquals(result.get(0).get(1), "p1")

    sql """truncate table ${testTable} partitions (p1, p1);"""

    result = sql "show partitions from ${testTable}"
    logger.info("${result}")
    assertEquals(result.size(), 3)
    assertEquals(result.get(0).get(1), "p1")
	
    sql "DROP TABLE IF EXISTS ${testTable}"
}

