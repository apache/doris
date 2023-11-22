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


suite("test_truncate_table1") {
    // test truncate partition table which has no partition
    def testTable = "test_truncate_no_partition_table"
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
		()
		DISTRIBUTED BY HASH(k2) BUCKETS 1
		PROPERTIES (
			"replication_num" = "1"
		);
		"""
    List<List<Object>> result = sql "show partitions from ${testTable}"
    logger.info("${result}")
    assertEquals(result.size(), 0)

    sql """truncate table ${testTable};"""
    result = sql "show partitions from ${testTable}"
    logger.info("${result}")
    assertEquals(result.size(), 0)
	
    sql "DROP TABLE IF EXISTS ${testTable}"



    // test truncate non partition table
    def testNonPartitionTable = "test_truncate_non_partition_table"
    sql "DROP TABLE IF EXISTS ${testNonPartitionTable}"
    sql """
        CREATE TABLE ${testNonPartitionTable}
		(
			k1 DATE,
			k2 DECIMAL(10, 2) DEFAULT "10.5",
			k3 CHAR(10) COMMENT "string column",
			k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
		)
		DISTRIBUTED BY HASH(k2) BUCKETS 1
		PROPERTIES (
			"replication_num" = "1"
		);
		"""
    List<List<Object>> result1 = sql "show partitions from ${testNonPartitionTable}"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)

    sql """truncate table ${testNonPartitionTable};"""
    result1 = sql "show partitions from ${testNonPartitionTable}"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
	
    sql "DROP TABLE IF EXISTS ${testNonPartitionTable}"
}

