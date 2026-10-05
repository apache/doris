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


suite("test_truncate_table_nereids") {
    def testTable = "test_truncate_table_with_partition_nereids"

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

    def getPartitionIds = { ->
        def result = sql_return_maparray("show partitions from ${testTable}")
        return result.collectEntries { [it.PartitionName, it.PartitionId as long] }
    }

    def partitionIds1 = getPartitionIds()
    assertEquals(["p1", "p2", "p3"].toSet(), partitionIds1.keySet())

    sql "insert into ${testTable} values ('2020-01-01', 1.0, 'a', 1)"
    sql "insert into ${testTable} values ('2020-03-10', 1.0, 'a', 1)"
    order_qt_select_1 "SELECT * FROM ${testTable}"

    // if truncate without force, empty partions also kept in recycle bin.
    checkNereidsExecute("truncate table ${testTable};")

    def partitionIds2 = getPartitionIds()
    assertEquals(["p1", "p2", "p3"].toSet(), partitionIds2.keySet())
    assertNotEquals(partitionIds1.get("p1"), partitionIds2.get("p1"))
    assertNotEquals(partitionIds1.get("p2"), partitionIds2.get("p2"))
    assertNotEquals(partitionIds1.get("p3"), partitionIds2.get("p3"))
    order_qt_select_2 "SELECT * FROM ${testTable}"

    sql "insert into ${testTable} values ('2020-02-10', 1.0, 'a', 1)"
    order_qt_select_3 "SELECT * FROM ${testTable}"
    checkNereidsExecute("truncate table ${testTable} partitions (p1, p2);")
    order_qt_select_4 "SELECT * FROM ${testTable}"

    def partitionIds3 = getPartitionIds()
    assertEquals(["p1", "p2", "p3"].toSet(), partitionIds3.keySet())
    assertNotEquals(partitionIds2.get("p1"), partitionIds3.get("p1"))
    assertNotEquals(partitionIds2.get("p2"), partitionIds3.get("p2"))
    assertEquals(partitionIds2.get("p3"), partitionIds3.get("p3"))

    sql "DROP TABLE IF EXISTS ${testTable}"

    // test truncate partition table which has no partition
    testTable = "test_truncate_no_partition_table_nereids"
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

    checkNereidsExecute("truncate table ${testTable};")

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

    checkNereidsExecute("truncate table ${testNonPartitionTable};")
    result1 = sql "show partitions from ${testNonPartitionTable}"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
	
    sql "DROP TABLE IF EXISTS ${testNonPartitionTable}"

    // test truncate table force
    testTable = "test_truncate_table_force_nereids"

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

    partitionIds1 = getPartitionIds()
    assertEquals(["p1", "p2", "p3"].toSet(), partitionIds1.keySet())

    sql "insert into ${testTable} values ('2020-01-01', 1.0, 'a', 1)"
    sql "insert into ${testTable} values ('2020-03-10', 1.0, 'a', 1)"
    order_qt_select_5 "SELECT * FROM ${testTable}"

    checkNereidsExecute("truncate table ${testTable} force;")
    // if we use force, the empty partitions skiped, 
    // so that partition ID doesnt change.
    partitionIds2 = getPartitionIds()
    assertEquals(["p1", "p2", "p3"].toSet(), partitionIds2.keySet())
    assertNotEquals(partitionIds1.get("p1"), partitionIds2.get("p1"))
    assertEquals(partitionIds1.get("p2"), partitionIds2.get("p2"))
    assertNotEquals(partitionIds1.get("p3"), partitionIds2.get("p3"))
    order_qt_select_6 "SELECT * FROM ${testTable}"

    sql "insert into ${testTable} values ('2020-02-10', 1.0, 'a', 1)"
    order_qt_select_7 "SELECT * FROM ${testTable}"

    checkNereidsExecute("truncate table ${testTable} partitions (p1, p2) force;")
    order_qt_select_8 "SELECT * FROM ${testTable}"

    partitionIds3 = getPartitionIds()
    assertEquals(["p1", "p2", "p3"].toSet(), partitionIds3.keySet())
    assertEquals(partitionIds2.get("p1"), partitionIds3.get("p1"))
    assertNotEquals(partitionIds2.get("p2"), partitionIds3.get("p2"))
    assertEquals(partitionIds2.get("p3"), partitionIds3.get("p3"))

    sql "DROP TABLE IF EXISTS ${testTable}"
}

