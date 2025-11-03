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

suite("test_delete_from_partition") {
    def tableName = "delete_partition_test"

    // Test range partition
    sql """DROP TABLE IF EXISTS ${tableName} """
    sql """CREATE TABLE IF NOT EXISTS ${tableName}
    (`p1` INT NOT NULL, `p2` INT NOT NULL, `id` INT REPLACE NOT NULL, `maximum` INT MAX DEFAULT "0" )
    ENGINE=olap AGGREGATE KEY(`p1`, `p2`) PARTITION BY RANGE(`p1`, `p2`) ( PARTITION `p10` VALUES LESS THAN ("10", "10"), PARTITION `p100` VALUES LESS THAN ("100", "100"), PARTITION `p1000` VALUES LESS THAN ("1000", "1000"))
    DISTRIBUTED BY HASH(`p2`) BUCKETS 2 PROPERTIES ( "replication_num" = "1" )"""

    sql """insert into ${tableName} values (1, 1, 3, 0), (1, 1, 1, 1), (3, 3, 3, 3), (4, 4, 4, 4), (5, 5, 5, 5), (6, 6, 6, 6), (7, 7, 7, 7), (8, 8, 8, 8), (9, 9, 9, 9), (12, 12, 12, 12), (200, 200, 200, 200), (5, 20, 5, 20), (20, 5, 20, 5)"""
    qt_sql """select * from ${tableName} order by p1, p2"""

    // Delete data with partition specified
    sql """delete from ${tableName} partition p10 where p1 = 1"""
    qt_sql """select * from ${tableName} order by p1, p2"""

    // Delete data without partitioon specifed.
    // Test equal operator
    sql """delete from ${tableName} where p1 = 3"""
    qt_sql """select * from ${tableName} order by p1, p2"""
    // Test less than operator
    sql """delete from ${tableName} where p2 < 5"""
    qt_sql """select * from ${tableName} order by p1, p2"""
    // Test in operator
    sql """delete from ${tableName} where p1 in (5, 6) and p2 != 20"""
    qt_sql """select * from ${tableName} order by p1, p2"""
    // Test and operator
    sql """delete from ${tableName} where p1 > 6 and p1 < 50"""
    qt_sql """select * from ${tableName} order by p1, p2"""

    sql """DROP TABLE IF EXISTS ${tableName}"""

    // Test list partition
    sql """CREATE TABLE IF NOT EXISTS ${tableName}
    (`l1` VARCHAR(20) NOT NULL, `l2` VARCHAR(20) NOT NULL, `id` INT REPLACE NOT NULL, `maximum` INT MAX DEFAULT "0" )
    ENGINE=olap AGGREGATE KEY(`l1`, `l2`) PARTITION BY LIST(`l1`, `l2`) ( PARTITION `p1` VALUES IN (("a", "a"), ("b", "b"), ("c", "c")),
    PARTITION `p2` VALUES IN (("d", "d"), ("e", "e"), ("f", "f")), PARTITION `p3` VALUES IN (("g", "g"), ("h", "h"), ("i", "i")) ) DISTRIBUTED BY HASH(`l1`) BUCKETS 2 PROPERTIES ( "replication_num" = "1" )"""

    sql """insert into ${tableName} values ("a", "a", 1, 1), ("b", "b", 3, 2), ("c", "c", 3, 3), ("d", "d", 4, 4), ("e", "e", 5, 5), ("f", "f", 6, 6), ("g", "g", 7, 7), ("h", "h", 8, 8), ("i", "i", 9, 9)"""
    qt_sql """select * from ${tableName} order by l1, l2"""

    sql """delete from ${tableName} where l1 = "a";"""
    qt_sql """select * from ${tableName} order by l1, l2"""

    sql """delete from ${tableName} where l1 in ("b", "h")"""
    qt_sql """select * from ${tableName} order by l1, l2"""

    sql """delete from ${tableName} where l1 < "i" and l2 < "h";"""
    qt_sql """select * from ${tableName} order by l1, l2"""

    def testDeleteTableName = "test_delete_partition"
    sql """DROP TABLE IF EXISTS ${testDeleteTableName} """
    sql """create table ${testDeleteTableName}
    (
            k1 TINYINT not null,
                    k2 INT NOT NULL DEFAULT "1" COMMENT "int column",
    k3 VARCHAR(20)
    )
    UNIQUE KEY(`k1`, `k2`)
    PARTITION BY LIST(`k1`)
    (
            PARTITION `p1` VALUES IN (1),
    PARTITION `p2` VALUES IN (2)
    )
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES ('replication_num' = '1');"""

    sql """insert into ${testDeleteTableName} values (1, 2, "test1"), (2, 2, "test");"""

    qt_sql """select * from ${testDeleteTableName} order by k1;"""

    sql """delete from
    ${testDeleteTableName} partition p1
    using ${testDeleteTableName} partition (p1) as t1
    left outer join ${testDeleteTableName} partitions (p1, p2) as t2 on t1.k2 = t2.k2
    where
    ${testDeleteTableName}.k2 = t1.k2
    and t2.k2 is not null;"""

    qt_sql """select * from ${testDeleteTableName};"""
}
