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

suite("test_basic_delete_job") {
    def unpartitionTable = "un_partition_table"
    def oneRangeColumnTable = "one_range_column_table"
    def twoRangeColumnTable = "two_range_column_table"
    def oneListColumnTable = "one_list_column_table"
    def twoListColumnTable = "two_list_column_table"


    // Test no partition
    sql """DROP TABLE IF EXISTS ${unpartitionTable} """
    sql """CREATE TABLE ${unpartitionTable} (
          `id` int NOT NULL,
          `name` varchar(25) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`name`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """
    sql """insert into ${unpartitionTable} values (1, "a"), (2, "b"), (3, "c")"""
    qt_unpartition1 """select * from ${unpartitionTable} order by id"""
    sql """delete from ${unpartitionTable} where id < 0"""
    qt_unpartition2 """select * from ${unpartitionTable} order by id"""
    sql """delete from ${unpartitionTable} where id = 1"""
    qt_unpartition3 """select * from ${unpartitionTable} order by id"""
    sql """delete from ${unpartitionTable} where name = "c" """
    qt_unpartition4 """select * from ${unpartitionTable} order by id"""

    // Test one range partition column
    sql """DROP TABLE IF EXISTS ${oneRangeColumnTable} """
    sql """CREATE TABLE ${oneRangeColumnTable} (
          `id` int NOT NULL,
          `name` varchar(25) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (PARTITION p0 VALUES [("0"), ("10")),
        PARTITION p1 VALUES [("10"), ("20")),
        PARTITION p2 VALUES [("20"), ("30")))
        DISTRIBUTED BY HASH(`name`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into ${oneRangeColumnTable} values (1, "a"), (2, "b"), (3, "c"), (11, "d"), (12, "e"), (13, "f"), (21, "g"), (22, "h"), (23, "i")"""
    qt_one_range1 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} where id < 0"""
    qt_one_range2 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} where id = 1"""
    qt_one_range3 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} partition(p0) where id = 11"""
    qt_one_range4 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} partition(p0) where id < 22"""
    qt_one_range5 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} where name = "d" """
    qt_one_range6 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} partition(p2) where name = "g" """
    qt_one_range7 """select * from ${oneRangeColumnTable} order by id"""
    sql """delete from ${oneRangeColumnTable} partition(p1) where name = "h" """
    qt_one_range8 """select * from ${oneRangeColumnTable} order by id"""

    // Test two range partition columns
    sql """DROP TABLE IF EXISTS ${twoRangeColumnTable} """
    sql """CREATE TABLE ${twoRangeColumnTable} (
          `id1` int NOT NULL,
          `id2` int NOT NULL,
          `name` varchar(25) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id1`)
        PARTITION BY RANGE(`id1`, `id2`)
        (PARTITION p0 VALUES [("0", "0"), ("10", "10")),
        PARTITION p1 VALUES [("10", "10"), ("20", "20")),
        PARTITION p2 VALUES [("20", "20"), ("30", "30")))
        DISTRIBUTED BY HASH(`name`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into ${twoRangeColumnTable} values (0, 0, "a"), (1, 100, "b"), (2, 3, "c"), (3, 3, "d"), (4, 6, "e"), (6, 7, "f"), (8, 8, "g"), (9, 1000, "h"), (10, 9, "i")"""
    sql """insert into ${twoRangeColumnTable} values (10, 10, "j"), (11, 10, "k"), (12, 13, "l"), (13, 5, "m"), (14, 16, "n"), (16, 17, "o"), (18, 88, "p"), (19, 1000, "q"), (20, 19, "r")"""
    sql """insert into ${twoRangeColumnTable} values (20, 20, "s"), (21, 20, "t"), (22, 3, "u"), (23, 5, "v"), (24, 6, "w"), (26, 7, "x"), (28, 8, "y"), (29, 1000, "z"), (30, 29, "zz")"""

    qt_two_range1 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where id1 < 0"""
    qt_two_range2 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where id2 < 0"""
    qt_two_range3 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where id1 = 1"""
    qt_two_range4 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where id1 = 10"""
    qt_two_range5 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where name = "u" """
    qt_two_range6 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} partition(p0) where id1 < 15"""
    qt_two_range7 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where id1 < 15"""
    qt_two_range8 """select * from ${twoRangeColumnTable} order by id1, id2"""
    sql """delete from ${twoRangeColumnTable} where id2 > 10"""
    qt_two_range9 """select * from ${twoRangeColumnTable} order by id1, id2"""

    // Test one list partition column
    sql """DROP TABLE IF EXISTS ${oneListColumnTable} """
    sql """CREATE TABLE ${oneListColumnTable} (
          `id` int NOT NULL,
          `name` varchar(25) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY LIST(`id`)
        (PARTITION p0 VALUES IN ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
        PARTITION p1 VALUES IN ("10", "11", "12", "13", "14", "15", "16", "17", "18", "19"),
        PARTITION p2 VALUES IN ("20", "21", "22", "23", "24", "25", "26", "27", "28", "29"))
        DISTRIBUTED BY HASH(`name`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into ${oneListColumnTable} values (1, "a"), (2, "b"), (3, "c"), (11, "d"), (12, "e"), (13, "f"), (21, "g"), (22, "h"), (23, "i")"""
    qt_one_list1 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} where id < 0"""
    qt_one_list2 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} where id = 1"""
    qt_one_list3 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} partition(p0) where id = 11"""
    qt_one_list4 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} partition(p0) where id < 22"""
    qt_one_list5 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} where name = "d" """
    qt_one_list6 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} partition(p2) where name = "g" """
    qt_one_list7 """select * from ${oneListColumnTable} order by id"""
    sql """delete from ${oneListColumnTable} partition(p1) where name = "h" """
    qt_one_list8 """select * from ${oneListColumnTable} order by id"""

    // Test two list partition columns
    sql """DROP TABLE IF EXISTS ${twoListColumnTable} """
    sql """CREATE TABLE ${twoListColumnTable} (
          `id1` int NOT NULL,
          `id2` int NOT NULL,
          `name` varchar(25) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id1`)
        PARTITION BY LIST(`id1`, `id2`)
        (PARTITION p0 VALUES IN (("0", "100"), ("1", "101"), ("2", "102")),
        PARTITION p1 VALUES IN (("10", "200"), ("11", "201"), ("12", "202")),
        PARTITION p2 VALUES IN (("20", "300"), ("21", "301"), ("22", "302")))
        DISTRIBUTED BY HASH(`name`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into ${twoListColumnTable} values (0, 100, "a"), (1, 101, "b"), (2, 102, "c")"""
    sql """insert into ${twoListColumnTable} values (10, 200, "d"), (11, 201, "e"), (12, 202, "f")"""
    sql """insert into ${twoListColumnTable} values (20, 300, "g"), (21, 301, "h"), (22, 302, "i")"""

    qt_two_list1 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where id1 < 0"""
    qt_two_list2 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where id2 < 0"""
    qt_two_list3 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where id1 = 1"""
    qt_two_list4 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where id2 = 200"""
    qt_two_list5 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where name = "h" """
    qt_two_list6 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} partition(p0) where id1 < 12"""
    qt_two_list7 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where id1 < 12"""
    qt_two_list8 """select * from ${twoListColumnTable} order by id1, id2"""
    sql """delete from ${twoListColumnTable} where id2 > 300"""
    qt_two_list9 """select * from ${twoListColumnTable} order by id1, id2"""
}
