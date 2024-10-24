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

suite("test_alter_table_replace") {
    def tbNameA = "test_alter_table_replace_a"
    def tbNameB = "test_alter_table_replace_b"
    def tbNameC = "test_alter_table_replace_c"
    def tbNameD = "test_alter_table_replace_d"

    sql "DROP TABLE IF EXISTS ${tbNameA}"
    sql "DROP TABLE IF EXISTS ${tbNameB}"
    sql "DROP TABLE IF EXISTS ${tbNameC}"
    sql "DROP TABLE IF EXISTS ${tbNameD}"
    sql """
        CREATE TABLE ${tbNameA} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",

            `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
            `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
            `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
            `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
            PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
            PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
        )
        DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
        """

    sql """
        CREATE TABLE ${tbNameB} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",

            `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
            `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
            `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
            `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
            PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
            PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
        )
        DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
        """

    sql """
        CREATE TABLE ${tbNameC} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",

            `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
            `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
            `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
            `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
            PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
            PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
        )
        DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
        """

    sql """ ALTER TABLE ${tbNameC} ADD TEMPORARY PARTITION `tp201701` VALUES LESS THAN ("2017-02-01");"""
    sql """ ALTER TABLE ${tbNameC} ADD TEMPORARY PARTITION `tp201702` VALUES LESS THAN ("2017-03-01");"""
    sql """ ALTER TABLE ${tbNameC} ADD TEMPORARY PARTITION `tp201703` VALUES LESS THAN ("2017-04-01");"""

    sql """
        CREATE TABLE ${tbNameD} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",

            `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
            `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
            `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
            `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-02-01")
        )
        DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
        """
    sql """ ALTER TABLE ${tbNameD} ADD TEMPORARY PARTITION `tp201701` VALUES LESS THAN ("2017-02-01");"""

    sql """ INSERT INTO ${tbNameA} VALUES
            (1, '2017-01-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1)),
            (2, '2017-02-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2)),
            (3, '2017-03-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """

    sql """ INSERT INTO ${tbNameB} VALUES
            (4, '2017-01-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1)),
            (5, '2017-02-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2)),
            (6, '2017-03-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """
    sql """ INSERT INTO ${tbNameC} VALUES
            (7, '2017-01-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1)),
            (8, '2017-02-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2)),
            (9, '2017-03-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """
    sql """ INSERT INTO ${tbNameC} TEMPORARY PARTITION(tp201701, tp201702, tp201703) VALUES
            (-7, '2017-01-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1)),
            (-8, '2017-02-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2)),
            (-9, '2017-03-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """
    sql """ INSERT INTO ${tbNameD} TEMPORARY PARTITION(tp201701) VALUES
            (-7, '2017-01-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1))
        """

    qt_select """ select * from ${tbNameA} order by user_id"""
    qt_select """ select * from ${tbNameB} order by user_id"""

    sql """ALTER TABLE ${tbNameA} REPLACE WITH TABLE ${tbNameB} PROPERTIES('swap' = 'true');"""

    qt_select """ select * from ${tbNameA} order by user_id"""
    qt_select """ select * from ${tbNameB} order by user_id"""

    sql """ALTER TABLE ${tbNameA} REPLACE WITH TABLE ${tbNameB} PROPERTIES('swap' = 'false');"""
    qt_select """ select * from ${tbNameA} order by user_id"""
    test {
        sql """ select * from ${tbNameB} order by user_id"""
        // check exception message contains
        exception "${tbNameB}"
    }

    sql """ ALTER TABLE ${tbNameC} REPLACE PARTITION (p201701, p201702, p201703)
            WITH TEMPORARY PARTITION (tp201701, tp201702, tp201703)
            PROPERTIES("use_temp_partition_name" = "true");
        """
    qt_before_recover """ select * from ${tbNameC} order by user_id desc """
    sql """ ALTER TABLE ${tbNameC} DROP PARTITION tp201701 FORCE """
    sql """ ALTER TABLE ${tbNameC} DROP PARTITION tp201702 FORCE """
    sql """ ALTER TABLE ${tbNameC} DROP PARTITION tp201703 FORCE """
    sql """ RECOVER PARTITION p201701 FROM ${tbNameC} """
    sql """ RECOVER PARTITION p201702 FROM ${tbNameC} """
    sql """ RECOVER PARTITION p201703 FROM ${tbNameC} """
    qt_after_recover """ select * from ${tbNameC} order by user_id """

    sql """ ALTER TABLE ${tbNameD} REPLACE PARTITION (p201701)
            WITH TEMPORARY PARTITION (tp201701) FORCE
            PROPERTIES("use_temp_partition_name" = "true");
        """
    qt_replace_p_force """ select * from ${tbNameD} order by user_id"""

    sql """ ALTER TABLE ${tbNameD} DROP PARTITION tp201701 FORCE """

    test {
        sql """ RECOVER PARTITION p201701 FROM ${tbNameD} """
        // check exception message contains
        exception "${tbNameD}"
    }

    sql "DROP TABLE IF EXISTS ${tbNameA} FORCE;"
    sql "DROP TABLE IF EXISTS ${tbNameB} FORCE;"
}
