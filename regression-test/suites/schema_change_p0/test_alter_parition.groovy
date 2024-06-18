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

suite("test_alter_partition") {
    def tbName = "test_alter_partition"

    sql "DROP TABLE IF EXISTS ${tbName} FORCE"
    sql """
        CREATE TABLE ${tbName} (
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
        PROPERTIES (
                "replication_num" = "1"
            );
        ;
        """

    sql """ INSERT INTO ${tbName} VALUES
            (1, '2017-01-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1)),
            (2, '2017-02-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2)),
            (3, '2017-03-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """

    qt_select """ select * from ${tbName} order by user_id"""

    // modify in_memory property
    // https://github.com/apache/doris/pull/18731
    if (!isCloudMode()) {
        test {
            sql """ALTER TABLE ${tbName} MODIFY PARTITION p201701 SET ("in_memory" = "true");"""
            exception "Not support set 'in_memory'='true' now!"
        }
    }
    sql "DROP TABLE IF EXISTS ${tbName} FORCE"
}
