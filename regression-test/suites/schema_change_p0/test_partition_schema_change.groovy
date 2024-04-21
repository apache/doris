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

suite('test_partition_schema_change') {
    sql 'DROP TABLE IF EXISTS example_range_tbl;'

    sql '''
    CREATE TABLE IF NOT EXISTS example_range_tbl
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
    PARTITION BY RANGE(`date`)
    (
        PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
        PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
        PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
    )
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
    PROPERTIES
    (
        "replication_num" = "1", "light_schema_change" = "false"
    );
    '''

    sql """
    INSERT INTO example_range_tbl VALUES
        (1, '2017-01-02', 'Beijing', 10, 1, "2017-01-02 00:00:00", 1, 30, 20);
    """

    sql """
    INSERT INTO example_range_tbl VALUES
        (1, '2017-02-02', 'Beijing', 10, 1, "2017-02-02 00:00:00", 1, 30, 20);
    """

    sql 'select * from example_range_tbl order by `date`;'

    if (!isCloudMode()) {
        sql "ALTER TABLE example_range_tbl SET ('light_schema_change' = 'true');"
    }

    sql """
    INSERT INTO example_range_tbl VALUES
        (1, '2017-03-02', 'Beijing', 10, 1, "2017-03-02 00:00:00", 1, 30, 20);
    """

    sql 'select * from example_range_tbl order by `date`;'

    sql "ALTER table example_range_tbl ADD COLUMN new_column INT MAX default '1';"

    sql """
    INSERT INTO example_range_tbl VALUES
        (2, '2017-02-03', 'Beijing', 10, 1, "2017-02-02 00:00:00", 1, 30, 20, 2);
    """

    sql 'select * from example_range_tbl order by `date`;'
}
