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
suite("unique_aggregate_key_hash_check") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "DROP TABLE IF EXISTS example_tbl_agg3"
    sql "DROP TABLE IF EXISTS example_tbl_unique"

    test {
        sql """CREATE TABLE IF NOT EXISTS example_tbl_agg3
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
            AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
            DISTRIBUTED BY HASH(`cost`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Distribution column[cost] is not key column"
    }
    test {
        sql """
            CREATE TABLE IF NOT EXISTS example_tbl_unique
            (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `PHONE` LARGEINT COMMENT "用户电话",
                `address` VARCHAR(500) COMMENT "用户地址",
                `register_time` DATETIME COMMENT "用户注册时间"
            )
            UNIQUE KEY(`user_id`, `username`)
            DISTRIBUTED BY HASH(`phone`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception "Distribution column[phone] is not key column"
    }

}
