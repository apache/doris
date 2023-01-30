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

suite("test_multiply") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_multiply"
    sql """DROP TABLE IF EXISTS `${tableName}`"""
    sql """ CREATE TABLE `${tableName}` (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `col2` DECIMAL(27,9) COMMENT "数据灌入日期时间",
        `col3` DECIMAL(16,8) COMMENT "数据灌入日期时间")
        DUPLICATE KEY(`user_id`) DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1" ); """

    sql """ insert into `${tableName}` values(1,null,2.2); """
    sql """ insert into `${tableName}` values(1,0,0); """
    qt_select """ select COALESCE(col2, 0) * COALESCE(col3, 0) from `${tableName}`; """
    qt_select """ select col2 * col3 from `${tableName}`; """
}
