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

suite("test_binary_predicate_cast") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
sql """
    CREATE TABLE IF NOT EXISTS test_binary_predicate_cast
    (
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `phone` LARGEINT COMMENT "用户电话"
    )
    UNIQUE KEY(`user_id`)
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
sql """insert into test_binary_predicate_cast values (10003,195456789345678955654444443875),(10003,195456789345678955654444443874),(10003,195456789345678955654444443873),(10003,195456789345678955654444443877),(10003,195456789345678955654444443878),(10004,195456789345678955654444443878),(10005,195456789345678955654444443877),(10006,195456789345678955654444443878),(10009,195456789345678955654444443878)
    """

qt_test_largeint_string "select * from test_binary_predicate_cast where phone='195456789345678955654444443878' order by user_id"

}
