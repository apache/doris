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

suite("test_largeint") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tbName = "test_largeint"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName}
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
    sql "insert into ${tbName} values (10003,195456789345678955654444443875);"
    sql "insert into ${tbName} values (10003,195456789345678955654444443874);"
    sql "insert into ${tbName} values (10003,195456789345678955654444443873);"
    sql "insert into ${tbName} values (10003,195456789345678955654444443877);"
    sql "insert into ${tbName} values (10003,195456789345678955654444443878);"
    sql "insert into ${tbName} values (10004,195456789345678955654444443878);"
    sql "insert into ${tbName} values (10005,195456789345678955654444443877);"
    sql "insert into ${tbName} values (10006,195456789345678955654444443878);"
    sql "insert into ${tbName} values (10009,195456789345678955654444443878);"

    qt_select "select count(1) from ${tbName} where phone='195456789345678955654444443878';"
    sql "DROP TABLE ${tbName}"
}
