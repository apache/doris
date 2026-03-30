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

suite("test_nereids_drop_table") {
    def table_name = "nereids_test_drop_table"
    def db_name = "nereids_test_db_name"

    sql """DROP DATABASE IF EXISTS ${db_name}"""

    sql """CREATE DATABASE ${db_name}"""

    sql """  
      CREATE TABLE IF NOT EXISTS ${table_name}
      (
      `user_id` LARGEINT NOT NULL COMMENT "用户id"
      ) ENGINE = olap
      DUPLICATE KEY(`user_id`)
      DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
      PROPERTIES ("replication_num" = "1");
    """
    checkNereidsExecute("DROP TABLE ${table_name}")

}
