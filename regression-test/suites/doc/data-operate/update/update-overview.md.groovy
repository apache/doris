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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/update/update-overview.md") {
    try {
        multi_sql """
            DROP TABLE IF EXISTS example_tbl_unique_merge_on_write;
            CREATE TABLE IF NOT EXISTS example_tbl_unique_merge_on_write
            (
                `user_id` LARGEINT NOT NULL,
                `username` VARCHAR(50) NOT NULL ,
                `city` VARCHAR(20),
                `age` SMALLINT,
                `sex` TINYINT,
                `phone` LARGEINT,
                `address` VARCHAR(500),
                `register_time` DATETIME
            )
            UNIQUE KEY(`user_id`, `username`)
            DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true"
            );
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/update/update-overview.md failed to exec, please fix it", t)
    }
}
