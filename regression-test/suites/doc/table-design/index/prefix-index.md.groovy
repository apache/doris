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

suite("docs/table-design/index/prefix-index.md") {
    try {
        sql "DROP TABLE IF EXISTS tbale1"
        sql "DROP TABLE IF EXISTS tbale2"

        sql """
        CREATE TABLE IF NOT EXISTS `table1` (
            user_id BIGINT,
            age INT,
            message VARCHAR(100),
            max_dwell_time BIGINT,
            min_dwell_time DATETIME
        ) PROPERTIES ("replication_num" = "1")
        """

        sql """
        CREATE TABLE IF NOT EXISTS `table2` (
            user_id VARCHAR(20),
            age INT,
            message VARCHAR(100),
            max_dwell_time BIGINT,
            min_dwell_time DATETIME
        ) PROPERTIES ("replication_num" = "1")
        """

        sql "SELECT * FROM table1 WHERE user_id=1829239 and age=20"
        sql "SELECT * FROM table1 WHERE age=20"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/index/prefix-index.md failed to exec, please fix it", t)
    }
}
