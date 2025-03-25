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

suite("docs/admin-manual/data-admin/recyclebin.md", "p0,nonConcurrent") {
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS example_db;
            USE example_db;
            DROP TABLE IF EXISTS example_tbl;
            CREATE TABLE IF NOT EXISTS example_db.example_tbl(
                a INT
            ) PARTITION BY RANGE(a) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2)
            ) DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            INSERT INTO example_tbl VALUES (1);
            
            ALTER TABLE example_tbl DROP PARTITION p1;
            DROP TABLE example_tbl;
            DROP DATABASE example_db;
        """

        sql """RECOVER DATABASE example_db;"""
        sql """RECOVER TABLE example_db.example_tbl;"""
        sql """RECOVER PARTITION p1 FROM example_tbl;"""

    } catch (Throwable t) {
        Assertions.fail("examples in docs/admin-manual/data-admin/recyclebin.md failed to exec, please fix it", t)
    }
}
