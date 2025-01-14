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

suite("docs/table-design/schema-change.md") {
    try {
        def waitUntilSchemaChangeDone = { tbl ->
            waitForSchemaChangeDone({
                sql " SHOW ALTER TABLE COLUMN FROM example_db WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 "
            })
        }

        multi_sql "create database if not exists example_db; use example_db; drop table if exists my_table;"
        sql """
        CREATE TABLE IF NOT EXISTS example_db.my_table(
            col1 int,
            col2 int,
            col3 int,
            col4 int,
            col5 int
        ) DUPLICATE KEY(col1, col2, col3)
        DISTRIBUTED BY RANDOM BUCKETS 1
        ROLLUP (
            example_rollup_index (col1, col3, col4, col5)
        )
        PROPERTIES (
            "replication_num" = "1"
        )
        """
        sql """
            ALTER TABLE example_db.my_table
            ADD COLUMN new_key_col INT KEY DEFAULT "0" AFTER col1
            TO example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")
        sql """
            ALTER TABLE example_db.my_table   
            ADD COLUMN new_val_col INT DEFAULT "0" AFTER col4
            TO example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")

        sql "drop table if exists example_db.my_table"
        sql """
        CREATE TABLE IF NOT EXISTS example_db.my_table(
            col1 int,
            col2 int,
            col3 int,
            col4 int SUM,
            col5 int MAX
        ) AGGREGATE KEY(col1, col2, col3)
        DISTRIBUTED BY HASH(col1) BUCKETS 1
        ROLLUP (
            example_rollup_index (col1, col3, col4, col5)
        )
        PROPERTIES (
            "replication_num" = "1"
        )
        """
        sql """
            ALTER TABLE example_db.my_table
            ADD COLUMN new_key_col INT DEFAULT "0" AFTER col1
            TO example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")
        sql """
            ALTER TABLE example_db.my_table
            ADD COLUMN new_val_col INT SUM DEFAULT "0" AFTER col4
            TO example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")

        sql """
            ALTER TABLE example_db.my_table
            ADD COLUMN (c1 INT DEFAULT "1", c2 FLOAT SUM DEFAULT "0")
            TO example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")

        sql """
            ALTER TABLE example_db.my_table
            DROP COLUMN col3
            FROM example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")


        sql "drop table if exists example_db.my_table"
        sql """
        CREATE TABLE IF NOT EXISTS example_db.my_table(
            col0 int,
            col1 int DEFAULT "1",
            col2 int,
            col3 varchar(32),
            col4 int SUM,
            col5 varchar(32) REPLACE DEFAULT "abc"
        ) AGGREGATE KEY(col0, col1, col2, col3)
        DISTRIBUTED BY HASH(col0) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
        """
        sql """
            ALTER TABLE example_db.my_table 
            MODIFY COLUMN col1 BIGINT KEY DEFAULT "1" AFTER col2
        """
        waitUntilSchemaChangeDone("my_table")
        sql """
            ALTER TABLE example_db.my_table
            MODIFY COLUMN col5 VARCHAR(64) REPLACE DEFAULT "abc"
        """
        waitUntilSchemaChangeDone("my_table")
        sql """
            ALTER TABLE example_db.my_table 
            MODIFY COLUMN col3 varchar(50) KEY NULL comment 'to 50'
        """
        waitUntilSchemaChangeDone("my_table")

        sql "drop table if exists my_table"
        sql """
            CREATE TABLE IF NOT EXISTS example_db.my_table(
                k1 int DEFAULT "1",
                k2 int,
                k3 varchar(32),
                k4 date,
                v1 int SUM,
                v2 int MAX,
            ) AGGREGATE KEY(k1, k2, k3, k4)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            ROLLUP (
               example_rollup_index(k1, k2, k3, v1, v2)
            )
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        sql """
            ALTER TABLE example_db.my_table
            ORDER BY (k3,k1,k2,v2,v1)
            FROM example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")

        sql "drop table if exists example_db.tbl1"
        sql """
            CREATE TABLE IF NOT EXISTS example_db.tbl1(
                k1 int,
                k2 int,
                k3 int
            ) AGGREGATE KEY(k1, k2, k3)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            ROLLUP (
               rollup1 (k1, k2),
               rollup2 (k2)
            )
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        sql """
            ALTER TABLE tbl1
            ADD COLUMN k4 INT default "1" to rollup1,
            ADD COLUMN k4 INT default "1" to rollup2,
            ADD COLUMN k5 INT default "1" to rollup2
        """
        waitUntilSchemaChangeDone("tbl1")

        sql "drop table if exists example_db.my_table"
        sql """
            CREATE TABLE IF NOT EXISTS example_db.my_table(
                k1 int DEFAULT "1",
                k2 int,
                k3 varchar(32),
                k4 date,
                v1 int SUM,
            ) AGGREGATE KEY(k1, k2, k3, k4)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            ROLLUP (
               example_rollup_index(k1, k3, k2, v1)
            )
            PROPERTIES (
               "replication_num" = "1"
            )
        """
        sql """
            ALTER TABLE example_db.my_table
            ADD COLUMN v2 INT MAX DEFAULT "0" TO example_rollup_index,
            ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index
        """
        waitUntilSchemaChangeDone("my_table")

        sql "SHOW ALTER TABLE COLUMN"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/schema-change.md failed to exec, please fix it", t)
    }
}
