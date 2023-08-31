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

suite("test_alter_table_status") {
    def tbName1 = "alter_table_status"

    try {
        sql "DROP TABLE IF EXISTS ${tbName1}"
        sql """
                CREATE TABLE IF NOT EXISTS ${tbName1} (
                    k1 INT,
                    v1 INT,
                    v2 INT
                )
                DUPLICATE KEY (k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "light_schema_change" = "false", "disable_auto_compaction" = "true");
            """

        // set table state to ROLLUP
        sql """ADMIN SET TABLE ${tbName1} STATUS PROPERTIES ("state" = "rollup");"""
        // try alter table comment
        test {
            sql """ ALTER TABLE ${tbName1} MODIFY COMMENT 'test'; """
            exception "Table[alter_table_status]'s state(ROLLUP) is not NORMAL. Do not allow doing ALTER ops"
        }

        // set table state to NORMAL
        sql """ADMIN SET TABLE ${tbName1} STATUS PROPERTIES ("state" = "normal");"""
        // try alter table comment
        sql """ ALTER TABLE ${tbName1} MODIFY COMMENT 'test'; """
    } finally {
        // drop table
        sql """ DROP TABLE  ${tbName1} force"""
    }
}
