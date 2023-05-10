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

suite("test_dytable_alter_default_value") {
    def test_alter_default_value = { table_name, column_name, src_type, src_value, dst_type, dst_value -> 
        sql "CREATE DATABASDE test IF NOT EXISTS"
        sql "USE test"
        sql "DROP TABLE ${table_name} IF EXISTS"
        sql """
            CREATE TABLE ${table_name} (
                id BIGINT
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            properties(
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
        """
        // step1: insert data
        for (i = 0; i < 10; i++) {
            sql "INSERT INTO ${table_name} VALUES (${i})"
        }
        // step2: add column has default value == src_value
        sql "ALTER TABLE ${table_name} ADD COLUMN ${column_name} ${src_type} DEFAULT '${src_value}'"
        // step3: insert data
        for (i = 10; i < 20; i++) {
            sql "INSERT INTO ${table_name} VALUES (${i}, DEFAULT)"
        }
        // step4: delete where column == src_value
        sql "DELETE FROM ${table_name} WHERE ${column_name} = '${src_value}'"
        def result1 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${src_value}'"
        assertEquals(result1[0][0], 0)
        // step5: insert data
        for (i = 30; i < 40; i++) {
            sql "INSERT INTO ${table_name} VALUES (${i}, DEFAULT)"
        }
        // step6: alter default value == dst_value
        sql "ALTER TABLE ${table_name} MODIFY COLUMN ${column_name} ${dst_type} DEFAULT '${dst_value}'"
        Thread.sleep(3000)
        // step7: insert data
        for (i = 40; i < 50; i++) {
            sql "INSERT INTO ${table_name} VALUES (${i}, DEFAULT)"
        }
        def result2 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${src_value}'"
        assertEquals(result2[0][0], 10)
        def result3 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${dst_value}'"
        assertEquals(result3[0][0], 10)
        // step8: delete where column == dst_value
        sql "DELETE FROM ${table_name} WHERE ${column_name} = '${dst_value}'"
        def result4 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${src_value}'"
        assertEquals(result4[0][0], 10)
        def result5 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${dst_value}'"
        assertEquals(result5[0][0], 0)
        sql "DROP TABLE ${table_name}"
        sql "DROP DATABASE test"
    }

    // column type isn't modified
    test_alter_default_value("t1", "col", "BIGINT", "123", "BIGINT", "456")
    test_alter_default_value("t2", "col", "VARCHAR(10)", "abc", "VARCHAR(10)", "def")
    // column type is modified
    test_alter_default_value("t3", "col", "VARCHAR(10)", "abc", "VARCHAR(20)", "abcdef")
    test_alter_default_value("t4", "col", "BIGINT", "123", "TEXT", "abcdef")

}
