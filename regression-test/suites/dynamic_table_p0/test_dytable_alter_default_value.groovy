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
    def test_alter_default_value = { database_name, table_name, column_name, src_type, src_value, dst_type, dst_value -> 
        sql "USE ${database_name}"
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                id BIGINT,
                `${column_name}` ${src_type} DEFAULT '${src_value}'
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            properties("replication_num" = "1");
        """
        for (i = 0; i < 10; i++) {
            sql "INSERT INTO ${table_name} VALUES (${i}, DEFAULT)"
        }
        sql "ALTER TABLE ${table_name} MODIFY COLUMN ${column_name} ${dst_type} DEFAULT '${dst_value}'"
        Thread.sleep(3000)
        for (i = 10; i < 20; i++) {
            sql "INSERT INTO ${table_name} VALUES (${i}, DEFAULT)"
        }
        def result1 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${src_value}'"
        assertEquals(result1[0][0], 10)
        def result2 = sql "SELECT COUNT(*) FROM ${table_name} WHERE ${column_name} = '${dst_value}'"
        assertEquals(result2[0][0], 10)
        sql "DROP TABLE IF EXISTS ${table_name}"
    }

    // column type isn't modified
    test_alter_default_value("demo", "t1", "col", "BIGINT", "123", "BIGINT", "456")
    test_alter_default_value("demo", "t2", "col", "VARCHAR(10)", "abc", "VARCHAR(10)", "def")
    // column type is modified
    test_alter_default_value("demo", "t3", "col", "VARCHAR(10)", "abc", "VARCHAR(20)", "abcdef")
    test_alter_default_value("demo", "t4", "col", "BIGINT", "123", "TEXT", "abcdef")

}
