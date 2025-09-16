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

suite("test_nereids_show_build_index") {
    if (!isCloudMode()) {
        sql "DROP DATABASE IF EXISTS test_show_build_index"
        sql "CREATE DATABASE IF NOT EXISTS test_show_build_index"
        sql "DROP TABLE IF EXISTS test_show_build_index.test_show_build_index_tbl1"
        sql "DROP TABLE IF EXISTS test_show_build_index.test_show_build_index_tbl2"
        sql """
        CREATE TABLE IF NOT EXISTS test_show_build_index.test_show_build_index_tbl1 (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `note` TEXT COMMENT "备注",
            INDEX idx_user_id1 (`user_id`) USING INVERTED COMMENT '',
            INDEX idx_note1 (`note`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = "V1" );
        """
        sql """
        CREATE TABLE IF NOT EXISTS test_show_build_index.test_show_build_index_tbl2 (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `note` TEXT COMMENT "备注",
            INDEX idx_user_id2 (`user_id`) USING INVERTED COMMENT '',
            INDEX idx_note2 (`note`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = "V1" );
        """
        sql "BUILD INDEX idx_user_id1 ON test_show_build_index.test_show_build_index_tbl1;"
        sql "BUILD INDEX idx_user_id2 ON test_show_build_index.test_show_build_index_tbl2;"
        sleep(30000)
        checkNereidsExecute("show build index from test_show_build_index;")
        checkNereidsExecute("show build index in test_show_build_index;")
        checkNereidsExecute("show build index from test_show_build_index order by JobId;")
        checkNereidsExecute("show build index from test_show_build_index order by JobId desc;")
        checkNereidsExecute("show build index from test_show_build_index where TableName = 'test_show_build_index_tbl2';")
        checkNereidsExecute("show build index from test_show_build_index where PartitionName = 'test_show_build_index_tbl1';")
        checkNereidsExecute("show build index from test_show_build_index where CreateTime = '2025-06-04 20:58:27';")
        checkNereidsExecute("show build index from test_show_build_index where FinishTime = '2025-06-04 20:58:27';")
        checkNereidsExecute("show build index from test_show_build_index where State = 'FINISHED';")
        checkNereidsExecute("show build index from test_show_build_index where State = 'FINISHED' order by JobId;")
        checkNereidsExecute("show build index from test_show_build_index where State = 'FINISHED' order by TableName;")
        checkNereidsExecute("show build index from test_show_build_index where State = 'FINISHED' order by TableName limit 1;")
        checkNereidsExecute("show build index from test_show_build_index where State = 'FINISHED' order by TableName limit 1,1;")
        checkNereidsExecute("show build index from test_show_build_index where State = 'FINISHED' and CreateTime = '2025-06-04 21:01:50';")
        checkNereidsExecute("show build index from test_show_build_index where FinishTime != '2025-06-04 21:53:48';")
        checkNereidsExecute("show build index from test_show_build_index where FinishTime >= '2025-06-04 21:53:48';")
        checkNereidsExecute("show build index from test_show_build_index where FinishTime > '2025-06-04 21:53:48';")
        checkNereidsExecute("show build index from test_show_build_index where FinishTime <= '2025-06-04 21:53:48';")
        checkNereidsExecute("show build index from test_show_build_index where FinishTime < '2025-06-04 21:53:48';")
        checkNereidsExecute("show build index from test_show_build_index where TableName != 'test_show_build_index_tbl2';")
        checkNereidsExecute("show build index from test_show_build_index where CreateTime >= '2025-06-05 22:48:08';")
        checkNereidsExecute("show build index from test_show_build_index where CreateTime > '2025-06-05 22:48:08';")
        checkNereidsExecute("show build index from test_show_build_index where CreateTime <= '2025-06-05 22:48:08';")
        checkNereidsExecute("show build index from test_show_build_index where CreateTime < '2025-06-05 22:48:08';")

        def res1 = sql """show build index from test_show_build_index"""
        assertEquals(2, res1.size())
        def res2 = sql """show build index from test_show_build_index order by TableName"""
        assertEquals(2, res2.size())
        assertEquals("test_show_build_index_tbl1", res2.get(0).get(1))
        def res3 = sql """show build index from test_show_build_index order by TableName limit 1"""
        assertEquals(1, res3.size())
        assertEquals("test_show_build_index_tbl1", res3.get(0).get(1))
        def res7 = sql """show build index from test_show_build_index where TableName = 'test_show_build_index_tbl2';"""
        assertEquals(1, res7.size())
        assertEquals("test_show_build_index_tbl2", res7.get(0).get(2))

        assertThrows(Exception.class, {
            sql """show build index from test_show_build_index where JobId = 1749041691284;"""
        })
        assertThrows(Exception.class, {
            sql """show build index from test_show_build_index where JobId = 1749041691284 or TableName = 'xx';"""
        })
        assertThrows(Exception.class, {
            sql """show build index from test_show_build_index where indexname = 'xx';"""
        })
    }
}
