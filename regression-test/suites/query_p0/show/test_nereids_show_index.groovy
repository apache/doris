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

suite("test_nereids_show_index") {
    sql "DROP DATABASE IF EXISTS test_show_index"
    sql "CREATE DATABASE IF NOT EXISTS test_show_index"
    sql "DROP TABLE IF EXISTS test_show_index.test_show_index_tbl1"
    sql "DROP TABLE IF EXISTS test_show_index.test_show_index_tbl2"
    sql """
        CREATE TABLE IF NOT EXISTS test_show_index.test_show_index_tbl1 (
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
        CREATE TABLE IF NOT EXISTS test_show_index.test_show_index_tbl2 (
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

    checkNereidsExecute("show index from test_show_index.test_show_index_tbl2;")
    checkNereidsExecute("show indexes from test_show_index.test_show_index_tbl1;")
    checkNereidsExecute("show key from test_show_index.test_show_index_tbl1;")
    checkNereidsExecute("show keys from test_show_index.test_show_index_tbl2;")
    checkNereidsExecute("show index from test_show_index_tbl1 from test_show_index;")
    checkNereidsExecute("show index from test_show_index_tbl2 from test_show_index;")
    checkNereidsExecute("show index from test_show_index.test_show_index_tbl1 from test_show_index;")
    checkNereidsExecute("show index from test_show_index.test_show_index_tbl2 from test_show_index;")
    checkNereidsExecute("show index in test_show_index.test_show_index_tbl2;")
    checkNereidsExecute("show indexes in test_show_index.test_show_index_tbl1;")
    checkNereidsExecute("show key in test_show_index.test_show_index_tbl1;")
    checkNereidsExecute("show keys in test_show_index.test_show_index_tbl2;")
    checkNereidsExecute("show index in test_show_index_tbl1 from test_show_index;")
    checkNereidsExecute("show index in test_show_index_tbl2 from test_show_index;")
    checkNereidsExecute("show index in test_show_index.test_show_index_tbl1 from test_show_index;")
    checkNereidsExecute("show index in test_show_index.test_show_index_tbl2 from test_show_index;")
    checkNereidsExecute("show index in test_show_index_tbl1 in test_show_index;")
    checkNereidsExecute("show index in test_show_index_tbl2 in test_show_index;")
    checkNereidsExecute("show index in test_show_index.test_show_index_tbl1 in test_show_index;")
    checkNereidsExecute("show index in test_show_index.test_show_index_tbl2 in test_show_index;")

    def res1 = sql """show index from test_show_index.test_show_index_tbl2"""
    assertEquals(2, res1.size())
    def res2 = sql """show index from test_show_index.test_show_index_tbl1"""
    assertEquals(2, res2.size())
    def res3 = sql """show index from test_show_index_tbl1 from test_show_index"""
    assertEquals(2, res3.size())
    def res4 = sql """show index from test_show_index.test_show_index_tbl1 from test_show_index"""
    assertEquals(2, res4.size())
}
