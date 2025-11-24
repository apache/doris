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

suite("test_cloud_build_index_update") {
    if (!isCloudMode()) {
        return;
    }

    def timeout = 60000

    sql """set enable_add_index_for_new_data = true;"""
    sql """ set enable_profile = true;"""
    sql """ set profile_level = 2;"""

    // test delete bitmap
    sql """DROP TABLE IF EXISTS test_cloud_build_idx_uq_table;"""

    sql """
        CREATE TABLE test_cloud_build_idx_uq_table
            (
              `user_id` LARGEINT NOT NULL,
              `username` VARCHAR(50) NOT NULL,
              `age` int,
              `address` VARCHAR(500)
            )
        UNIQUE KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"
        );
    """

    sql """
        insert into test_cloud_build_idx_uq_table values 
        (1, "aa", 23, "aaaaa"),
        (2, "bb", 24, "bbbbb"),
        (3, "cc", 25, "ccccc"),
        (4, "dd", 26, "dddd"),
        (5, "ee", 26, "eeeee")
    """

    sql """

        insert into test_cloud_build_idx_uq_table values 
            (6, "ff", 27, "fffff"),
            (7, "gg", 28, "ggggg"),
            (8, "hh", 29, "hhhhh"),
            (1, "aa", 30, "a_update"),
            (5, "ee", 31, "e_udate");
    """


    sql """
        insert into test_cloud_build_idx_uq_table values 
            (6, "ff", 27, "fffff"),
            (7, "gg", 28, "ggggg"),
            (8, "hh", 29, "hhhhh"),
            (1, "aa", 30, "a_update"),
            (5, "ee", 31, "e_udate");
    """

    sql """
        insert into test_cloud_build_idx_uq_table values 
            (6, "ff", 27, "fffff"),
            (7, "gg", 28, "ggggg"),
            (8, "hh", 29, "hhhhh"),
            (1, "aa", 30, "a_update"),
            (5, "ee", 31, "e_udate");
    """

    sql """create index idx1 on test_cloud_build_idx_uq_table(address) using inverted;"""

    check_inverted_index_filter_rows("select * from test_cloud_build_idx_uq_table where address='hhhhh'" +
            " order by user_id,username,age,address", 0)

    sql """build index on test_cloud_build_idx_uq_table"""
    wait_for_last_build_index_finish("test_cloud_build_idx_uq_table", timeout)
    check_inverted_index_filter_rows("select * from test_cloud_build_idx_uq_table where address='hhhhh'" +
            " order by user_id,username,age,address", 12)

    sql """drop index idx1 on test_cloud_build_idx_uq_table"""
    wait_for_last_build_index_finish("test_cloud_build_idx_uq_table", timeout)

    check_inverted_index_filter_rows("select * from test_cloud_build_idx_uq_table where address='hhhhh'" +
            " order by user_id,username,age,address", 0)

    qt_select_uq_output "select * from test_cloud_build_idx_uq_table order by user_id,username,age,address"

    // test delete predicate
    sql """DROP TABLE IF EXISTS test_cloud_build_idx_del_tab;"""

    sql """
       CREATE TABLE test_cloud_build_idx_del_tab
        (
          `user_id` LARGEINT NOT NULL,
          `username` VARCHAR(50) NOT NULL,
          `age` int,
          `address` VARCHAR(500)
        )
        duplicate KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true"
        );
    """

    sql """insert into test_cloud_build_idx_del_tab values 
        (1, "aa", 23, "aaaaa"),(2, "bb", 24, "bbbbb"),(3, "cc", 25, "ccccc"),(4, "dd", 26, "dddd"),(5, "ee", 26, "eeeee")"""

    sql """insert into test_cloud_build_idx_del_tab values 
        (1, "aa", 23, "aaaaa"),(2, "bb", 24, "bbbbb"),(3, "cc", 25, "ccccc"),(4, "dd", 26, "dddd"),(5, "ee", 26, "eeeee")"""

    sql """insert into test_cloud_build_idx_del_tab values 
        (1, "aa", 23, "aaaaa"),(2, "bb", 24, "bbbbb"),(3, "cc", 25, "ccccc"),(4, "dd", 26, "dddd"),(5, "ee", 26, "eeeee")"""

    sql """delete from test_cloud_build_idx_del_tab where age=23;"""

    qt_select_del_tab_ret1 """select count(1) from test_cloud_build_idx_del_tab where age=23;"""
    qt_select_del_tab_ret2 """select count(1) from test_cloud_build_idx_del_tab where age!=23;"""

    sql """create index idx1 on test_cloud_build_idx_del_tab(address) using inverted;"""

    build_index_on_table("idx1", "test_cloud_build_idx_del_tab")
    wait_for_last_build_index_finish("test_cloud_build_idx_del_tab", timeout)

    check_inverted_index_filter_rows("select * from test_cloud_build_idx_del_tab" +
            " where address='eeeee' order by user_id,username,age,address", 12)

    qt_select_del_tab_ret3 """select count(1) from test_cloud_build_idx_del_tab where age=23;"""
    qt_select_del_tab_ret4 """select count(1) from test_cloud_build_idx_del_tab where age!=23;"""

    sql """drop index idx1 on test_cloud_build_idx_del_tab;"""
    wait_for_last_build_index_finish("test_cloud_build_idx_del_tab", timeout)

    check_inverted_index_filter_rows("select * from test_cloud_build_idx_del_tab" +
            " where address='eeeee' order by user_id,username,age,address", 0)

    qt_select_del_tab_ret5 """select count(1) from test_cloud_build_idx_del_tab where age=23;"""
    qt_select_del_tab_ret6 """select count(1) from test_cloud_build_idx_del_tab where age!=23;"""

}