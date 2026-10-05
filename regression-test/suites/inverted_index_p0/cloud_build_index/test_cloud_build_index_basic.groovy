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

suite("test_cloud_build_index_basic"){
    if (!isCloudMode()) {
        return;
    }

    def timeout = 60000

    sql "set enable_add_index_for_new_data = true"

    sql """ set enable_profile = true;"""
    sql """ set profile_level = 2;"""

    def tabName1 = "test_cloud_build_index_basic_table"

    sql "DROP TABLE IF EXISTS ${tabName1}"

    // 1 string type, inverted index

    sql """
        CREATE TABLE test_cloud_build_index_basic_table
        (
          `user_id` LARGEINT NOT NULL,
          `username` VARCHAR(50) NOT NULL,
          `age` int,
          `address` VARCHAR(40)
        )
        duplicate KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
    """

    sql """
        insert into test_cloud_build_index_basic_table values 
        (1, "aa", 23, "aaaaa"),
        (2, "bb", 24, "bbbbb"),
        (3, "cc", 25, "ccccc"),
        (4, "dd", 26, "dddd"),
        (5, "ee", 26, "eeeee")
    """

    sql """create index idx1 on test_cloud_build_index_basic_table(address) using inverted;"""
    check_inverted_index_filter_rows("select * from test_cloud_build_index_basic_table where address='dddd'", 0)

    build_index_on_table("idx1", tabName1)
    wait_for_last_build_index_finish(tabName1, timeout)
    check_inverted_index_filter_rows("select * from test_cloud_build_index_basic_table where address='dddd'", 4)

    sql "drop index idx1 on test_cloud_build_index_basic_table"
    wait_for_last_build_index_finish(tabName1, timeout)
    check_inverted_index_filter_rows("select * from test_cloud_build_index_basic_table where address='dddd'", 0)

    // 2 string type, ngram index
    sql "set enable_function_pushdown = true;"
    sql """create index idx2 on test_cloud_build_index_basic_table(address) using ngram_bf properties("gram_size"="3", "bf_size"="256");"""
    check_bf_index_filter_rows("select * from test_cloud_build_index_basic_table where address like '%sdf%'", 0)

    build_index_on_table("idx2", tabName1)
    wait_for_last_build_index_finish(tabName1, timeout)
    check_bf_index_filter_rows("select * from test_cloud_build_index_basic_table where address like '%sdf%'", 5)

    sql "drop index idx2 on test_cloud_build_index_basic_table"
    wait_for_last_build_index_finish(tabName1, timeout)
    check_bf_index_filter_rows("select * from test_cloud_build_index_basic_table where address like '%sdf%'", 0)

    // test add column/reset varchar len
    sql "DROP TABLE IF EXISTS test_cloud_light_sc_table"

    sql """
        CREATE TABLE test_cloud_light_sc_table
        (
          `user_id` LARGEINT NOT NULL,
          `username` VARCHAR(50) NOT NULL,
          `age` int NOT NULL,
          `address` VARCHAR(40) NOT NULL
        )
        duplicate KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("disable_auto_compaction" = "true");
    """

    sql """
        insert into test_cloud_light_sc_table values 
        (1, "aa", 23, "aaaaa"),
        (2, "bb", 24, "bbbbb"),
        (3, "cc", 25, "ccccc"),
        (4, "dd", 26, "dddd"),
        (5, "ee", 26, "eeeee")
    """

    sql """alter table test_cloud_light_sc_table add column content varchar(50) not null DEFAULT '3';"""

    sql """
        insert into test_cloud_light_sc_table values 
        (6, "aa", 23, "hh", "hh1"),
        (7, "bb", 24, "ii","ii1"),
        (8, "cc", 25, "jj","jj1"),
        (9, "dd", 26, "kk", "kk1"),
        (10, "ee", 26, "ll", "ll1")
    """

    sql """create index idx1 on test_cloud_light_sc_table(address) using inverted;"""
    sql """create index idx2 on test_cloud_light_sc_table(content) using inverted;"""

    check_inverted_index_filter_rows("select * from test_cloud_light_sc_table where address='eeeee'", 0)
    check_inverted_index_filter_rows("select * from test_cloud_light_sc_table where content='jj1'", 0)

    sql """alter table test_cloud_light_sc_table modify column address varchar(400);"""
    sql """build index on test_cloud_light_sc_table;"""
    wait_for_last_build_index_finish("test_cloud_light_sc_table", timeout)
    check_inverted_index_filter_rows("select * from test_cloud_light_sc_table where address='eeeee'", 4)
    check_inverted_index_filter_rows("select * from test_cloud_light_sc_table where content='jj1'", 4)

    sql """drop index idx1 on test_cloud_light_sc_table;"""
    wait_for_last_build_index_finish("test_cloud_light_sc_table", timeout)
    check_inverted_index_filter_rows("select * from test_cloud_light_sc_table where address='eeeee'", 0)
    sql """drop index idx2 on test_cloud_light_sc_table;"""
    wait_for_last_build_index_finish("test_cloud_light_sc_table", timeout)
    check_inverted_index_filter_rows("select * from test_cloud_light_sc_table where content='jj1'", 0)

    qt_select_sc_output "select * from test_cloud_light_sc_table order by user_id,username,age,address,content"

}