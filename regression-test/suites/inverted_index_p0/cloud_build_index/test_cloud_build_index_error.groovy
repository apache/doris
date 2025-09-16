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

suite("test_cloud_build_error") {
    if (!isCloudMode()) {
        return;
    }

    def timeout = 60000
    def delta_time = 1000

    def check_build_index_err_state = { table_name, error_key_word ->
        def useTime = 0
        for(int t = delta_time; t <= timeout; t += delta_time){
            def alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                assertTrue(false, "unexpected finish state")
            } else if (alter_res.contains("CANCELLED")) {
                logger.info(table_name + " latest alter job failed, detail: " + alter_res)
                assertTrue(alter_res.contains(error_key_word))
                return
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= timeout, "check_build_index_err_state timeout")
    }

    sql "set enable_add_index_for_new_data = true"

    // build index for inverted index with parser failed
    sql "DROP TABLE IF EXISTS test_cloud_build_parser_index_tab"
    sql """
      CREATE TABLE test_cloud_build_parser_index_tab
        (
          `user_id` int NOT NULL,
          `username` VARCHAR(50) NOT NULL,
          `age` int,
          `address` VARCHAR(40)
        )
        duplicate KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
    """

    sql """
     insert into test_cloud_build_parser_index_tab values 
        (1, "aa", 23, "aaaaa"),
        (2, "bb", 24, "bbbbb"),
        (3, "cc", 25, "ccccc"),
        (4, "dd", 26, "dddd"),
        (5, "ee", 26, "eeeee")
    """

    sql """
            create index idx1 on test_cloud_build_parser_index_tab(address) using inverted PROPERTIES("parser"="standard") ;
       """

    wait_for_last_col_change_finish("test_cloud_build_parser_index_tab", timeout)

    test {
        sql """build index on test_cloud_build_parser_index_tab;"""

        exception "INVERTED index is not needed to build"
    }

    // rename cause build index failed
    // build index for inverted index with parser failed
    sql "DROP TABLE IF EXISTS test_cloud_build_idx_rename_col"
    sql """
      CREATE TABLE test_cloud_build_idx_rename_col
        (
          `user_id` int NOT NULL,
          `username` VARCHAR(50) NOT NULL,
          `age` int,
          `address` VARCHAR(40)
        )
        duplicate KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
    """

    sql """
     insert into test_cloud_build_idx_rename_col values 
        (1, "aa", 23, "aaaaa"),
        (2, "bb", 24, "bbbbb"),
        (3, "cc", 25, "ccccc"),
        (4, "dd", 26, "dddd"),
        (5, "ee", 26, "eeeee")
    """

    sql """
            create index idx1 on test_cloud_build_idx_rename_col(address) using inverted;
       """

    sql """
        alter table test_cloud_build_idx_rename_col rename column username uname;
    """

    build_index_on_table("idx1", "test_cloud_build_idx_rename_col")

    check_build_index_err_state("test_cloud_build_idx_rename_col", "col is dropped")

    // drop column cause failed
    sql "DROP TABLE IF EXISTS test_cloud_build_idx_drop_col"
    sql """
      CREATE TABLE test_cloud_build_idx_drop_col
        (
          `user_id` int NOT NULL,
          `username` VARCHAR(50) NOT NULL,
          `age` int,
          `address` VARCHAR(40)
        )
        duplicate KEY(`user_id`, `username`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
    """

    sql """
     insert into test_cloud_build_idx_drop_col values 
        (1, "aa", 23, "aaaaa"),
        (2, "bb", 24, "bbbbb"),
        (3, "cc", 25, "ccccc"),
        (4, "dd", 26, "dddd"),
        (5, "ee", 26, "eeeee")
    """

    sql """
            create index idx1 on test_cloud_build_idx_drop_col(address) using inverted;
       """

    sql """
        alter table test_cloud_build_idx_drop_col drop column age;
    """

    build_index_on_table("idx1", "test_cloud_build_idx_drop_col")

    check_build_index_err_state("test_cloud_build_idx_drop_col", "col is dropped")

    sql """
         alter table test_cloud_build_idx_drop_col ADD COLUMN `age` int  AFTER username;
    """

    build_index_on_table("idx1", "test_cloud_build_idx_drop_col")

    check_build_index_err_state("test_cloud_build_idx_drop_col", "col id not match")
}



