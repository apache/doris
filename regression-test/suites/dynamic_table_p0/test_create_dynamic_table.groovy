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

suite("test_dynamic_table", "dynamic_table"){

    def create_none_dynamic_table_result = "fail"
    try {
        sql """
                CREATE TABLE IF NOT EXISTS t_dynamic1
                (
                    ... 
                )
                DISTRIBUTED BY HASH(`name`) BUCKETS 10
                PROPERTIES("replication_num" = "1")
            """
        create_none_dynamic_table_result = "success"
    } catch(Exception ex) {
        logger.info("create none dynamic table result: " + ex)
    }
    assertEquals(create_none_dynamic_table_result, "fail")

    def create_dynamic_table_assign_not_exist_key_result = "fail"
    try {
        sql """
                CREATE TABLE IF NOT EXISTS t_dynamic1
                (
                    name varchar(50),
                    ... 
                )
                DISTRIBUTED BY HASH(`id`) BUCKETS 10
                PROPERTIES("replication_num" = "1")
            """
        create_dynamic_table_assign_not_exist_key_result = "success"
    } catch(Exception ex) {
        logger.info("create dynamic table assign not exist key, result: " + ex)
    }
    assertEquals(create_dynamic_table_assign_not_exist_key_result, "fail")

    def TbName1 = "test_ceate_dymanic_table_1"
    sql "DROP TABLE IF EXISTS ${TbName1}"
    sql """
        CREATE TABLE IF NOT EXISTS ${TbName1}
                (
                    name varchar(50),
                    ... 
                )
                DUPLICATE KEY(`name`)
                DISTRIBUTED BY HASH(`name`) BUCKETS 10
                PROPERTIES("replication_num" = "1")
        """

    def TbName2 = "test_ceate_dymanic_table_2"
    sql "DROP TABLE IF EXISTS ${TbName2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${TbName2}
                (
                    id int,
                    name varchar(50),
                    date datetime,
                    index id_idx(`id`) USING INVERTED COMMENT 'id index',
                    index name_idx(`name`) USING INVERTED PROPERTIES("parser"="english") COMMENT 'name index',
                    index date_idx(`date`) COMMENT 'date index',
                    ... 
                )
                DUPLICATE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 10
                PROPERTIES("replication_num" = "1")
        """

    def TbName3 = "test_ceate_dymanic_table_3"
    sql "DROP TABLE IF EXISTS ${TbName3}"
    sql """
        CREATE TABLE IF NOT EXISTS ${TbName3}
                (
                    `repo.id` int,
                    ... 
                )
                DUPLICATE KEY(`repo.id`)
                DISTRIBUTED BY HASH(`repo.id`) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

    
    def create_children_colume_without_single_quota_result = "fail"
    def TbName4 = "test_ceate_dymanic_table_4"
    try {
        sql """
            CREATE TABLE IF NOT EXISTS ${TbName4}
                (
                    repo.id int,
                    ... 
                )
                DUPLICATE KEY(`repo.id`)
                DISTRIBUTED BY HASH(`repo.id`) BUCKETS 10
                PROPERTIES("replication_num" = "1");
            """
        create_children_colume_without_single_quota_result = "success"
    }catch(Exception ex) {
        logger.info("create dynamic table without single quotation mark, result: " + ex)
    }
    assertEquals(create_children_colume_without_single_quota_result, "fail")

    // test complicate date type
    def TbName5 = "test_ceate_dymanic_table_5"
    try {
        sql """
            CREATE TABLE IF NOT EXISTS ${TbName5}
            (
                date datetime,
                `name.list` array<int>,
                tag ARRAY<NOT_NULL(ARRAY<text>)>,
                data ARRAY<ARRAY<NOT_NULL(ARRAY<date>)>>,
                ...
            )
            DUPLICATE KEY(`date`)
            DISTRIBUTED BY HASH(`date`) BUCKETS 10
            PROPERTIES("replication_num" = "3");
            """
    }catch(Exception ex) {
        logger.info("create array table, result: " + ex)
    }

}
