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

suite("test_get_binlog_case_index") {
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_get_binlog_case_index")
        return
    }

    def insert_data = { tableName ->
        [
            """INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99);""",
            """INSERT INTO ${tableName} VALUES (2, "andy", "andy love apple", 100);""",
            """INSERT INTO ${tableName} VALUES (2, "bason", "bason hate pear", 99);""",
            """INSERT INTO ${tableName} VALUES (3, "andy", "andy love apple", 100);""",
            """INSERT INTO ${tableName} VALUES (3, "bason", "bason hate pear", 99);"""
        ]
    }

    def sqls = { tableName ->
        [
            """ select * from ${tableName} order by id, name, hobbies, score """,
            """ select * from ${tableName} where name match "andy" order by id, name, hobbies, score """,
            """ select * from ${tableName} where hobbies match "pear" order by id, name, hobbies, score """,
            """ select * from ${tableName} where score < 100 order by id, name, hobbies, score """
        ]
    }

    def run_sql = { tableName -> 
        sqls(tableName).each { sqlStatement ->
            def target_res = target_sql sqlStatement
            def res = sql sqlStatement
            assertEquals(res, target_res)
        }
    }

    def create_table_v1 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
        """
    }

    def create_table_v2 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = "V2");
        """
    }

    def create_table_mow_v1 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1", 
            "binlog.enable" = "true", 
            "enable_unique_key_merge_on_write" = "true");
        """
    }

    def create_table_mow_v2 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1", 
            "binlog.enable" = "true", 
            "enable_unique_key_merge_on_write" = "true",
            "inverted_index_storage_format" = "V2");
        """
    }

    def run_test = { create_table, tableName ->
        long seq = -1
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql create_table
        sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
        assertTrue(syncer.getBinlog("${tableName}"))
        long firstSeq = syncer.context.seq

        logger.info("=== Test 1: normal case ===")
        insert_data(tableName).each { sqlStatement ->
            sql sqlStatement
            assertTrue(syncer.getBinlog("${tableName}"))
        }

        long endSeq = syncer.context.seq

        logger.info("=== Test 2: Abnormal seq case ===")
        logger.info("=== Test 2.1: too old seq case ===")
        syncer.context.seq = -1
        assertTrue(syncer.context.seq == -1)
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.context.seq == firstSeq)


        logger.info("=== Test 2.2: too new seq case ===")
        syncer.context.seq = endSeq + 100
        assertTrue((syncer.getBinlog("${tableName}")) == false)


        logger.info("=== Test 2.3: not find table case ===")
        assertTrue(syncer.getBinlog("this_is_an_invalid_tbl") == false)


        logger.info("=== Test 2.4: seq between first and end case ===")
        long midSeq = (firstSeq + endSeq) / 2
        syncer.context.seq = midSeq
        assertTrue(syncer.getBinlog("${tableName}"))
        long test5Seq = syncer.context.seq
        assertTrue(firstSeq <= test5Seq && test5Seq <= endSeq)

        logger.info("=== Test 3: Get binlog with different priv user case ===")
        logger.info("=== Test 3.1: read only user get binlog case ===")
        // TODO: bugfix
        // syncer.context.seq = -1
        // readOnlyUser = "read_only_user"
        // sql """DROP USER IF EXISTS ${readOnlyUser}"""
        // sql """CREATE USER ${readOnlyUser} IDENTIFIED BY '123456'"""
        // sql """GRANT ALL ON ${context.config.defaultDb}.* TO ${readOnlyUser}"""
        // sql """GRANT SELECT_PRIV ON TEST_${context.dbName}.${tableName} TO ${readOnlyUser}"""
        // syncer.context.user = "${readOnlyUser}"
        // syncer.context.passwd = "123456"
        // assertTrue(syncer.getBinlog("${tableName}"))

    }

    // inverted index format v1
    logger.info("=== Test 1: Inverted index format v1 case ===")
    def tableName = "tbl_get_binlog_case_index_v1"
    run_test.call(create_table_v1(tableName), tableName)
    
    // inverted index format v2
    logger.info("=== Test 2: Inverted index format v2 case ===")
    tableName = "tbl_get_binlog_case_index_v2"
    run_test.call(create_table_v2(tableName), tableName)

    // inverted index format v1 with mow
    logger.info("=== Test 3: Inverted index format v1 with mow case ===")
    tableName = "tbl_get_binlog_case_index_mow_v1"
    run_test.call(create_table_mow_v1(tableName), tableName)

    // inverted index format v2 with mow
    logger.info("=== Test 4: Inverted index format v2 with mow case ===")
    tableName = "tbl_get_binlog_case_index_mow_v2"
    run_test.call(create_table_mow_v2(tableName), tableName)

    logger.info("=== Test 3: no priv user get binlog case ===")
    syncer.context.seq = -1
    def noPrivUser = "no_priv_user2"
    def emptyTable = "tbl_empty_test"
    sql "DROP TABLE IF EXISTS ${emptyTable}"
    sql """
        CREATE TABLE ${emptyTable} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
    """
    sql """CREATE USER IF NOT EXISTS ${noPrivUser} IDENTIFIED BY '123456'"""
    sql """GRANT ALL ON ${context.config.defaultDb}.* TO ${noPrivUser}"""
    syncer.context.user = "${noPrivUser}"
    syncer.context.passwd = "123456"
    assertTrue((syncer.getBinlog("${tableName}")) == false)
    

    logger.info("=== Test 3.3: Non-existent user set in syncer get binlog case ===")
    syncer.context.user = "this_is_an_invalid_user"
    syncer.context.passwd = "this_is_an_invalid_user"
    assertTrue(syncer.getBinlog("${tableName}", false) == false)
}