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

suite("test_get_binlog_case") {

    def create_table = { TableName ->
        sql "DROP TABLE IF EXISTS ${TableName}"
        sql """
            CREATE TABLE if NOT EXISTS ${TableName} 
            (
                `test` INT,
                `id` INT
            )
            ENGINE=OLAP
            UNIQUE KEY(`test`, `id`)
            DISTRIBUTED BY HASH(id) BUCKETS 1 
            PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1"
            )
        """
    }
    
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_get_binlog_case")
        return
    }
    def seqTableName = "tbl_get_binlog_case"
    def test_num = 0
    def insert_num = 5
    long seq = -1
    create_table.call(seqTableName)
    sql """ALTER TABLE ${seqTableName} set ("binlog.enable" = "true")"""
    sql """
            INSERT INTO ${seqTableName} VALUES (${test_num}, 0)
        """
    assertTrue(syncer.getBinlog("${seqTableName}"))
    long firstSeq = syncer.context.seq




    logger.info("=== Test 1: normal case ===")
    test_num = 1
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${seqTableName} VALUES (${test_num}, ${index})
            """
        assertTrue(syncer.getBinlog("${seqTableName}"))
    }

    long endSeq = syncer.context.seq




    logger.info("=== Test 2: Abnormal seq case ===")
    logger.info("=== Test 2.1: too old seq case ===")
    syncer.context.seq = -1
    assertTrue(syncer.context.seq == -1)
    assertTrue(syncer.getBinlog("${seqTableName}"))
    assertTrue(syncer.context.seq == firstSeq)


    logger.info("=== Test 2.2: too new seq case ===")
    syncer.context.seq = endSeq + 100
    assertTrue((syncer.getBinlog("${seqTableName}")) == false)


    logger.info("=== Test 2.3: not find table case ===")
    assertTrue(syncer.getBinlog("this_is_an_invalid_tbl") == false)


    logger.info("=== Test 2.4: seq between first and end case ===")
    long midSeq = (firstSeq + endSeq) / 2
    syncer.context.seq = midSeq
    assertTrue(syncer.getBinlog("${seqTableName}"))
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
    // sql """GRANT SELECT_PRIV ON TEST_${context.dbName}.${seqTableName} TO ${readOnlyUser}"""
    // syncer.context.user = "${readOnlyUser}"
    // syncer.context.passwd = "123456"
    // assertTrue(syncer.getBinlog("${seqTableName}"))

    
    logger.info("=== Test 3.2: no priv user get binlog case ===")
    syncer.context.seq = -1
    def noPrivUser = "no_priv_user2"
    def emptyTable = "tbl_empty_test"
    sql "DROP TABLE IF EXISTS ${emptyTable}"
    sql """
        CREATE TABLE if NOT EXISTS ${emptyTable} 
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1 
        PROPERTIES ( 
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql """CREATE USER IF NOT EXISTS ${noPrivUser} IDENTIFIED BY '123456'"""
    sql """GRANT ALL ON ${context.config.defaultDb}.* TO ${noPrivUser}"""
    sql """GRANT ALL ON TEST_${context.dbName}.${emptyTable} TO ${noPrivUser}"""
    syncer.context.user = "${noPrivUser}"
    syncer.context.passwd = "123456"
    assertTrue((syncer.getBinlog("${seqTableName}")) == false)
    

    logger.info("=== Test 3.3: Non-existent user set in syncer get binlog case ===")
    syncer.context.user = "this_is_an_invalid_user"
    syncer.context.passwd = "this_is_an_invalid_user"
    assertTrue(syncer.getBinlog("${seqTableName}", false) == false)
}