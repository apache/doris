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

suite("test_mow_txn_case") {

    // TODO: bugfix
    def fullPriv = ["SELECT_PRIV"/*, "LOAD_PRIV"*/, "ALTER_PRIV", "CREATE_PRIV", "DROP_PRIV"]
    def nowPriv = []
    def recursionPriv = { fullPrivList, idx, nowPrivList, num, callback ->
        for (; (num - nowPrivList.size() <= fullPrivList.size() - idx) && (nowPrivList.size()) < num; ++idx) {
            nowPrivList.push(fullPrivList[idx])
            call(fullPrivList, idx + 1, nowPrivList, num, callback)
            nowPrivList.pop()
        }
        if (nowPrivList.size() == num) {
            String privStr = ""
            for (int i = 0; i < num; ++i) {
                privStr += nowPrivList[i]
                if (i < num - 1) {
                    privStr += ", "
                }
            }
            callback.call(privStr)
        }
    }

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_mow_txn_case")
        return
    }
    def txnTableName = "tbl_txn_case"
    def test_num = 0
    sql "DROP TABLE IF EXISTS ${txnTableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${txnTableName} 
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1 
           PROPERTIES ( 
                "enable_unique_key_merge_on_write" = "true",
               "replication_allocation" = "tag.location.default: 1"
           )
        """
    sql """ALTER TABLE ${txnTableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${txnTableName}"
    target_sql """
                  CREATE TABLE if NOT EXISTS ${txnTableName} 
                  (
                      `test` INT,
                      `id` INT
                  )
                  ENGINE=OLAP
                  UNIQUE KEY(`test`, `id`)
                  DISTRIBUTED BY HASH(id) BUCKETS 1 
                  PROPERTIES ( 
                        "enable_unique_key_merge_on_write" = "true",
                      "replication_allocation" = "tag.location.default: 1"
                  )
              """
    assertTrue(syncer.getTargetMeta("${txnTableName}"))

    


    logger.info("=== Test 1: common txn case ===")
    test_num = 1
    sql """
            INSERT INTO ${txnTableName} VALUES (${test_num}, 0)
        """
    assertTrue(syncer.getBinlog("${txnTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.beginTxn("${txnTableName}"))
    assertTrue(syncer.ingestBinlog())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    target_sql " sync "
    def res = target_sql """SELECT * FROM ${txnTableName} WHERE test=${test_num}"""
    assertEquals(res.size(), 1)




    logger.info("=== Test 2: Wrong BeginTxnRequest context case ===")


    logger.info("=== Test 2.1: Begin a txn with non-existent table case ===")
    assertTrue(syncer.beginTxn("tbl_non_existent") == false)


    logger.info("=== Test 2.2: Begin a txn with duplicate labels case ===")
    assertTrue(syncer.beginTxn("${txnTableName}") == false)


    // End Test 2
    syncer.closeBackendClients()




    logger.info("=== Test 3: Begin a txn with different priv user case ===")
    test_num = 3
    sql """
            INSERT INTO ${txnTableName} VALUES (${test_num}, 0)
        """
    assertTrue(syncer.getBinlog("${txnTableName}"))


    logger.info("=== Test 3.1: Begin a txn with non-existent user set in syncer case ===")
    syncer.context.user = "this_is_an_invalid_user"
    syncer.context.passwd = "this_is_an_invalid_user"
    assertTrue(syncer.beginTxn("${txnTableName}") == false)


    logger.info("=== Test 3.2: Begin a txn with no priv user case ===")
    def noPrivUser = "no_priv_user1"
    def emptyTable = "tbl_empty_test"
    target_sql "DROP TABLE IF EXISTS ${emptyTable}"
    target_sql """
                CREATE TABLE if NOT EXISTS ${emptyTable} 
                (
                    `test` INT,
                    `id` INT
                )
                ENGINE=OLAP
                UNIQUE KEY(`test`, `id`)
                DISTRIBUTED BY HASH(id) BUCKETS 1 
                PROPERTIES ( 
                    "enable_unique_key_merge_on_write" = "true",
                    "replication_allocation" = "tag.location.default: 1"
                )
            """
    target_sql """DROP USER IF EXISTS ${noPrivUser}"""
    target_sql """CREATE USER ${noPrivUser} IDENTIFIED BY '123456'"""
    target_sql """GRANT ALL ON ${context.config.defaultDb}.* TO ${noPrivUser}"""
    target_sql """GRANT ALL ON TEST_${context.dbName}.${emptyTable} TO ${noPrivUser}"""
    syncer.context.user = "${noPrivUser}"
    syncer.context.passwd = "123456"
    assertTrue(syncer.beginTxn("${txnTableName}") == false)

    // TODO: bugfix
    // Recursively selecting privileges, 
    // if not all privileges are obtained, txn should not be began
    logger.info("=== Test 3.3: Begin a txn with low priv user case ===")
    def lowPrivUser = "low_priv_user0"
    target_sql """DROP USER IF EXISTS ${lowPrivUser}"""
    target_sql """CREATE USER ${lowPrivUser} IDENTIFIED BY '123456'"""
    target_sql """GRANT ALL ON ${context.config.defaultDb}.* TO ${lowPrivUser}"""
    syncer.context.user = "${lowPrivUser}"
    syncer.context.passwd = "123456"

    def beginTxnCallback = { privStr ->
        target_sql """GRANT ${privStr} ON TEST_${context.dbName}.${txnTableName} TO ${lowPrivUser}"""
        assertTrue((syncer.beginTxn("${txnTableName}")) == false)
        target_sql """REVOKE ${privStr} ON TEST_${context.dbName}.${txnTableName} FROM ${lowPrivUser}"""
    }

    for (int i = 1; i <= 4; ++i) {
        recursionPriv.call(fullPriv, 0, nowPriv, i, beginTxnCallback)
    }

    logger.info("=== Test 3.4: Complete the txn with SHOW_PRIV user case ===")
    def showPrivUser = "show_priv_user0"
    target_sql """DROP USER IF EXISTS ${showPrivUser}"""
    target_sql """CREATE USER ${showPrivUser} IDENTIFIED BY '123456'"""
    target_sql """GRANT ALL ON ${context.config.defaultDb}.* TO ${showPrivUser}"""
    target_sql """
                  GRANT 
                  SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV 
                  ON TEST_${context.dbName}.${txnTableName}
                  TO ${showPrivUser}
               """
    syncer.context.user = "${showPrivUser}"
    syncer.context.passwd = "123456"
    assertTrue(syncer.beginTxn("${txnTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.ingestBinlog())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    res = target_sql """SELECT * FROM ${txnTableName} WHERE test=${test_num}"""
    assertEquals(res.size(), 1)

    // End Test 3
    syncer.context.user = context.config.feSyncerUser
    syncer.context.passwd = context.config.feSyncerPassword
    syncer.closeBackendClients()




    logger.info("=== Test 4: Wrong CommitTxnRequest context case ===")
    test_num = 4
    sql """
            INSERT INTO ${txnTableName} VALUES (${test_num}, 0)
        """
    assertTrue(syncer.getBinlog("${txnTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.beginTxn("${txnTableName}"))
    assertTrue(syncer.ingestBinlog())

    
    logger.info("=== Test 4.1: Wrong txnId case ===")
    def originTxnId = syncer.context.txnId
    syncer.context.txnId = -1
    assertTrue(syncer.commitTxn() == false)
    syncer.context.txnId = originTxnId


    logger.info("=== Test 4.2: Wrong commit info case ===")
    // TODO: bugfix
    // def originCommitInfos = syncer.resetCommitInfos()
    // syncer.context.addCommitInfo(-1, -1)
    // assertTrue(syncer.commitTxn()) == false)


    logger.info("=== Test 4.3: Empty commit info case ===")
    // TODO: bugfix
    // assertTrue(syncer.commitTxn() == false)


    logger.info("=== Test 4.4: duplicate txnId case ===")
    // TODO: bugfix
    // def lastCommitInfo = syncer.copyCommitInfos()
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    target_sql " sync "
    res = target_sql """SELECT * FROM ${txnTableName} WHERE test=${test_num}"""
    assertEquals(res.size(), 1)
    // syncer.context.commitInfos = lastCommitInfo
    // assertTrue(syncer.commitTxn() == false)

    // End Test 4
    syncer.closeBackendClients()
    



    logger.info("=== Test 5: User root beginTxn, Other user commitTxn case ===")
    test_num = 5
    sql """
            INSERT INTO ${txnTableName} VALUES (${test_num}, 0)
        """
    assertTrue(syncer.getBinlog("${txnTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.beginTxn("${txnTableName}"))
    assertTrue(syncer.ingestBinlog())


    logger.info("=== Test 5.1: Non-existent user commitTxn case ===")
    syncer.context.user = "this_is_an_invalid_user"
    syncer.context.passwd = "this_is_an_invalid_user"
    assertTrue(syncer.commitTxn() == false)


    logger.info("=== Test 5.2: No priv user commitTxn case ===")
    syncer.context.user = "${noPrivUser}"
    syncer.context.passwd = "123456"
    assertTrue(syncer.commitTxn() == false)


    logger.info("=== Test 5.3: Low priv user commitTxn case ===")
    syncer.context.user = "${lowPrivUser}"
    syncer.context.passwd = "123456"

    def commitTxnCallback = { privStr ->
        target_sql """GRANT ${privStr} ON TEST_${context.dbName}.${txnTableName} TO ${lowPrivUser}"""
        assertTrue(syncer.commitTxn() == false)
        target_sql """REVOKE ${privStr} ON TEST_${context.dbName}.${txnTableName} FROM ${lowPrivUser}"""
    }
    for (int i = 1; i <= 4; ++i) {
        recursionPriv.call(fullPriv, 0, nowPriv, i, commitTxnCallback)
    }


    logger.info("=== Test 5.4: SHOW_PRIV user commitTxn case ===")
    syncer.context.user = "${showPrivUser}"
    syncer.context.passwd = "123456"
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    target_sql " sync "
    res = target_sql """SELECT * FROM ${txnTableName} WHERE test=${test_num}"""
    assertEquals(res.size(), 1)

    // End Test 5
    syncer.context.user = context.config.feSyncerUser
    syncer.context.passwd = context.config.feSyncerPassword
    syncer.closeBackendClients()
    
}