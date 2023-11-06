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

suite("test_ingest_binlog") {

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_ingest_binlog")
        return
    }

    def tableName = "tbl_ingest_binlog"
    def insert_num = 5
    def test_num = 0
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${tableName} 
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
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql """
                  CREATE TABLE if NOT EXISTS ${tableName} 
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
    assertTrue(syncer.getTargetMeta("${tableName}"))




    logger.info("=== Test 1: Common ingest binlog case ===")
    test_num = 1
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
        """
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.beginTxn("${tableName}"))
        assertTrue(syncer.getBackendClients())
        assertTrue(syncer.ingestBinlog())
        assertTrue(syncer.commitTxn())
        assertTrue(syncer.checkTargetVersion())
        syncer.closeBackendClients()
    }

    res = target_sql """SELECT * FROM ${tableName} WHERE test=${test_num}"""
    assertTrue(res.size() == insert_num)




    logger.info("=== Test 2: Wrong IngestBinlogRequest case ===")
    test_num = 2
    sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 0)
        """
    assertTrue(syncer.getBinlog("${tableName}"))
    assertTrue(syncer.beginTxn("${tableName}"))
    assertTrue(syncer.getBackendClients())


    logger.info("=== Test 2.1: Wrong txnId case ===")
    // TODO: bugfix
    // def originTxnId = syncer.context.txnId
    // syncer.context.txnId = -1
    // assertTrue(syncer.ingestBinlog() == false)
    // syncer.context.txnId = originTxnId


    logger.info("=== Test 2.2: Wrong binlog version case ===")
    // -1 means use the number of syncer.context
    // Boolean ingestBinlog(long fakePartitionId = -1, long fakeVersion = -1)
    // use fakeVersion = 1, 1 is doris be talet first version, so no binlog, only http error
    assertTrue(syncer.ingestBinlog(-1, 1) == false)


    logger.info("=== Test 2.3: Wrong partitionId case ===")
    // TODO: bugfix
    // assertTrue(syncer.ingestBinlog(1, -1) == false)


    logger.info("=== Test 2.4: Right case ===")
    assertTrue(syncer.ingestBinlog())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    res = target_sql """SELECT * FROM ${tableName} WHERE test=${test_num}"""
    assertTrue(res.size() == 1)


    // End Test 2
    syncer.closeBackendClients()
}
