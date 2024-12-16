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

suite("test_txn_insert") {
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_txn_case")
        return
    }
    def txnTableName = "test_txn_insert"
    for (int i = 0; i < 3; i++) {
        sql "DROP TABLE IF EXISTS ${txnTableName}_${i} force"
        sql """
           CREATE TABLE if NOT EXISTS ${txnTableName}_${i} 
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           DUPLICATE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1 
           PROPERTIES ( 
               "replication_allocation" = "tag.location.default: 1"
           )
        """
        sql """ALTER TABLE ${txnTableName}_${i} set ("binlog.enable" = "true")"""
        assertTrue(syncer.getSourceMeta("${txnTableName}_${i}"))

        target_sql "DROP TABLE IF EXISTS ${txnTableName}_${i} force"
        target_sql """
                  CREATE TABLE if NOT EXISTS ${txnTableName}_${i} 
                  (
                      `test` INT,
                      `id` INT
                  )
                  ENGINE=OLAP
                  DUPLICATE KEY(`test`, `id`)
                  DISTRIBUTED BY HASH(id) BUCKETS 1 
                  PROPERTIES ( 
                      "replication_allocation" = "tag.location.default: 1"
                  )
              """
        assertTrue(syncer.getTargetMeta("${txnTableName}_${i}"))
    }

    def sync = { String tableName ->
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.getBackendClients())
        assertTrue(syncer.beginTxn("${tableName}"))
        assertTrue(syncer.ingestBinlog())
        assertTrue(syncer.commitTxn())
        assertTrue(syncer.checkTargetVersion())
        target_sql " sync "
    }

    def check_row_count = { String tableName, int count ->
        def res = target_sql """SELECT count() FROM ${tableName}"""
        logger.info("target row count: ${res}")
        assertEquals(count, res[0][0])
    }

    // test duplicate table
    logger.info("=== Test 1: insert values ===")
    sql """ INSERT INTO ${txnTableName}_0 VALUES (1, 0) """
    sync("${txnTableName}_0")
    def res = target_sql """SELECT * FROM ${txnTableName}_0 WHERE test=1 """
    assertEquals(res.size(), 1)

    logger.info("=== Test 2: txn insert values ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_0 VALUES (20, 0), (30, 0) """
    sql """ INSERT INTO ${txnTableName}_0 VALUES (40, 0) """
    sql """ commit """
    sync("${txnTableName}_0")
    check_row_count("${txnTableName}_0", 4)

    logger.info("=== Test 3: txn insert select into 1 table ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_1")
    check_row_count("${txnTableName}_1", 4)

    logger.info("=== Test 4: txn insert select into 1 table twice ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_1")
    check_row_count("${txnTableName}_1", 12)

    logger.info("=== Test 5: txn insert select into 2 tables ===")
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_2 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_1 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_1")
    check_row_count("${txnTableName}_1", 20)
    check_row_count("${txnTableName}_2", 4)

    // test multi partitions
    logger.info("=== Test 6: table with multi partitions ===")
    sql """ DROP TABLE IF EXISTS ${txnTableName}_3 force """
    sql """
        CREATE TABLE if NOT EXISTS ${txnTableName}_3 (`test` INT, `id` INT)
        DUPLICATE KEY(`test`)
        PARTITION BY RANGE(test) ( FROM (1) TO (50) INTERVAL 10 )
        DISTRIBUTED BY HASH(id) BUCKETS 2 
        PROPERTIES ( "replication_num" = "1" )
    """
    sql """ALTER TABLE ${txnTableName}_3 set ("binlog.enable" = "true")"""
    assertTrue(syncer.getSourceMeta("${txnTableName}_3"))
    target_sql "DROP TABLE IF EXISTS ${txnTableName}_3 force"
    target_sql """
        CREATE TABLE if NOT EXISTS ${txnTableName}_3 (`test` INT, `id` INT)
        DUPLICATE KEY(`test`)
        PARTITION BY RANGE(test) ( FROM (1) TO (50) INTERVAL 10 )
        DISTRIBUTED BY HASH(id) BUCKETS 2 
        PROPERTIES ( "replication_num" = "1" )
    """
    assertTrue(syncer.getTargetMeta("${txnTableName}_3"))
    sql """ set enable_insert_strict = false """
    sql """ begin """
    sql """ INSERT INTO ${txnTableName}_3 select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_3 PARTITION (p_1_11, p_11_21) select * from ${txnTableName}_0 """
    sql """ INSERT INTO ${txnTableName}_3 PARTITION (p_31_41) select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_3")
    check_row_count("${txnTableName}_3", 7)
    sql """ set enable_insert_strict = true """

    // delete and insert
    logger.info("=== Test 7: delete and insert ===")
    sql """ begin """
    sql """ delete from ${txnTableName}_2 where test < 30 """
    sql """ insert into ${txnTableName}_2 select * from ${txnTableName}_0 where test < 30 """
    sql """ commit """
    sync("${txnTableName}_2")
    check_row_count("${txnTableName}_2", 4)

    // insert and delete
    /*logger.info("=== Test 8: insert and delete ===")
    sql """ begin """
    sql """ insert into ${txnTableName}_2 select * from ${txnTableName}_0 where test < 30 """
    sql """ delete from ${txnTableName}_2 where test < 30 """
    sql """ commit """
    sync("${txnTableName}_2")
    check_row_count("${txnTableName}_2", 4)*/

    // mow table
    logger.info("=== Test 9: mow table insert ===")
    sql """ DROP TABLE IF EXISTS ${txnTableName}_u0 force """
    sql """
        CREATE TABLE if NOT EXISTS ${txnTableName}_u0 (`test` INT, `id` INT)
        UNIQUE KEY(`test`)
        DISTRIBUTED BY HASH(test) BUCKETS 2 
        PROPERTIES ( "replication_num" = "1" )
    """
    sql """ALTER TABLE ${txnTableName}_u0 set ("binlog.enable" = "true")"""
    assertTrue(syncer.getSourceMeta("${txnTableName}_u0"))
    target_sql "DROP TABLE IF EXISTS ${txnTableName}_u0 force"
    target_sql """
        CREATE TABLE if NOT EXISTS ${txnTableName}_u0 (`test` INT, `id` INT)
        UNIQUE KEY(`test`)
        DISTRIBUTED BY HASH(test) BUCKETS 2 
        PROPERTIES ( "replication_num" = "1" )
    """
    assertTrue(syncer.getTargetMeta("${txnTableName}_u0"))
    sql """ insert into ${txnTableName}_0 values (1, 1) """
    // target_sql """ insert into ${txnTableName}_0 values (1, 1) """
    sql """ begin """
    sql """ insert into ${txnTableName}_u0 select * from ${txnTableName}_1 """
    sql """ insert into ${txnTableName}_u0 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_u0")
    check_row_count("${txnTableName}_u0", 4)
    res = target_sql """SELECT * FROM ${txnTableName}_u0 WHERE test=1 """
    assertEquals(res.size(), 1)
    assertEquals(res[0][1], 1)

    logger.info("=== Test 10: mow table update ===")
    sql """ begin """
    sql """ insert into ${txnTableName}_u0 select * from ${txnTableName}_0 """
    sql """ update ${txnTableName}_u0 set id = id + 10 where test = 1 """
    sql """ commit """
    sync("${txnTableName}_u0")
    check_row_count("${txnTableName}_u0", 4)
    res = target_sql """SELECT * FROM ${txnTableName}_u0 WHERE test=1 """
    assertEquals(res.size(), 1)
    assertEquals(res[0][1], 11)

    logger.info("=== Test 11: mow table delete from using ===")

    // test table with multi indexes
    logger.info("=== Test 12: table with multi indexes ===")
    target_sql """ create materialized view mv_${txnTableName}_3 as select id from ${txnTableName}_3; """
    createMV """ create materialized view mv_${txnTableName}_3 as select id from ${txnTableName}_3; """
    res = sql """ select id from ${txnTableName}_3 """
    assertEquals(res.size(), 7)
    res = target_sql """ select id from ${txnTableName}_3 """
    assertEquals(res.size(), 7)
    /*sql """ begin """
    sql """ insert into ${txnTableName}_3 select * from ${txnTableName}_0 """
    sql """ insert into ${txnTableName}_3 select * from ${txnTableName}_0 """
    sql """ commit """
    sync("${txnTableName}_3")
    check_row_count("${txnTableName}_3", 12)
    res = sql """ select id from ${txnTableName}_3 """
    assertEquals(res.size(), 12)
    res = target_sql """ select id from ${txnTableName}_3 """
    assertEquals(res.size(), 12)*/

    // test schema change
    // test one sub txn is error
    // test only enable one table binlog

    // End Test
    syncer.closeBackendClients()
}