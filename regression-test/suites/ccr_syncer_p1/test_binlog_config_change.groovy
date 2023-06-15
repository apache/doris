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

suite("test_binlog_config_change") {

    def syncer = getSyncer()
    def tableName = "tbl_binlog_config_change"
    def test_num = 0
    def insert_num = 5

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

    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: Target cluster follow source cluster case ===")
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

    def res = target_sql """SELECT * FROM ${tableName} WHERE test=${test_num}"""
    assertTrue(res.size() == insert_num)

    // TODO: bugfix
    // test 2: source cluster disable and re-enable binlog
    // target_sql "DROP TABLE IF EXISTS ${tableName}"
    // target_sql """
    //     CREATE TABLE if NOT EXISTS ${tableName} 
    //     (
    //         `test` INT,
    //         `id` INT
    //     )
    //     ENGINE=OLAP
    //     UNIQUE KEY(`test`, `id`)
    //     DISTRIBUTED BY HASH(id) BUCKETS 1 
    //     PROPERTIES ( 
    //         "replication_allocation" = "tag.location.default: 1"
    //     )
    // """
    // sql """ALTER TABLE ${tableName} set ("binlog.enable" = "false")"""
    // sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    // syncer.context.seq = -1

    // assertTrue(syncer.getBinlog("${tableName}"))
    // assertTrue(syncer.beginTxn("${tableName}"))
    // assertTrue(syncer.ingestBinlog())
    // assertTrue(syncer.commitTxn())
    // assertTrue(syncer.checkTargetVersion())

    // res = target_sql """SELECT * FROM ${tableName} WHERE test=${test_num}"""
    // assertTrue(res.size() == insert_num)

}