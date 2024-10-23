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

package org.apache.doris.regression.suite

suite("test_MoW_backup_restore", "p1") {

    def syncer = getSyncer()
    def repo = "__keep_on_local__"
    def tableName = "demo_MoW"
    sql """drop table if exists ${tableName}"""
    sql """CREATE TABLE IF NOT EXISTS ${tableName} 
    ( `user_id` INT NOT NULL, `value` INT NOT NULL)
    UNIQUE KEY(`user_id`) 
    DISTRIBUTED BY HASH(`user_id`) 
    BUCKETS 1 
    PROPERTIES ("replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true",
    "enable_unique_key_merge_on_write" = "true");"""

    // version1 (1,1)(2,2)
    sql """insert into ${tableName} values(1,1),(2,2)"""
    sql """backup snapshot ${context.dbName}.snapshot1 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_3 """select * from ${tableName} order by user_id"""

    // version2 (1,10)(2,2)
    sql """insert into ${tableName} values(1,10)"""
    sql """backup snapshot ${context.dbName}.snapshot2 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_4 """select * from ${tableName} order by user_id"""

    // version3 (1,100)(2,2)
    sql """update ${tableName} set value = 100 where user_id = 1"""
    sql """backup snapshot ${context.dbName}.snapshot3 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_5 """select * from ${tableName} order by user_id"""

    // version4 (2,2)
    sql """delete from ${tableName} where user_id = 1"""
    sql """backup snapshot ${context.dbName}.snapshot4 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_6 """select * from ${tableName} order by user_id"""

    // version1 (1,1)(2,2)
    assertTrue(syncer.getSnapshot("snapshot1", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_7 """select * from ${tableName} order by user_id"""

    // version2 (1,10)(2,2)
    assertTrue(syncer.getSnapshot("snapshot2", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_8 """select * from ${tableName} order by user_id"""
    // version3 (1,100)(2,2)
    assertTrue(syncer.getSnapshot("snapshot3", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_9 """select * from ${tableName} order by user_id"""
    // version4 (2,2)
    assertTrue(syncer.getSnapshot("snapshot4", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_10 """select * from ${tableName} order by user_id"""

    sql """drop table if exists ${tableName}"""
    sql """CREATE TABLE IF NOT EXISTS ${tableName} 
    ( `user_id` INT NOT NULL, `value` INT NOT NULL)
    UNIQUE KEY(`user_id`) 
    DISTRIBUTED BY HASH(`user_id`) 
    BUCKETS 1 
    PROPERTIES ("replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true",
    "enable_unique_key_merge_on_write" = "true");""" 

    // version1 (1,1)(2,2)
    assertTrue(syncer.getSnapshot("snapshot1", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_11 """select * from ${tableName} order by user_id"""

    // version2 (1,10)(2,2)
    assertTrue(syncer.getSnapshot("snapshot2", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_12 """select * from ${tableName} order by user_id"""
    // version3 (1,100)(2,2)
    assertTrue(syncer.getSnapshot("snapshot3", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_13 """select * from ${tableName} order by user_id"""
    // version4 (2,2)
    assertTrue(syncer.getSnapshot("snapshot4", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_14 """select * from ${tableName} order by user_id"""
}
