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

suite("test_seq_map_backup_restore", "p1") {
    // TODO regresion test case always failed due to Config.max_backup_restore_job_num_per_db is too small
    // backup and restore regression test is too vulnerable, it may need run serially
    // and setting a reasonable value for max_backup_restore_job_num_per_db
    sql """
        admin set frontend config('max_backup_restore_job_num_per_db'=50);
    """

    def dbName = "${context.dbName}"

    def syncer = getSyncer()
    def repo = "__keep_on_local__"
    def tableName = "demo_seq_map"
    sql """drop table if exists ${dbName}.$tableName"""
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """

    // version1 final value should be first insert
    sql """insert into ${dbName}.$tableName values(1,2,2,2,2,2),(2,3,3,3,3,3)"""
    sql """insert into ${dbName}.$tableName values(1,1,1,1,1,1),(2,2,2,2,2,2)"""
    sql """backup snapshot ${dbName}.seq_map_snapshot1 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_3 """select * from ${dbName}.$tableName order by a"""

    // version2 update key 1 values to 10
    sql """insert into ${dbName}.$tableName values(1,10,10,10,10,10)"""
    sql """backup snapshot ${dbName}.seq_map_snapshot2 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_4 """select * from ${dbName}.$tableName order by a"""

    // version3 update key 1 column d to 100
    sql """update ${dbName}.$tableName set d = 100 where a = 1"""
    sql """backup snapshot ${dbName}.seq_map_snapshot3 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_5 """select * from ${dbName}.$tableName order by a"""

    // version4 delete key 1
    sql """delete from ${dbName}.$tableName where a = 1"""
    sql """backup snapshot ${dbName}.seq_map_snapshot4 to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    qt_6 """select * from ${dbName}.$tableName order by a"""

    // version1
    assertTrue(syncer.getSnapshot("seq_map_snapshot1", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_7 """select * from ${dbName}.$tableName order by a"""

    // version2
    assertTrue(syncer.getSnapshot("seq_map_snapshot2", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_8 """select * from ${dbName}.$tableName order by a"""
    // version3
    assertTrue(syncer.getSnapshot("seq_map_snapshot3", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_9 """select * from ${dbName}.$tableName order by a"""
    // version4
    assertTrue(syncer.getSnapshot("seq_map_snapshot4", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_10 """select * from ${dbName}.$tableName order by a"""

    sql """drop table if exists ${dbName}.$tableName"""
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """

    // version1
    assertTrue(syncer.getSnapshot("seq_map_snapshot1", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_11 """select * from ${dbName}.$tableName order by a"""

    // version2
    assertTrue(syncer.getSnapshot("seq_map_snapshot2", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_12 """select * from ${dbName}.$tableName order by a"""
    // version3
    assertTrue(syncer.getSnapshot("seq_map_snapshot3", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_13 """select * from ${dbName}.$tableName order by a"""
    // version4
    assertTrue(syncer.getSnapshot("seq_map_snapshot4", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_14 """select * from ${dbName}.$tableName order by a"""

    sql """drop table if exists ${dbName}.$tableName"""

    tableName = "test_seq_map_seq_without_value"
    sql """drop table if exists ${dbName}.$tableName"""
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `s1` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = ""
            );
    """
    sql "insert into ${dbName}.$tableName(a, s1) values (1,10);"
    sql "insert into ${dbName}.$tableName(a, s1) values (1,5);"
    qt_15 "select a, s1 from ${dbName}.$tableName order by a;"
    sql """backup snapshot ${dbName}.seq_map_seq_without_value_snapshot to ${repo} on (${tableName}) properties("type"="full")"""
    syncer.waitSnapshotFinish()
    assertTrue(syncer.getSnapshot("seq_map_seq_without_value_snapshot", "${tableName}"))
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish()
    qt_16 """select * from ${dbName}.$tableName order by a"""

    sql """drop table if exists ${dbName}.$tableName"""
}
