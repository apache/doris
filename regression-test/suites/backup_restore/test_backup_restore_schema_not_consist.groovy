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

import org.apache.doris.regression.suite.ClusterOptions

suite('test_backup_restore_atomic_schema_not_consist', 'docker') {
    String suiteName = "test_backup_restore_atomic_schema_not_consist"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    String snapshotName = "${suiteName}_snapshot_" + UUID.randomUUID().toString().replace('-', '')
    def exist = { res -> Boolean
        return res.size() != 0
    }
    def isNewSchema = { res -> Boolean
        return res[0][1].contains("`k1` int NULL")
    }

    def options = new ClusterOptions()
    // contains 3 frontends
    options.feNum = 3
    // contains 3 backends
    options.beNum = 3
    // each backend has 1 HDD disk and 3 SSD disks
    options.beDisks = ['HDD=1', 'SSD=3']

    docker (options) {
        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        sql "DROP TABLE IF EXISTS ${dbName}.t1"
        sql "DROP TABLE IF EXISTS ${dbName}.t2"
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.t1(
              `k` int NULL,
              `v` int NULL
            ) ENGINE = OLAP
            DISTRIBUTED BY HASH(k) BUCKETS 4
            PROPERTIES (
              "replication_num" = "3"
            );
        """
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.t2(
              `k` int NULL,
              `v` int NULL
            ) ENGINE = OLAP
            DISTRIBUTED BY HASH(k) BUCKETS 4
            PROPERTIES (
              "replication_num" = "3"
            );
        """

        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}
            TO `${repoName}`
            ON (t1, t2)
        """
        syncer.waitSnapshotFinish(dbName)

        def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
        assertTrue(snapshot != null)

        sql " drop table ${dbName}.t1 "
        sql " drop table ${dbName}.t2 "

        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.t1(
              `k1` int NULL,
              `v1` int NULL
            ) ENGINE = OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 4
            PROPERTIES (
              "replication_num" = "3"
            );
        """
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.t2(
              `k1` int NULL,
              `v1` int NULL
            ) ENGINE = OLAP
            DISTRIBUTED BY HASH(k1) BUCKETS 4
            PROPERTIES (
              "replication_num" = "3"
            );
        """
        def res = sql " show create table ${dbName}.t1 "
        assertTrue(isNewSchema(res))
        res = sql " show create table ${dbName}.t2 "
        assertTrue(isNewSchema(res))
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON ( `t1`, `t2`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true",
                "atomic_restore" = "true"
            )
        """
        syncer.waitRestoreError(dbName, "already exist but with different schema")

        def frontends = cluster.getAllFrontends()
        for (def fe : frontends) {
            def feUrl = "jdbc:mysql://${fe.host}:${fe.queryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
            feUrl = context.config.buildUrlWithDb(feUrl, context.dbName)
            connect('root', '', feUrl) {
                log.info("connect to ${fe.host}:${fe.queryPort}")
                sql " use ${dbName} "
                res = sql " show tables like \"t1\" "
                assertTrue(exist(res))
                res = sql " show tables like \"t2\" "
                assertTrue(exist(res))
                res = sql " show create table t1 "
                assertTrue(isNewSchema(res))
                res = sql " show create table t2 "
                assertTrue(isNewSchema(res))
            }
        }

        sql "DROP TABLE ${dbName}.t1 FORCE"
        sql "DROP TABLE ${dbName}.t2 FORCE"
        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}
