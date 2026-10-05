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

import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.client.FrontendClientImpl

suite("test_backup_restore_get_snapshot", "backup_restore,docker") {
    def options = new ClusterOptions()
    options.beConfigs += ["enable_java_support=false"]
    options.feNum = 3
    options.beNum = 3
    options.beDisks = ['HDD=1', 'SSD=1']

    docker(options) {
        def syncer = getSyncer()

        sql """
            CREATE TABLE backup_table (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "3"
            )
            """

        List<String> values = []
        for (int i = 1; i <= 10; ++i) {
            values.add("(${i}, ${i})")
        }
        sql "INSERT INTO backup_table VALUES ${values.join(",")}"
        def result = sql "SELECT * FROM backup_table"
        assertEquals(result.size(), values.size());

        sql """
            BACKUP SNAPSHOT snapshot_name
            TO `__keep_on_local__`
            ON (backup_table)
        """

        syncer.waitSnapshotFinish()

        // Stop the master FE, to elect a follower as the new master.
        def stopFeIndex = cluster.getMasterFe().index
        cluster.stopFrontends(stopFeIndex)

        // Wait a new master
        def masterElected = false
        for (int i = 0; i < 60; i++) {
            def frontends = cluster.getFrontends();
            for (int j = 0; j < frontends.size(); j++) {
                logger.info("stop fe {}, idx {}, is master {}", stopFeIndex, j, frontends.get(j).isMaster)
                if (j == stopFeIndex) {
                    continue
                }
                if (frontends.get(j).isMaster) {
                    masterElected = true
                    break
                }
            }
            if (masterElected) {
                break
            }
            sleep(1000)
        }

        assertTrue(masterElected)

        // Run master sql, force to referesh the underlying 
        def user = context.config.jdbcUser
        def password = context.config.jdbcPassword
        def fe = cluster.getMasterFe()
        def url = String.format(
                "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                fe.host, fe.queryPort)
        url = Config.buildUrlWithDb(url, context.dbName)
        logger.info("connect to docker cluster: suite={}, url={}", name, url)
        connect(user, password, url) {
            // The snapshot backupMeta & snapshotInfo must exists.
            assertTrue(syncer.getSnapshot("snapshot_name", "backup_table"))
        }
    }
}


