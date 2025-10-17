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
import org.apache.doris.regression.util.NodeType
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_rollback_snapshot", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs.add("enable_workload_group=false")
    options.beConfigs.add('enable_java_support=false')
    options.msConfigs += [
        'enable_multi_version_status=true',
        'enable_cluster_snapshot=true',
    ]

    docker(options) {
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")

        // create some dbs and tables
        for (int i = 0; i < 5; i++) {
            def dbName = "db_" + i
            sql "create database if not exists ${dbName}"
            for (int j = 0; j < 2; j++) {
                def tableName = "table_" + j
                sql """
                    create table ${dbName}.${tableName} (`k` int NOT NULL, `v` int NOT NULL)
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY HASH(`k`) BUCKETS 1;
                """
            }
        }

        // create snapshot
        sql " ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES ('ttl' = '3600', 'label' = 'test_snapshot'); "

        // wait for snapshot is done
        String snapshotId = null
        for (int i = 0; i < 30; i++) {
            def result = sql_return_maparray "SELECT * FROM information_schema.cluster_snapshots where label = 'test_snapshot';"
            logger.info("snapshot: ${result}")
            for (final def snapshot in result) {
                if (snapshot['STATE'].contains("NORMAL")) {
                    snapshotId = snapshot['ID']
                    break
                }
            }
            if (snapshotId != null) {
                break
            }
            sleep(1000)
        }
        logger.info("snapshotId: ${snapshotId}")
        assertNotNull(snapshotId)

        // create some dbs and tables
        for (int i = 6; i < 10; i++) {
            def dbName = "db_" + i
            sql "create database if not exists ${dbName}"
            for (int j = 0; j < 2; j++) {
                def tableName = "table_" + j
                sql """
                    create table ${dbName}.${tableName} (`k` int NOT NULL, `v` int NOT NULL)
                    DUPLICATE KEY(`k`)
                    DISTRIBUTED BY HASH(`k`) BUCKETS 1;
                """
            }
        }

        // stop be
        cluster.stopBackends()
        // rollback snapshot
        def cluster_snapshot_json = """
            {
                "from_instance_id": "default_instance_id",
                "from_snapshot_id": "${snapshotId}",
                "instance_id": "new_instance_id",
                "name": "new_instance_name",
                "is_succeed": "true"
            }
        """
        def file = new File('cluster_snapshot.json')
        file.text = cluster_snapshot_json
        logger.info('write cluster snapshot json to file: {}', file.absolutePath)
        try {
            cluster.rollbackSnapshotFrontends(file.absolutePath)
            sleep(30000)
            context.reconnectFe()
            def dbs = sql "show databases"
            logger.info("dbs: ${dbs}")
            assertTrue(dbs.toString().contains("db_0"))
            assertFalse(dbs.toString().contains("db_9"))
        } finally {
            file.delete()
        }
    }
}
