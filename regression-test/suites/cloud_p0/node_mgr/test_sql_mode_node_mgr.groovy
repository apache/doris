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
import groovy.json.JsonSlurper

suite('test_sql_mode_node_mgr', 'p1') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.cloudMode = true
    options.sqlModeNodeMgr = true
    options.waitTimeout = 0
    options.feNum = 3
    options.feConfigs += ["resource_not_ready_sleep_seconds=1",
            "heartbeat_interval_second=1",]
    options.noBeCloudInstanceId = true;
    options.noBeMetaServiceEndpoint = true;

    //
    docker(options) {
        logger.info("docker started");
        def result = sql """show frontends; """
        logger.info("show frontends result {}", result);
        // Extract fields from frontends result
        def frontendFields = result.collect { row ->
            [
                name: row[0],
                ip: row[1],
                queryPort: row[4],
                role: row[7],
                isMaster: row[8],
                alive: row[11]
            ]
        }

        logger.info("Extracted frontend fields: {}", frontendFields)

        // Verify at least one frontend is alive and master
        // Verify only one frontend is master
        assert frontendFields.count { it.isMaster == "true" } == 1, "Expected exactly one master frontend"

        // Verify three frontends are alive
        assert frontendFields.count { it.alive == "true" } == 3, "Expected exactly three alive frontends"

        result = sql """show backends; """
        logger.info("show backends result {}", result);

        // Extract fields from backends result
        def backendFields = result.collect { row ->
            [
                id: row[0],
                ip: row[1],
                alive: row[9]
            ]
        }

        logger.info("Extracted backend fields: {}", backendFields)

        // Verify three backends are alive
        assert backendFields.count { it.alive == "true" } == 3, "Expected exactly three alive backends"

        sql """ drop table if exists example_table """
        sql """ CREATE TABLE IF NOT EXISTS example_table (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES (
                    "replication_num" = "1"
                ); """
        sql """ insert into example_table values(1, "1") """
        sql """ select * from example_table """

        logger.info("Restarting frontends and backends...")
        cluster.restartFrontends();
        cluster.restartBackends();

        sleep(30000)
        context.reconnectFe()

        sql """ select * from example_table """

        sql """ insert into example_table values(1, "1") """

        sql """ select * from example_table order by id """
    }

    options.noBeCloudInstanceId = false;
    options.noBeMetaServiceEndpoint = false;

    //
    docker(options) {
        logger.info("docker started");

        // Generate a unique UUID for the root path
        def uuid = UUID.randomUUID().toString()
        logger.info("Generated UUID for root path: ${uuid}")
        // Create a new storage vault
        sql """ CREATE STORAGE VAULT test_vault
                PROPERTIES (
                    "type" = "s3",
                    "s3.endpoint" = "${getS3Endpoint()}",
                    "s3.region" = "${getS3Region()}",
                    "s3.bucket" = "${getS3BucketName()}",
                    "s3.access_key" = "${getS3AK()}",
                    "s3.secret_key" = "${getS3SK()}",
                    "s3.root.path" = "${uuid}",
                    "provider" = "${getS3Provider()}"
                ); """

        // Set the newly created vault as default
        sql """ SET test_vault AS DEFAULT STORAGE VAULT; """

        // Verify the vault was created and set as default
        def vaultResult = sql """ SHOW STORAGE VAULT; """
        logger.info("Show storage vault result: {}", vaultResult)

        assert vaultResult.size() > 0, "Expected at least one storage vault"
        assert vaultResult.any { row -> 
            row[0] == "test_vault" && row[5] == "true"
        }, "Expected 'test_vault' to be created and set as default"

        def result = sql """show frontends; """
        logger.info("show frontends result {}", result);
        // Extract fields from frontends result
        def frontendFields = result.collect { row ->
            [
                name: row[0],
                ip: row[1],
                queryPort: row[4],
                role: row[7],
                isMaster: row[8],
                alive: row[11]
            ]
        }

        logger.info("Extracted frontend fields: {}", frontendFields)

        // Verify at least one frontend is alive and master
        // Verify only one frontend is master
        assert frontendFields.count { it.isMaster == "true" } == 1, "Expected exactly one master frontend"

        // Verify three frontends are alive
        assert frontendFields.count { it.alive == "true" } == 3, "Expected exactly three alive frontends"

        result = sql """show backends; """
        logger.info("show backends result {}", result);

        // Extract fields from backends result
        def backendFields = result.collect { row ->
            [
                id: row[0],
                ip: row[1],
                alive: row[9]
            ]
        }

        logger.info("Extracted backend fields: {}", backendFields)

        // Verify three backends are alive
        assert backendFields.count { it.alive == "true" } == 3, "Expected exactly three alive backends"

        sql """ drop table if exists example_table """
        sql """ CREATE TABLE IF NOT EXISTS example_table (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES (
                    "replication_num" = "1"
                ); """
        sql """ insert into example_table values(1, "1") """
        result = sql """ select * from example_table """
        assert result.size() == 1

        logger.info("Restarting frontends and backends...")
        cluster.restartFrontends();
        cluster.restartBackends();

        sleep(30000)
        context.reconnectFe()

        result = sql """ select * from example_table """
        assert result.size() == 1

        sql """ insert into example_table values(1, "1") """

        result = sql """ select * from example_table order by id """
        assert result.size() == 2

    }
}