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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.kms.model.DisableKeyRequest
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest

suite("test_key_id_deleted", "docker") {
    def options = new ClusterOptions()

    // create key, get cmk id
    def credProvider = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(context.config.tdeAk, context.config.tdeSk)
    );
    def client = KmsClient.builder()
            .region(Region.of(context.config.tdeKeyRegion))
            .endpointOverride(URI.create(context.config.tdeKeyEndpoint))
            .credentialsProvider(credProvider)
            .build();
    def resp = client.createKey();
    def keyId = resp.keyMetadata().keyId();

    options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'sys_log_verbose_modules=org',
            "doris_tde_key_endpoint=${context.config.tdeKeyEndpoint}",
            "doris_tde_key_region=${context.config.tdeKeyRegion}",
            "doris_tde_key_provider=${context.config.tdeKeyProvider}",
            "doris_tde_algorithm=${context.config.tdeAlgorithm}",
            "doris_tde_key_id=${keyId}"
    ]

    options.feNum = 2
    options.beNum = 1
    options.cloudMode = true

    options.connectToFollower = false

    options.tdeAk = context.config.tdeAk
    options.tdeSk = context.config.tdeSk

    docker(options) {
        def tblName = "test_key_id_deleted"
        sql """ DROP TABLE IF EXISTS ${tblName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v` varchar(10) NOT NULL) 
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 8
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "enable_unique_key_merge_on_write" = "true"
                )
                """

        (1..20).each { i ->
            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
        }
        (1..20).each { i ->
            sql """ INSERT INTO ${tblName} VALUES (${i}, "${i}") """
        }

        qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

        sql """ DROP TABLE IF EXISTS ${tblName} """

        // disable cmk id
        def req = DisableKeyRequest.builder().keyId(keyId).build();
        client.disableKey((DisableKeyRequest)req);

        cluster.restartFrontends()
        cluster.restartBackends()
        sleep(30000)
        context.reconnectFe()

        test {
            sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v` varchar(10) NOT NULL) 
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 8
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "enable_unique_key_merge_on_write" = "true"
                )
                """

            exception("The master key has not been decrypted")
        }


        // delete cmk id
        def deleteReq = ScheduleKeyDeletionRequest.builder().keyId(keyId).build();
        client.scheduleKeyDeletion((ScheduleKeyDeletionRequest)deleteReq)

        cluster.restartFrontends()
        cluster.restartBackends()
        sleep(30000)
        context.reconnectFe()
        
        test {
            sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v` varchar(10) NOT NULL) 
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 8
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "enable_unique_key_merge_on_write" = "true"
                )
                """
            exception("The master key has not been decrypted")
        }
    }
}
