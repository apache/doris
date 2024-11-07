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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/tiered-storage/remote-storage.md") {

    def clean = {
        multi_sql """
            DROP TABLE IF EXISTS create_table_use_created_policy;
            DROP TABLE IF EXISTS create_table_not_have_policy;
            DROP TABLE IF EXISTS create_table_partition;
            DROP STORAGE POLICY IF EXISTS test_policy;
            DROP RESOURCE IF EXISTS 'remote_hdfs';
            DROP RESOURCE IF EXISTS 'remote_s3';
        """
    }

    try {
        clean()
        multi_sql """
            DROP TABLE IF EXISTS create_table_use_created_policy;
            DROP STORAGE POLICY IF EXISTS test_policy;
            DROP RESOURCE IF EXISTS 'remote_s3';

            CREATE RESOURCE "remote_s3"
            PROPERTIES
            (
                "type" = "s3",
                "s3.endpoint" = "${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.root.path" = "path/to/root",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.connection.maximum" = "50",
                "s3.connection.request.timeout" = "3000",
                "s3.connection.timeout" = "1000"
            );
            
            
            CREATE STORAGE POLICY test_policy
            PROPERTIES(
                "storage_resource" = "remote_s3",
                "cooldown_ttl" = "1d"
            );
            
            CREATE TABLE IF NOT EXISTS create_table_use_created_policy 
            (
                k1 BIGINT,
                k2 LARGEINT,
                v1 VARCHAR(2048)
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH (k1) BUCKETS 3
            PROPERTIES(
                "storage_policy" = "test_policy",
                "enable_unique_key_merge_on_write" = "false",
                "replication_num" = "1"
            );
        """

        multi_sql """
            DROP TABLE IF EXISTS create_table_use_created_policy;
            DROP STORAGE POLICY IF EXISTS test_policy;
            DROP RESOURCE IF EXISTS 'remote_hdfs';

            CREATE RESOURCE "remote_hdfs" PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS"="127.0.0.1:8120",
                "hadoop.username"="hive",
                "hadoop.password"="hive",
                "dfs.nameservices" = "my_ha",
                "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
                "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
                "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
                "dfs.client.failover.proxy.provider.my_ha" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            );

            CREATE STORAGE POLICY test_policy PROPERTIES (
                    "storage_resource" = "remote_hdfs",
                    "cooldown_ttl" = "300"
            );

            CREATE TABLE IF NOT EXISTS create_table_use_created_policy (
                k1 BIGINT,
                k2 LARGEINT,
                v1 VARCHAR(2048)
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH (k1) BUCKETS 3
            PROPERTIES(
                "storage_policy" = "test_policy",
                "enable_unique_key_merge_on_write" = "false",
                "replication_num" = "1"
            );
        """

        multi_sql """
            DROP TABLE IF EXISTS create_table_not_have_policy;
            CREATE TABLE IF NOT EXISTS create_table_not_have_policy 
            (
                k1 BIGINT,
                k2 LARGEINT,
                v1 VARCHAR(2048)
            )
            UNIQUE KEY(k1)
            PARTITION BY RANGE(k1) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2),
                PARTITION p3 VALUES LESS THAN (3)
            )
            DISTRIBUTED BY HASH (k1) BUCKETS 3
            PROPERTIES(
                "enable_unique_key_merge_on_write" = "false",
                "replication_num" = "1"
            );
        """
        if (!isCloudMode()) {
            sql """ALTER TABLE create_table_not_have_policy set ("storage_policy" = "test_policy");"""
        }
        multi_sql """
            DROP TABLE IF EXISTS create_table_partition;
            CREATE TABLE IF NOT EXISTS create_table_partition 
            (
                k1 BIGINT,
                k2 LARGEINT,
                v1 VARCHAR(2048)
            )
            UNIQUE KEY(k1)
            PARTITION BY RANGE(k1) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2),
                PARTITION p3 VALUES LESS THAN (3)
            )
            DISTRIBUTED BY HASH (k1) BUCKETS 3
            PROPERTIES(
                "enable_unique_key_merge_on_write" = "false",
                "replication_num" = "1"
            );
        """
        if (!isCloudMode()) {
            sql """ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="test_policy");"""
        }
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/tiered-storage/remote-storage.md failed to exec, please fix it", t)
    } finally {
        clean()
    }
}
