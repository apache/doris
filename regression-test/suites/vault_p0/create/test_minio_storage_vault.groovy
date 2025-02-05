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

suite ("test_minio_storage_vault") {

    if (!isCloudMode()) {
        logger.warn("skip this test, because this case only run in cloud mode")
    }
    if (!enableStoragevault()) {
        logger.info("skip this test case")
        return
    }
    String enabled = context.config.otherConfigs.get("enableExternalMinioTest")
    if (enabled == null && enabled.equalsIgnoreCase("false")) {
        logger.warn("skip this test, because minio service isn't available")
    }
    String extMinioHost = context.config.otherConfigs.get("extMinioHost")
    String extMinioPort = context.config.otherConfigs.get("extMinioPort")
    String extMinioAk = context.config.otherConfigs.get("extMinioAk")
    String extMinioSk = context.config.otherConfigs.get("extMinioSk")
    String extMinioRegion = context.config.otherConfigs.get("extMinioRegion")
    String extMinioBucket = context.config.otherConfigs.get("extMinioBucket")

    String suffix = UUID.randomUUID().toString().split("-")[0]
    String vaultName = "minio_vault_" + suffix

    sql """
         CREATE STORAGE VAULT IF NOT EXISTS ${vaultName}
         PROPERTIES (
             "type"="S3",
             "s3.endpoint"="${extMinioHost}:${extMinioPort}",
             "s3.access_key" = "${extMinioAk}",
             "s3.secret_key" = "${extMinioSk}",
             "s3.region" = "${extMinioRegion}",
             "s3.root.path" = "test_${suffix}",
             "s3.bucket" = "${extMinioBucket}",
             "provider" = "S3"
         );
    """

    sql """ drop table if exists user"""
    sql """
        CREATE TABLE `user` (
          `id` int NULL,
          `name` varchar(32) NULL
        ) 
        PROPERTIES (
            "storage_vault_name" = "${vaultName}"
        );
    """

    sql """
        insert into user values (1,'Tom'), (2, 'Jelly'), (3, 'Spike'), (4, 'Tuffy');
    """

    order_qt_sql "select * from user"
}
