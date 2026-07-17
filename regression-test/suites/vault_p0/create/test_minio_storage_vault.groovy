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
        logger.info("skip this test case, because enableStoragevault = false")
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

    // **********************************************************************
    // *      case 1: test MinIO as Storage Vault using S3 Path Style       *
    // **********************************************************************
    String suffix = UUID.randomUUID().toString().split("-")[0]
    String vaultName = "minio_vault_" + suffix

    multi_sql """
         CREATE STORAGE VAULT IF NOT EXISTS ${vaultName}
         PROPERTIES (
             "type"="S3",
             "s3.endpoint"="${extMinioHost}:${extMinioPort}",
             "s3.access_key" = "${extMinioAk}",
             "s3.secret_key" = "${extMinioSk}",
             "s3.region" = "${extMinioRegion}",
             "s3.root.path" = "test_${suffix}",
             "s3.bucket" = "${extMinioBucket}",
             "provider" = "S3",
             "use_path_style" = "true"
         );
         drop table if exists user;
         CREATE TABLE `user` (
          `id` int NULL,
          `name` varchar(32) NULL
        ) 
        PROPERTIES (
            "storage_vault_name" = "${vaultName}"
        );
        insert into user values (1,'Tom'), (2, 'Jelly'), (3, 'Spike'), (4, 'Tuffy');
    """

    order_qt_sql "select * from user"

    // **********************************************************************************************
    // *  case 2: test MinIO as Storage Vault not set `use_path_style` equal  `use_path_style=true` *
    // **********************************************************************************************
    String vaultNameNotSetPathStyle = "minio_not_set_path_style_vault_" + suffix
    multi_sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${vaultNameNotSetPathStyle}
         PROPERTIES (
             "type"="S3",
             "s3.endpoint"="${extMinioHost}:${extMinioPort}",
             "s3.access_key" = "${extMinioAk}",
             "s3.secret_key" = "${extMinioSk}",
             "s3.region" = "${extMinioRegion}",
             "s3.root.path" = "test3_${suffix}",
             "s3.bucket" = "${extMinioBucket}",
             "provider" = "S3"
         );
         drop table if exists user3;
         CREATE TABLE `user3` (
          `id` int NULL,
          `name` varchar(32) NULL
        ) 
        PROPERTIES (
            "storage_vault_name" = "${vaultNameNotSetPathStyle}"
        );
        insert into user3 values (1,'Tom'), (2, 'Jelly'), (3, 'Spike'), (4, 'Tuffy');
    """

    def result = sql "SELECT count(*) FROM user3"
    logger.info("result:${result}")
    assertEquals(result[0][0], 4)
    assertEquals(result.size(), 1)

    // **********************************************************************
    // *  case 3: test MinIO as Storage Vault using S3 Virtual Host Style   *
    // **********************************************************************
    String extMinioDomain = context.config.otherConfigs.get("extMinioDomain")
    cmd "echo -ne '\\n${extMinioHost} ${extMinioBucket}.${extMinioDomain}' >> /etc/hosts"
    String vaultNameHostStyle = "minio_host_style_vault_" + suffix
    multi_sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${vaultNameHostStyle}
         PROPERTIES (
             "type"="S3",
             "s3.endpoint"="${extMinioDomain}:${extMinioPort}",
             "s3.access_key" = "${extMinioAk}",
             "s3.secret_key" = "${extMinioSk}",
             "s3.region" = "${extMinioRegion}",
             "s3.root.path" = "test2_${suffix}",
             "s3.bucket" = "${extMinioBucket}",
             "provider" = "S3",
             "use_path_style" = "false"
         );
         drop table if exists user2;
         CREATE TABLE `user2` (
          `id` int NULL,
          `name` varchar(32) NULL
        ) 
        PROPERTIES (
            "storage_vault_name" = "${vaultNameHostStyle}"
        );
        insert into user2 values (1,'Tom'), (2, 'Jelly'), (3, 'Spike'), (4, 'Tuffy');
    """

    order_qt_sql "select * from user2"
}
