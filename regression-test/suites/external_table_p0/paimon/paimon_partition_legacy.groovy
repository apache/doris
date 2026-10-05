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

suite("paimon_partition_legacy", "p0,external") {

    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    try {
        String catalog_name = "test_partition_legacy_catalog"
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use test_partition_legacy;"""

        sql """set force_jni_scanner=true"""
        qt_order """ select * from test_partition_legacy_true order by event_value; """
        qt_order """ select * from test_partition_legacy_false order by event_value; """

        sql """set force_jni_scanner=false"""
        qt_order """ select * from test_partition_legacy_true order by event_value; """
        qt_order """ select * from test_partition_legacy_false order by event_value; """

    } finally {
        sql """set force_jni_scanner=false"""
    }

}