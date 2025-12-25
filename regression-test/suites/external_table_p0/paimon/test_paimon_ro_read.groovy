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

suite("test_paimon_ro_read", "p0,external,doris,external_docker,external_docker_doris") {
    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog_name = "test_paimon_ro_read_catalog"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    try {
        sql """drop catalog if exists ${catalog_name}"""

        sql """
                CREATE CATALOG ${catalog_name} PROPERTIES (
                        'type' = 'paimon',
                        'warehouse' = 's3://warehouse/wh',
                        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                        's3.access_key' = 'admin',
                        's3.secret_key' = 'password',
                        's3.path.style.access' = 'true'
                );
            """
        sql """switch `${catalog_name}`"""
        sql """use test_paimon_ro_read_db"""

        // normal read can get latest data
        order_qt_normal """select * from paimon_pk_for_ro"""

        // ro read can only get data in full compaction view
        order_qt_ro """select * from paimon_pk_for_ro@ro"""

        test {
            sql """select * from paimon_incr@ro('startSnapshotId'='0')"""
            exception "Please use '@ro' for Paimon"
        }

        test {
            sql """select * from paimon_incr@ro('name'='branch1')"""
            exception "Please use '@ro' for Paimon"
        }

        test {
            sql """select * from paimon_incr@ro(tag1)"""
            exception "Please use '@ro' for Paimon"
        }
    } finally {
        sql """drop catalog if exists ${catalog_name}"""
    }
}


