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

suite("test_paimon_ctas_atomicity_negative",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_ctas_atomicity_negative"
    String dbName = "paimon_ctas_atomicity_negative_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        spark_paimon_multi """
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.ctas_target;
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        sql """
            create table ctas_target engine=paimon
            as select cast(1 as int) as id, cast('candidate' as string) as payload
        """
        assertEquals(1, (sql """show tables like 'ctas_target'""").size())
        order_qt_ctas_rows """select id, payload from ctas_target"""

        // IF NOT EXISTS remains a no-op and must not append the SELECT result.
        sql """
            create table if not exists ctas_target engine=paimon
            as select cast(2 as int) as id, cast('ignored' as string) as payload
        """
        assertEquals(1, (sql """show tables like 'ctas_target'""").size())
        order_qt_after_if_not_exists """select id, payload from ctas_target"""

        // An existing non-idempotent target keeps catalog error precedence and existing data.
        test {
            sql """
                create table ctas_target engine=paimon
                as select cast(2 as int) as id, cast('replacement' as string) as payload
            """
            exception "already exists"
        }
        order_qt_after_existing_target_error """select id, payload from ctas_target"""
    } finally {
        spark_paimon """drop table if exists paimon.${dbName}.ctas_target"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
