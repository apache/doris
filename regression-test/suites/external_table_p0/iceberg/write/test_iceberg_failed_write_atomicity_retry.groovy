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

suite("test_iceberg_failed_write_atomicity_retry", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_failed_write_atomicity_retry"
    String dbName = "failed_write_atomicity_retry_db"
    String tableName = "failed_write_atomicity_retry"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1'
        )
    """

    try {
        sql """switch ${catalogName}"""
        sql """drop database if exists ${dbName} force"""
        sql """create database ${dbName}"""
        sql """use ${dbName}"""

        sql """
            create table ${tableName} (
                id int,
                payload string
            )
        """
        sql """insert into ${tableName} values (1, 'committed')"""
        sql """set enable_strict_cast = true"""

        long snapshotsBeforeFailure = (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long
        long filesBeforeFailure = (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long

        // A pipeline failure after rows reach the Iceberg sink must not publish a partial snapshot.
        // Retrying the corrected logical write must therefore make each row visible exactly once.
        test {
            sql """
                insert into ${tableName}
                select cast(if(number = 1, 'invalid-id', cast(number + 2 as string)) as int),
                       concat('candidate-', number)
                from numbers('number' = '3')
            """
            exception "can't cast to INT in strict mode"
        }

        assertEquals(snapshotsBeforeFailure, (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long)
        assertEquals(filesBeforeFailure, (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long)
        order_qt_failed_write_state """
            select id, payload from ${tableName} order by id
        """

        sql """
            insert into ${tableName}
            select number + 2, concat('candidate-', number)
            from numbers('number' = '3')
        """
        assertEquals(snapshotsBeforeFailure + 1, (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long)
        order_qt_failed_write_retry """
            select id, payload, count(*)
            from ${tableName}
            group by id, payload
            order by id
        """
    } finally {
        sql """drop database if exists ${catalogName}.${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
