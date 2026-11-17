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

suite("test_iceberg_write_required_null_values_negative",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_required_null_values_negative"
    String dbName = "iceberg_write_required_null_values_negative_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    for (String fileFormat : ["orc", "parquet"]) {
        String tableName = "required_values_${fileFormat}"
        sql """
            create table ${tableName} (
                id int not null,
                required_text string not null
            )
            partition by list (bucket(8, id)) ()
            properties (
                "format-version" = "2",
                "write.format.default" = "${fileFormat}"
            )
        """

        long snapshotsBefore = (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long
        long filesBefore = (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long
        long rowsBefore = (sql """
            select count(*) from ${tableName}
        """)[0][0] as long

        // W07-S01: A thrown NULL error is insufficient proof of atomicity; all
        // visible and metadata state must remain at the pre-statement baseline.
        test {
            sql """insert into ${tableName} values (1, null)"""
            exception "required field 'required_text' contains NULL"
        }
        assertEquals(snapshotsBefore, (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long)
        assertEquals(filesBefore, (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long)
        assertEquals(rowsBefore, (sql """
            select count(*) from ${tableName}
        """)[0][0] as long)
    }

    // Spark can express required nested fields that Doris DDL cannot declare directly.
    for (String fileFormat : ["orc", "parquet"]) {
        String tableName = "required_nested_${fileFormat}"
        spark_iceberg """
            drop table if exists demo.${dbName}.${tableName}
        """
        spark_iceberg """
            create table demo.${dbName}.${tableName} (
                id int,
                info struct<required_metric: int not null, required_label: string not null>
            )
            using iceberg
            tblproperties (
                'format-version' = '2',
                'write.format.default' = '${fileFormat}'
            )
        """
        spark_iceberg """
            insert into demo.${dbName}.${tableName}
            values (1, named_struct('required_metric', 10, 'required_label', 'old-label'))
        """
        sql """refresh table ${tableName}"""

        // The STRUCT itself is optional. A NULL parent masks its required children and is a valid row.
        sql """insert into ${tableName} values (2, null)"""
        sql """refresh table ${tableName}"""

        long snapshotsBefore = (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long
        long filesBefore = (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long
        long rowsBefore = (sql """
            select count(*) from ${tableName}
        """)[0][0] as long

        test {
            sql """
                insert into ${tableName}
                values (
                    3,
                    named_struct(
                        'required_metric', cast(null as int),
                        'required_label', 'new-label'
                    )
                )
            """
            // Current FE versions may reject the nullable-to-required cast during planning; later
            // versions may let the block reach BE. Both paths must identify the required child.
            exception "required_metric"
        }
        assertEquals(snapshotsBefore, (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long)
        assertEquals(filesBefore, (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long)
        assertEquals(rowsBefore, (sql """
            select count(*) from ${tableName}
        """)[0][0] as long)
    }

    // Iceberg MAP keys are always required, including when the MAP parent is optional.
    for (String fileFormat : ["orc", "parquet"]) {
        String tableName = "required_map_key_${fileFormat}"
        sql """
            create table ${tableName} (
                id int not null,
                attrs map<int, int>
            )
            partition by list (bucket(8, id)) ()
            properties (
                "format-version" = "2",
                "write.format.default" = "${fileFormat}"
            )
        """

        // A NULL MAP parent masks all entries, so it must remain writable.
        sql """insert into ${tableName} values (1, null)"""
        sql """refresh table ${tableName}"""

        long snapshotsBefore = (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long
        long filesBefore = (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long
        long rowsBefore = (sql """
            select count(*) from ${tableName}
        """)[0][0] as long

        test {
            sql """insert into ${tableName} values (2, map(cast(null as int), 2))"""
            exception "map key 'attrs.key' contains NULL"
        }
        assertEquals(snapshotsBefore, (sql """
            select count(*) from ${tableName}\$snapshots
        """)[0][0] as long)
        assertEquals(filesBefore, (sql """
            select count(*) from ${tableName}\$files
        """)[0][0] as long)
        assertEquals(rowsBefore, (sql """
            select count(*) from ${tableName}
        """)[0][0] as long)
    }
}
