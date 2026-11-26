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

suite("test_iceberg_scan_schema_carrier_names",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists test_iceberg_scan_schema_carrier_names"""

    spark_iceberg_multi """
        SET spark.sql.shuffle.partitions=1;
        CREATE DATABASE IF NOT EXISTS demo.iceberg_scan_schema_carrier_names_db;
        DROP TABLE IF EXISTS demo.iceberg_scan_schema_carrier_names_db.mixed_case_orc;
        CREATE TABLE demo.iceberg_scan_schema_carrier_names_db.mixed_case_orc (
            ID INT,
            Name STRING,
            Payload STRUCT<Child: INT, Other: STRING>
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='orc'
        );
        INSERT INTO demo.iceberg_scan_schema_carrier_names_db.mixed_case_orc
        SELECT /*+ COALESCE(1) */ id, name, payload
        FROM VALUES
            (1, 'one', named_struct('Child', 11, 'Other', 'alpha')),
            (2, 'two', named_struct('Child', 22, 'Other', 'beta')),
            (3, 'three', named_struct('Child', 33, 'Other', 'gamma'))
            AS t(id, name, payload);

        DROP TABLE IF EXISTS demo.iceberg_scan_schema_carrier_names_db.mixed_case_partition_orc;
        CREATE TABLE demo.iceberg_scan_schema_carrier_names_db.mixed_case_partition_orc (
            id INT,
            City STRING
        ) USING iceberg
        PARTITIONED BY (City)
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='orc'
        );
        INSERT INTO demo.iceberg_scan_schema_carrier_names_db.mixed_case_partition_orc
        VALUES (1, 'Beijing'), (2, 'Beijing');
    """

    sql """
        CREATE CATALOG test_iceberg_scan_schema_carrier_names PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0',
            'meta.cache.iceberg.schema.ttl-second'='0'
        )
    """

    try {
        sql """
            ALTER TABLE
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
            CREATE TAG mixed_case_initial
        """

        spark_iceberg_multi """
            ALTER TABLE demo.iceberg_scan_schema_carrier_names_db.mixed_case_partition_orc
                RENAME COLUMN City TO city_name;
            INSERT INTO demo.iceberg_scan_schema_carrier_names_db.mixed_case_partition_orc
            VALUES (3, 'Shanghai');
        """
        sql """refresh catalog test_iceberg_scan_schema_carrier_names"""

        qt_mixed_case_scan """
            SELECT ID, Name
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_orc`
            ORDER BY ID
        """

        qt_mixed_case_topn """
            SELECT Name
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_orc`
            ORDER BY ID
            LIMIT 2
        """

        qt_mixed_case_nested """
            SELECT ID, Payload.Child, Payload.Other
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_orc`
            ORDER BY ID
        """

        qt_pinned_partition_schema """
            SELECT id, City
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
            FOR VERSION AS OF 'mixed_case_initial'
            ORDER BY id
        """

        qt_pinned_partition_filter """
            SELECT id, City
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
            FOR VERSION AS OF 'mixed_case_initial'
            WHERE City = 'Beijing'
            ORDER BY id
        """

        qt_pinned_partition_topn """
            SELECT id, City
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
            FOR VERSION AS OF 'mixed_case_initial'
            ORDER BY id
            LIMIT 1
        """

        qt_current_partition_schema """
            SELECT id, city_name
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
            ORDER BY id
        """

        qt_pinned_count """
            SELECT COUNT(*)
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
            FOR VERSION AS OF 'mixed_case_initial'
        """

        qt_current_count """
            SELECT COUNT(*)
            FROM
                `test_iceberg_scan_schema_carrier_names`.`iceberg_scan_schema_carrier_names_db`.`mixed_case_partition_orc`
        """
    } finally {
        sql """drop catalog if exists test_iceberg_scan_schema_carrier_names"""
    }
}
