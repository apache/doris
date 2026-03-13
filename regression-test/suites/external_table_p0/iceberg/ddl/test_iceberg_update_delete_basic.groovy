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

suite("test_iceberg_update_delete_basic", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_update_delete_basic"
    String dbName = "test_update_delete_basic_db"
    String tableName = "test_update_delete_basic_tbl"
    String tableNamePartition = "test_update_delete_basic_tbl_par"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
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
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    def formats = ["parquet", "orc"]
    for (String format : formats) {
        logger.info("Run update/delete test with format ${format}")
        String formatTableName = "${tableName}_${format}"
        String formatTableNamePartition = "${tableNamePartition}_${format}"

        sql """drop table if exists ${formatTableName}"""
        sql """
            CREATE TABLE ${formatTableName} (
                id INT,
                name STRING,
                age INT
            ) ENGINE=iceberg
            PROPERTIES (
                "format-version" = "2",
                "write.format.default" = "${format}"
            )
        """

        sql """
            INSERT INTO ${formatTableName} VALUES
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 35)
        """

        def q01 = "qt_${format}_q01"
        "${q01}" """UPDATE ${formatTableName} SET name = 'Updated' WHERE id = 1"""
        def q02 = "order_qt_${format}_q02"
        "${q02}" """SELECT * FROM ${formatTableName}"""
        // assertEquals(1, updated.size())
        // assertEquals("Updated", updated[0][0])

        def q03 = "qt_${format}_q03"
        "${q03}" """DELETE FROM ${formatTableName} WHERE id = 2"""
        def q04 = "order_qt_${format}_q04"
        "${q04}" """SELECT * FROM ${formatTableName}"""
        // assertEquals(2, countAfterDelete[0][0])

        def deleteFiles = sql """
            SELECT file_path, file_format
            FROM ${catalogName}.${dbName}.${formatTableName}\$delete_files
        """
        assert deleteFiles.size() > 0 : "Delete files should be created for ${formatTableName}"
        for (def row : deleteFiles) {
            String filePath = row[0].toString()
            String fileFormat = row[1].toString()
            assert filePath.contains("/data/delete_pos_")
            assert filePath.endsWith(format == "parquet" ? ".parquet" : ".orc")
            assert fileFormat.equalsIgnoreCase(format)
        }

        sql """drop table if exists ${formatTableNamePartition}"""
        sql """
            CREATE TABLE ${formatTableNamePartition} (
                id INT,
                name STRING,
                age INT,
                dt DATE
            ) ENGINE=iceberg
            PARTITION BY LIST (DAY(dt)) ()
            PROPERTIES (
                "format-version" = "2",
                "write.format.default" = "${format}"
            )
        """

        sql """
            INSERT INTO ${formatTableNamePartition} VALUES
            (10, 'Ann', 20, '2024-01-01'),
            (11, 'Ben', 21, '2024-01-02'),
            (12, 'Cat', 22, '2024-01-03')
        """

        def q06 = "qt_${format}_q06"
        "${q06}" """UPDATE ${formatTableNamePartition} SET name = 'UpdatedP' WHERE id = 10"""
        def q07 = "order_qt_${format}_q07"
        "${q07}" """SELECT * FROM ${formatTableNamePartition}"""

        def q08 = "qt_${format}_q08"
        "${q08}" """DELETE FROM ${formatTableNamePartition} WHERE id = 11"""
        def q09 = "order_qt_${format}_q09"
        "${q09}" """SELECT * FROM ${formatTableNamePartition}"""

        def partitionDeleteFiles = sql """
            SELECT file_path, file_format
            FROM ${catalogName}.${dbName}.${formatTableNamePartition}\$delete_files
        """
        assert partitionDeleteFiles.size() > 0 : "Delete files should be created for ${formatTableNamePartition}"
        for (def row : partitionDeleteFiles) {
            String filePath = row[0].toString()
            String fileFormat = row[1].toString()
            assert filePath.contains("/data/delete_pos_")
            assert filePath.endsWith(format == "parquet" ? ".parquet" : ".orc")
            assert fileFormat.equalsIgnoreCase(format)
        }

        sql """drop table if exists ${formatTableName}"""
        sql """drop table if exists ${formatTableNamePartition}"""
    }

    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
