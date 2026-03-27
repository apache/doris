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

suite("test_iceberg_merge_into_basic", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_merge_into_basic"
    String dbName = "test_merge_into_basic_db"
    String tableName = "test_merge_into_basic_tbl"
    String tableNamePartition = "test_merge_into_basic_tbl_par"
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
        logger.info("Run merge-into test with format ${format}")
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
        "${q01}" """
            MERGE INTO ${formatTableName} t
            USING (
                SELECT 1 AS id, 'Alice_new' AS name, 26 AS age, 'U' AS flag
                UNION ALL
                SELECT 2, 'Bob', 30, 'D'
                UNION ALL
                SELECT 4, 'Dora', 28, 'I'
            ) s
            ON t.id = s.id
            WHEN MATCHED AND s.flag = 'D' THEN DELETE
            WHEN MATCHED THEN UPDATE SET
                name = s.name,
                age = s.age
            WHEN NOT MATCHED THEN INSERT (id, name, age)
            VALUES (s.id, s.name, s.age)
        """

        def q02 = "order_qt_${format}_q02"
        "${q02}" """SELECT * FROM ${formatTableName}"""
        // assertEquals(3, rows.size())
        // assertEquals([1, "Alice_new", 26], rows[0])
        // assertEquals([3, "Charlie", 35], rows[1])
        // assertEquals([4, "Dora", 28], rows[2])

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
            (1, 'Alice', 25, '2024-01-01'),
            (2, 'Bob', 30, '2024-01-02'),
            (3, 'Charlie', 35, '2024-01-03')
        """

        def q04 = "qt_${format}_q04"
        "${q04}" """
            MERGE INTO ${formatTableNamePartition} t
            USING (
                SELECT 1 AS id, 'Alice_new' AS name, 26 AS age, DATE '2024-01-01' AS dt, 'U' AS flag
                UNION ALL
                SELECT 2, 'Bob', 30, DATE '2024-01-02', 'D'
                UNION ALL
                SELECT 4, 'Dora', 28, DATE '2024-01-04', 'I'
            ) s
            ON t.id = s.id
            WHEN MATCHED AND s.flag = 'D' THEN DELETE
            WHEN MATCHED THEN UPDATE SET
                name = s.name,
                age = s.age
            WHEN NOT MATCHED THEN INSERT (id, name, age, dt)
            VALUES (s.id, s.name, s.age, s.dt)
        """

        def q05 = "order_qt_${format}_q05"
        "${q05}" """SELECT * FROM ${formatTableNamePartition}"""

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
