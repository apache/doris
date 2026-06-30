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

suite("test_s3_load_parquet_orc_complex_type", "load_p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String s3Region = getS3Region()
    String s3BucketName = getS3BucketName()

    if (ak == null || ak.isEmpty() || sk == null || sk.isEmpty() || !enableBrokerLoad()) {
        logger.info("S3 broker load is not configured, skip")
        return
    }

    String sourceTable = "test_s3_load_complex_type_src"
    String parquetTable = "test_s3_load_complex_type_parquet"
    String orcTable = "test_s3_load_complex_type_orc"
    String renameParquetTable = "test_s3_load_complex_type_rename_parquet"
    String renameOrcTable = "test_s3_load_complex_type_rename_orc"
    String scalarParquetTable = "test_s3_load_complex_type_scalar_parquet"
    String scalarOrcTable = "test_s3_load_complex_type_scalar_orc"
    String s3BasePath = "regression/s3_load_complex_type/test_s3_load_parquet_orc_complex_type"

    def createComplexTable = { String tableName ->
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                c_int INT,
                c_array ARRAY<INT>,
                c_map MAP<STRING, INT>,
                c_struct STRUCT<f1:INT, f2:STRING>
            )
            DUPLICATE KEY(c_int)
            DISTRIBUTED BY HASH(c_int) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
    }

    def writeComplexFile = { String path, String format ->
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BucketName}/${path}/data_",
                "format" = "${format}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${s3Region}",
                "delete_existing_files" = "true"
            )
            SELECT * FROM ${sourceTable} ORDER BY c_int;
        """
    }

    def writeRenamedComplexFile = { String path, String format ->
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BucketName}/${path}/data_",
                "format" = "${format}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${s3Region}",
                "delete_existing_files" = "true"
            )
            SELECT c_int, c_array AS tmp_array, c_map AS tmp_map, c_struct AS tmp_struct
            FROM ${sourceTable}
            ORDER BY c_int;
        """
    }

    def writeScalarRenameFile = { String path, String format ->
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BucketName}/${path}/data_",
                "format" = "${format}",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${s3Region}",
                "delete_existing_files" = "true"
            )
            SELECT c_int, c_int AS tmp_array
            FROM ${sourceTable}
            ORDER BY c_int;
        """
    }

    def submitS3Load = { String tableName, String path, String format, String columns ->
        String label = "${tableName}_${UUID.randomUUID().toString().replace("-", "_")}"
        sql """
            LOAD LABEL ${label} (
                DATA INFILE("s3://${s3BucketName}/${path}/*")
                INTO TABLE ${tableName}
                FORMAT AS "${format}"
                ${columns}
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${ak}",
                "AWS_SECRET_KEY" = "${sk}",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "provider" = "${getS3Provider()}"
            )
            PROPERTIES (
                "timeout" = "600",
                "max_filter_ratio" = "0"
            );
        """

        int maxTryMilliSecs = 600000
        while (maxTryMilliSecs > 0) {
            String[][] result = sql """ SHOW LOAD WHERE LABEL = "${label}" ORDER BY createtime DESC LIMIT 1 """
            logger.info("Load result: ${result}")
            if (result[0][2].equals("FINISHED")) {
                return
            }
            if (result[0][2].equals("CANCELLED")) {
                assertTrue(false, "load failed: ${result}")
            }
            Thread.sleep(1000)
            maxTryMilliSecs -= 1000
        }
        assertTrue(false, "load timeout: ${label}")
    }

    def submitS3LoadAndExpectCancelled = {
            String tableName, String path, String format, String columns, String expectedError ->
        String label = "${tableName}_${UUID.randomUUID().toString().replace("-", "_")}"
        sql """
            LOAD LABEL ${label} (
                DATA INFILE("s3://${s3BucketName}/${path}/*")
                INTO TABLE ${tableName}
                FORMAT AS "${format}"
                ${columns}
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${ak}",
                "AWS_SECRET_KEY" = "${sk}",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "provider" = "${getS3Provider()}"
            )
            PROPERTIES (
                "timeout" = "600",
                "max_filter_ratio" = "0"
            );
        """

        int maxTryMilliSecs = 600000
        while (maxTryMilliSecs > 0) {
            String[][] result = sql """ SHOW LOAD WHERE LABEL = "${label}" ORDER BY createtime DESC LIMIT 1 """
            logger.info("Load result: ${result}")
            if (result[0][2].equals("FINISHED")) {
                assertTrue(false, "load should be cancelled: ${result}")
            }
            if (result[0][2].equals("CANCELLED")) {
                assertTrue(result[0].any { it != null && it.contains(expectedError) },
                        "load error should contain '${expectedError}': ${result}")
                return
            }
            Thread.sleep(1000)
            maxTryMilliSecs -= 1000
        }
        assertTrue(false, "load timeout: ${label}")
    }

    createComplexTable.call(sourceTable)
    sql """
        INSERT INTO ${sourceTable} VALUES
            (1, [1, 2, 3], {'a': 1, 'b': 2}, {1, 'hello'}),
            (2, [4, 5], {'x': 10}, {2, 'world'}),
            (3, [], {}, {3, ''}),
            (4, NULL, NULL, NULL);
    """

    createComplexTable.call(parquetTable)
    createComplexTable.call(orcTable)
    createComplexTable.call(renameParquetTable)
    createComplexTable.call(renameOrcTable)
    createComplexTable.call(scalarParquetTable)
    createComplexTable.call(scalarOrcTable)

    writeComplexFile.call("${s3BasePath}/parquet", "parquet")
    writeComplexFile.call("${s3BasePath}/orc", "orc")
    writeRenamedComplexFile.call("${s3BasePath}/rename_parquet", "parquet")
    writeRenamedComplexFile.call("${s3BasePath}/rename_orc", "orc")
    writeScalarRenameFile.call("${s3BasePath}/scalar_parquet", "parquet")
    writeScalarRenameFile.call("${s3BasePath}/scalar_orc", "orc")

    submitS3Load.call(parquetTable, "${s3BasePath}/parquet", "parquet", "(c_int, c_array, c_map, c_struct)")
    submitS3Load.call(orcTable, "${s3BasePath}/orc", "orc", "(c_int, c_array, c_map, c_struct)")
    submitS3Load.call(renameParquetTable, "${s3BasePath}/rename_parquet", "parquet",
            "(c_int, tmp_array, tmp_map, tmp_struct) SET (c_array = tmp_array, c_map = tmp_map, c_struct = tmp_struct)")
    submitS3Load.call(renameOrcTable, "${s3BasePath}/rename_orc", "orc",
            "(c_int, tmp_array, tmp_map, tmp_struct) SET (c_array = tmp_array, c_map = tmp_map, c_struct = tmp_struct)")
    submitS3LoadAndExpectCancelled.call(renameParquetTable, "${s3BasePath}/rename_parquet", "parquet",
            "(c_int, tmp_array, tmp_map, tmp_struct) SET (c_array = tmp_struct.f1)",
            "Parquet/orc complex type load only supports direct column mapping or rename column mapping")
    submitS3LoadAndExpectCancelled.call(renameOrcTable, "${s3BasePath}/rename_orc", "orc",
            "(c_int, tmp_array, tmp_map, tmp_struct) SET (c_array = tmp_struct.f1)",
            "Parquet/orc complex type load only supports direct column mapping or rename column mapping")
    submitS3LoadAndExpectCancelled.call(scalarParquetTable, "${s3BasePath}/scalar_parquet", "parquet",
            "(c_int, tmp_array) SET (c_array = tmp_array)", "complex column types")
    submitS3LoadAndExpectCancelled.call(scalarOrcTable, "${s3BasePath}/scalar_orc", "orc",
            "(c_int, tmp_array) SET (c_array = tmp_array)", "complex column types")

    qt_parquet_count """ SELECT COUNT(*) FROM ${parquetTable} """
    order_qt_parquet_nested """
        SELECT c_int, c_array[1], c_map['a'], c_struct.f2
        FROM ${parquetTable}
        ORDER BY c_int
    """

    qt_orc_count """ SELECT COUNT(*) FROM ${orcTable} """
    order_qt_orc_nested """
        SELECT c_int, c_array[1], c_map['a'], c_struct.f2
        FROM ${orcTable}
        ORDER BY c_int
    """

    qt_rename_parquet_count """ SELECT COUNT(*) FROM ${renameParquetTable} """
    order_qt_rename_parquet_nested """
        SELECT c_int, c_array[1], c_map['a'], c_struct.f1
        FROM ${renameParquetTable}
        ORDER BY c_int
    """

    qt_rename_orc_count """ SELECT COUNT(*) FROM ${renameOrcTable} """
    order_qt_rename_orc_nested """
        SELECT c_int, c_array[1], c_map['a'], c_struct.f1
        FROM ${renameOrcTable}
        ORDER BY c_int
    """
}
