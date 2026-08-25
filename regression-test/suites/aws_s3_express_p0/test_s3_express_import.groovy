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

suite("test_s3_express_import", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableS3ExpressOneZoneTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return
    }

    def requireConfig = { String key ->
        String value = context.config.otherConfigs.get(key)
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "${key} must be configured when enableS3ExpressOneZoneTest is true")
        }
        return value
    }

    String bucket = requireConfig("s3ExpressBucketName")
    String endpoint = requireConfig("s3ExpressEndpoint")
    String region = requireConfig("s3ExpressRegion")
    String prefix = "fixtures/v1"
    String accessKey = requireConfig("s3ExpressAk")
    String secretKey = requireConfig("s3ExpressSk")

    def bucketMatcher = bucket =~ /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?--([a-z0-9-]+-az[0-9]+)--x-s3$/
    if (!bucketMatcher.matches()) {
        throw new IllegalArgumentException(
                "s3ExpressBucketName must be a complete S3 Express directory bucket name")
    }
    if (!(region ==~ /^[a-z0-9-]+$/)) {
        throw new IllegalArgumentException("s3ExpressRegion contains unsupported characters")
    }
    String expectedEndpoint = "s3express-${bucketMatcher.group(1)}.${region}.amazonaws.com"
    if (endpoint != expectedEndpoint) {
        throw new IllegalArgumentException(
                "s3ExpressEndpoint must match the bucket Zone ID and Region")
    }

    List<String> explicitProperties = [
            '"provider" = "S3EXPRESS"',
            "\"s3.region\" = \"${region}\"",
            "\"s3.access_key\" = \"${accessKey}\"",
            "\"s3.secret_key\" = \"${secretKey}\"",
            '"use_path_style" = "false"'
    ]
    String explicitPropertiesSql = explicitProperties.join(",\n")

    // Existing import/resource metadata may still carry provider=S3 plus a zonal endpoint.
    // Keep this case separate from the explicit provider path so compatibility cannot regress.
    List<String> legacyProperties = [
            '"provider" = "S3"',
            "\"s3.endpoint\" = \"${endpoint}\"",
            "\"s3.region\" = \"${region}\"",
            "\"s3.access_key\" = \"${accessKey}\"",
            "\"s3.secret_key\" = \"${secretKey}\"",
            '"use_path_style" = "false"'
    ]
    String legacyPropertiesSql = legacyProperties.join(",\n")

    order_qt_s3_express_explicit_exact_csv """
        SELECT c1, c2
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/csv/data_1.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${explicitPropertiesSql}
        )
        ORDER BY c1, c2
    """

    order_qt_s3_express_legacy_exact_csv """
        SELECT c1, c2
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/csv/data_1.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${legacyPropertiesSql}
        )
        ORDER BY c1, c2
    """

    order_qt_s3_express_explicit_csv_glob """
        SELECT count(*), sum(c1), sum(c2)
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/csv/data_*.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${explicitPropertiesSql}
        )
    """

    order_qt_s3_express_legacy_csv_glob """
        SELECT count(*), sum(c1), sum(c2)
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/csv/data_*.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${legacyPropertiesSql}
        )
    """

    order_qt_s3_express_orc """
        SELECT count(*)
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/orc/t.orc",
            "format" = "orc",
            ${explicitPropertiesSql}
        )
    """

    order_qt_s3_express_legacy_orc """
        SELECT count(*)
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/orc/t.orc",
            "format" = "orc",
            ${legacyPropertiesSql}
        )
    """

    sql """DROP TABLE IF EXISTS test_s3_express_insert_parquet"""
    sql """
        CREATE TABLE test_s3_express_insert_parquet (
            id INT,
            uint8_column SMALLINT,
            uint16_column INT,
            uint32_column BIGINT,
            uint64_column LARGEINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_s3_express_insert_parquet
        SELECT id, uint8_column, uint16_column, uint32_column, uint64_column
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/parquet/unsigned_integers_1.parquet",
            "format" = "parquet",
            ${explicitPropertiesSql}
        )
    """
    order_qt_s3_express_insert_parquet """
        SELECT count(*), sum(id)
        FROM test_s3_express_insert_parquet
    """

    sql """TRUNCATE TABLE test_s3_express_insert_parquet"""
    sql """
        INSERT INTO test_s3_express_insert_parquet
        SELECT id, uint8_column, uint16_column, uint32_column, uint64_column
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/parquet/unsigned_integers_1.parquet",
            "format" = "parquet",
            ${legacyPropertiesSql}
        )
    """
    order_qt_s3_express_legacy_insert_parquet """
        SELECT count(*), sum(id)
        FROM test_s3_express_insert_parquet
    """

    // The fixture contains 1001 keys, requiring a real ListObjectsV2 continuation token.
    sql """DROP TABLE IF EXISTS test_s3_express_import_csv"""
    sql """
        CREATE TABLE test_s3_express_import_csv (
            c1 INT,
            c2 INT
        )
        DUPLICATE KEY(c1)
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_s3_express_import_csv
        SELECT c1, c2
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/pagination/*.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${explicitPropertiesSql}
        )
    """
    order_qt_s3_express_insert_pagination """
        SELECT count(*), sum(c1), sum(c2)
        FROM test_s3_express_import_csv
    """

    sql """TRUNCATE TABLE test_s3_express_import_csv"""
    sql """
        INSERT INTO test_s3_express_import_csv
        SELECT c1, c2
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/pagination/*.csv",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${legacyPropertiesSql}
        )
    """
    order_qt_s3_express_legacy_insert_pagination """
        SELECT count(*), sum(c1), sum(c2)
        FROM test_s3_express_import_csv
    """

    def waitForLoadFinished = { String label ->
        waitForBrokerLoadDone(label, 300)
        def loadResult = sql """SHOW LOAD WHERE LABEL = '${label}' ORDER BY CreateTime DESC LIMIT 1"""
        if (loadResult.isEmpty() || loadResult[0][2] != "FINISHED") {
            throw new IllegalStateException(
                    "Broker Load ${label} did not finish successfully: ${loadResult}")
        }
    }

    // Broker Load is deprecated, but keep both parameter forms consistent with the TVF coverage.
    sql """TRUNCATE TABLE test_s3_express_import_csv"""
    String explicitLoadLabel = "s3_express_explicit_" + UUID.randomUUID().toString().replace("-", "_")
    sql """
        LOAD LABEL ${explicitLoadLabel} (
            DATA INFILE ("s3://${bucket}/${prefix}/csv/data_*.csv")
            INTO TABLE test_s3_express_import_csv
            COLUMNS TERMINATED BY ","
            FORMAT AS "CSV"
            (c1, c2)
        )
        WITH S3 (
            ${explicitPropertiesSql}
        )
        PROPERTIES ("timeout" = "300")
    """
    waitForLoadFinished(explicitLoadLabel)
    order_qt_s3_express_explicit_broker_load """
        SELECT count(*), sum(c1), sum(c2)
        FROM test_s3_express_import_csv
    """

    sql """TRUNCATE TABLE test_s3_express_import_csv"""
    String legacyLoadLabel = "s3_express_legacy_" + UUID.randomUUID().toString().replace("-", "_")
    sql """
        LOAD LABEL ${legacyLoadLabel} (
            DATA INFILE ("s3://${bucket}/${prefix}/csv/data_*.csv")
            INTO TABLE test_s3_express_import_csv
            COLUMNS TERMINATED BY ","
            FORMAT AS "CSV"
            (c1, c2)
        )
        WITH S3 (
            ${legacyPropertiesSql}
        )
        PROPERTIES ("timeout" = "300")
    """
    waitForLoadFinished(legacyLoadLabel)
    order_qt_s3_express_legacy_broker_load """
        SELECT count(*), sum(c1), sum(c2)
        FROM test_s3_express_import_csv
    """
}
