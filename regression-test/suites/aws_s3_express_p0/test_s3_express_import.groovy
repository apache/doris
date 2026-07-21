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
            throw new IllegalArgumentException("${key} must be configured when enableS3ExpressOneZoneTest is true")
        }
        return value
    }

    String bucket = requireConfig("s3ExpressBucket")
    String region = requireConfig("s3ExpressRegion")
    String prefix = requireConfig("s3ExpressPrefix")
    String credentialsProviderType = requireConfig("s3ExpressCredentialsProviderType")

    def bucketMatcher = bucket =~ /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?--([a-z0-9-]+-az[0-9]+)--x-s3$/
    if (!bucketMatcher.matches()) {
        throw new IllegalArgumentException("s3ExpressBucket must be a complete S3 Express directory bucket name")
    }
    if (!(region ==~ /^[a-z0-9-]+$/)) {
        throw new IllegalArgumentException("s3ExpressRegion contains unsupported characters")
    }
    if (!(prefix ==~ '[A-Za-z0-9._/-]+') || prefix.startsWith("/") || prefix.endsWith("/")) {
        throw new IllegalArgumentException(
                "s3ExpressPrefix must be a non-empty key prefix without leading or trailing slash")
    }

    String zoneId = bucketMatcher.group(1)
    String endpoint = "s3express-${zoneId}.${region}.amazonaws.com"
    def objectUrl = { String key ->
        return "https://${bucket}.${endpoint}/${key}"
    }

    List<String> authProperties = [
            '"s3.provider" = "AWS"',
            "\"s3.credentials_provider_type\" = \"${credentialsProviderType}\""
    ]
    String authPropertiesSql = authProperties.join(",\n")
    String regionalPropertiesSql = (authProperties + ["\"s3.region\" = \"${region}\""]).join(",\n")

    // A canonical Object URL does not need s3.region in S3 TVF properties.
    qt_s3_express_object_url_exact_csv """
        SELECT c1, c2
        FROM S3(
            "uri" = "${objectUrl("${prefix}/csv/data_1.csv")}",
            "format" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:int",
            ${authPropertiesSql}
        )
        ORDER BY c1, c2
    """

    // ORC reads exercise ranged GetObject requests against the Express client.
    qt_s3_express_orc """
        SELECT count(*)
        FROM S3(
            "uri" = "s3://${bucket}/${prefix}/orc/t.orc",
            "format" = "orc",
            ${regionalPropertiesSql}
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
            ${regionalPropertiesSql}
        )
    """
    qt_s3_express_insert_parquet """
        SELECT count(*), sum(id)
        FROM test_s3_express_insert_parquet
    """

    // The fixture contains 1001 keys so this requires a real ListObjectsV2 continuation token.
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
            ${regionalPropertiesSql}
        )
    """
    qt_s3_express_insert_pagination """
        SELECT count(*), sum(c1), sum(c2)
        FROM test_s3_express_import_csv
    """

    def waitForLoadFinished = { String label ->
        waitForBrokerLoadDone(label, 300)
        def loadResult = sql """SHOW LOAD WHERE LABEL = '${label}' ORDER BY CreateTime DESC LIMIT 1"""
        if (loadResult.isEmpty() || loadResult[0][2] != "FINISHED") {
            throw new IllegalStateException("Broker Load ${label} did not finish successfully: ${loadResult}")
        }
    }

    sql """TRUNCATE TABLE test_s3_express_import_csv"""
    String objectUrlLabel = "s3_express_object_urls_" + UUID.randomUUID().toString().replace("-", "_")
    sql """
        LOAD LABEL ${objectUrlLabel} (
            DATA INFILE (
                "${objectUrl("${prefix}/csv/data_1.csv")}",
                "${objectUrl("${prefix}/csv/data_2.csv")}"
            )
            INTO TABLE test_s3_express_import_csv
            COLUMNS TERMINATED BY ","
            FORMAT AS "CSV"
            (c1, c2)
        )
        WITH S3 (
            ${regionalPropertiesSql}
        )
        PROPERTIES ("timeout" = "300")
    """
    waitForLoadFinished(objectUrlLabel)
    order_qt_s3_express_broker_object_urls """
        SELECT c1, c2
        FROM test_s3_express_import_csv
        ORDER BY c1, c2
    """

    sql """TRUNCATE TABLE test_s3_express_import_csv"""
    String globLabel = "s3_express_glob_" + UUID.randomUUID().toString().replace("-", "_")
    sql """
        LOAD LABEL ${globLabel} (
            DATA INFILE ("s3://${bucket}/${prefix}/csv/data_*.csv")
            INTO TABLE test_s3_express_import_csv
            COLUMNS TERMINATED BY ","
            FORMAT AS "CSV"
            (c1, c2)
        )
        WITH S3 (
            ${regionalPropertiesSql}
        )
        PROPERTIES ("timeout" = "300")
    """
    waitForLoadFinished(globLabel)
    qt_s3_express_broker_glob """
        SELECT count(*), sum(c1), sum(c2)
        FROM test_s3_express_import_csv
    """
}
