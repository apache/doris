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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Arrays
import java.util.UUID

suite("test_iceberg_encryption_native_reader", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = "http://${host}:${port}"
    String bucket = "doris-pme-${UUID.randomUUID().toString().replace('-', '')}"
    String prefix = "native-pme-${UUID.randomUUID()}"
    String plainKey = "${prefix}/mixed/00-plain.parquet"
    String encryptedKey = "${prefix}/mixed/10-encrypted.parquet"
    String truncatedKey = "${prefix}/invalid/truncated.parquet"
    File fixtures = new File(context.config.dataPath, "external_table_p0/iceberg/encryption")
    File encrypted = new File(fixtures, "iceberg_java.parquet")
    File plain = new File(fixtures, "iceberg_java_plain.parquet")
    assertEquals("PARE", new String(Arrays.copyOf(encrypted.bytes, 4), StandardCharsets.US_ASCII))
    assertEquals("PAR1", new String(Arrays.copyOf(plain.bytes, 4), StandardCharsets.US_ASCII))
    def previous = sql """SELECT @@enable_file_scanner_v2,
        @@enable_count_push_down_for_external_table, @@enable_parquet_file_page_cache"""
    def client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("admin", "password")))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
            .withPathStyleAccessEnabled(true).build()
    File truncated = Files.createTempFile("parquet-encryption-truncated-", ".parquet").toFile()
    def tvf = { String key -> """S3(
        "uri" = "s3://${bucket}/${key}", "format" = "parquet",
        "s3.endpoint" = "${endpoint}", "s3.region" = "us-east-1",
        "s3.access_key" = "admin", "s3.secret_key" = "password", "use_path_style" = "true")""" }
    def rows = """SELECT id, label, items, struct_element(`record`, 'code') AS code
        FROM ${tvf(plainKey)} ORDER BY id"""
    boolean bucketCreated = false
    try {
        client.createBucket(bucket)
        bucketCreated = true
        client.putObject(bucket, plainKey, plain)
        client.putObject(bucket, encryptedKey, encrypted)
        byte[] encryptedBytes = encrypted.bytes
        truncated.bytes = Arrays.copyOf(encryptedBytes, encryptedBytes.length - 1)
        client.putObject(bucket, truncatedKey, truncated)
        sql "SET enable_file_scanner_v2=true"
        order_qt_plain_before rows
        qt_plain_aggregate "SELECT COUNT(*), COUNT(label), SUM(id) FROM ${tvf(plainKey)}"
        for (String cache : ["false", "true"]) {
            sql "SET enable_parquet_file_page_cache=${cache}"
            for (String pushdown : ["false", "true"]) {
                sql "SET enable_count_push_down_for_external_table=${pushdown}"
                // Schema inference must reject a real encrypted footer without a file key.
                test {
                    sql "DESC FUNCTION ${tvf(encryptedKey)}"
                    exception "File encryption [MISSING_KEY]"
                }
                for (int attempt = 0; attempt < 2; attempt++) {
                    // The first file is plaintext, so schema discovery can succeed. The actual scan
                    // and metadata COUNT must still visit the later encrypted file and reject it.
                    test {
                        sql "SELECT id FROM ${tvf(prefix + '/mixed/*.parquet')} ORDER BY id"
                        exception "File encryption [MISSING_KEY]"
                    }
                    test {
                        sql "SELECT COUNT(*) FROM ${tvf(prefix + '/mixed/*.parquet')}"
                        exception "File encryption [MISSING_KEY]"
                    }
                }
                order_qt_plain_after_rejection rows
            }
        }
        test {
            sql "SELECT * FROM ${tvf(truncatedKey)}"
            exception "Invalid Parquet v2 footer magic"
        }
        qt_plain_after_truncation "SELECT COUNT(*), COUNT(label), SUM(id) FROM ${tvf(plainKey)}"
    } finally {
        try {
            sql "SET enable_file_scanner_v2=${previous[0][0]}"
            sql "SET enable_count_push_down_for_external_table=${previous[0][1]}"
            sql "SET enable_parquet_file_page_cache=${previous[0][2]}"
        } finally {
            try {
                // A failed bucket creation has nothing to clean up. Once created, this UUID bucket
                // belongs only to this attempt, including any object from an interrupted upload.
                if (bucketCreated) {
                    for (String key : [plainKey, encryptedKey, truncatedKey]) {
                        client.deleteObject(bucket, key)
                    }
                    client.deleteBucket(bucket)
                }
            } finally {
                client.shutdown()
                truncated.delete()
            }
        }
    }
}
