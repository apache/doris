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
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder

suite("test_iceberg_missing_data_file",
        "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = "http://${host}:${minioPort}"

    sql "drop catalog if exists test_iceberg_missing_data_file"
    sql """create catalog test_iceberg_missing_data_file properties (
        "type" = "iceberg",
        "iceberg.catalog.type" = "rest",
        "uri" = "http://${host}:${restPort}",
        "s3.endpoint" = "${endpoint}",
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.region" = "us-east-1"
    )"""
    sql "switch test_iceberg_missing_data_file"
    sql "create database if not exists missing_data_file_db"
    sql "use missing_data_file_db"
    sql "drop table if exists missing_data_file"
    sql """create table missing_data_file (id int, payload string)
        properties ("format-version" = "2", "write.format.default" = "parquet")"""
    sql "insert into missing_data_file values (1, 'first-file')"
    sql "insert into missing_data_file values (2, 'second-file')"

    // Inspect manifests only: reading table rows here could cache the object that will be removed.
    def files = sql "select file_path from missing_data_file\$files where content = 0"
    assertTrue(files.size() >= 2, "The snapshot must contain multiple data files")
    URI missingFile = new URI(files[0][0].toString())
    // Hadoop-backed catalogs may expose the same S3 objects through s3a or s3n URIs.
    assertTrue(missingFile.scheme in ["s3", "s3a", "s3n"],
            "Unexpected object storage URI: ${missingFile}")
    String key = missingFile.path.substring(1)
    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
            .withPathStyleAccessEnabled(true)
            .withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials("admin", "password")))
            .build()
    try {
        // Remove only an object created by this suite, leaving the snapshot metadata unchanged.
        client.deleteObject(missingFile.host, key)
        assertFalse(client.doesObjectExist(missingFile.host, key))
    } finally {
        client.shutdown()
    }

    sql "set enable_file_cache = false"
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    for (boolean ignoreMissing : [true, false]) {
        setBeConfigTemporary([ignore_not_found_file_in_external_table: ignoreMissing]) {
            for (boolean scannerV2 : [false, true]) {
                sql "set enable_file_scanner_v2 = ${scannerV2}"
                test {
                    sql "select id, payload from missing_data_file order by id"
                    exception "NOT_FOUND"
                }
            }
        }
    }
}
