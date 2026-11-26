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

import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential

suite("map_uniq_with_local_tvf", "p0") {
    def table_name = "map_uniq"
    sql "DROP TABLE IF EXISTS ${table_name};"
    sql """
            CREATE TABLE ${table_name} (
              `id` int(11) NULL,
              `m` MAP<text,text> NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
            ); """

    String bucket = getS3BucketName()
    String key = "regression/local_tvf_migration/${UUID.randomUUID()}/mm.orc"
    String provider = getS3Provider()
    String endpoint = getS3Endpoint()
    File fixture = new File(context.config.dataPath, "types/complex_types/mm.orc")
    def deleteFixture
    if (provider.equalsIgnoreCase("AZURE")) {
        if (endpoint == "azure-endpoint") {
            endpoint = "https://${getS3AK()}.blob.core.windows.net"
        } else if (!endpoint.contains("://")) {
            endpoint = "https://${endpoint}"
        }
        def blobClient = new BlobClientBuilder()
                .endpoint(endpoint)
                .containerName(bucket)
                .blobName(key)
                .credential(new StorageSharedKeyCredential(getS3AK(), getS3SK()))
                .buildClient()
        blobClient.uploadFromFile(fixture.absolutePath)
        deleteFixture = { blobClient.delete() }
    } else {
        def s3Client = getS3Client()
        s3Client.putObject(bucket, key, fixture)
        deleteFixture = { s3Client.deleteObject(bucket, key) }
    }
    try {
        sql """INSERT INTO ${table_name} SELECT id, m FROM S3(
            "uri" = "s3://${bucket}/${key}",
            "format" = "orc",
            "s3.endpoint" = "${endpoint}",
            "provider" = "${provider}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        )"""
    } finally {
        deleteFixture()
    }
    sql "sync"
    qt_sql "SELECT COUNT(*) FROM ${table_name}"
    qt_sql  """ select count(m) from ${table_name}; """
    qt_sql  """ select count(m) from ${table_name} where map_size(m) > 0;"""

}
