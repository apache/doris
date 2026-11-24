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

suite("test_parquet_boolean_dictionary", "p0,external") {
    String provider = getS3Provider()
    String endpoint = getS3Endpoint()
    if (provider.equalsIgnoreCase("AZURE")) {
        if (endpoint == "azure-endpoint") {
            endpoint = "https://${getS3AK()}.blob.core.windows.net"
        } else if (!endpoint.contains("://")) {
            endpoint = "https://${endpoint}"
        }
    }
    String bucket = context.config.otherConfigs.get('s3BucketName')
    String key = "parquet_boolean_dictionary/${UUID.randomUUID()}/values.parquet"
    File fixture = new File(context.dataPath, "test_data/boolean_rle_dictionary.parquet")
    def blobClient = null
    def s3Client = null
    boolean uploaded = false
    Map<String, String> originalSettings = [:]
    // Use the public SQL name: SHOW VARIABLES silently returns no rows for unknown names.
    for (String variable : ['enable_sql_cache', 'enable_query_cache', 'enable_parquet_lazy_materialization']) {
        originalSettings[variable] = sql("SHOW VARIABLES LIKE '${variable}'")[0][1].toString()
    }
    try {
        if (provider.equalsIgnoreCase("AZURE")) {
            blobClient = new BlobClientBuilder()
                    .endpoint(endpoint)
                    .containerName(bucket)
                    .blobName(key)
                    .credential(new StorageSharedKeyCredential(getS3AK(), getS3SK()))
                    .buildClient()
            blobClient.uploadFromFile(fixture.absolutePath)
        } else {
            s3Client = getS3Client()
            s3Client.putObject(bucket, key, fixture)
        }
        uploaded = true
        String source = """S3('uri'='s3://${bucket}/${key}', 'format'='parquet',
                'provider'='${provider}', 's3.endpoint'='${endpoint}', 's3.region'='${getS3Region()}',
                's3.access_key'='${getS3AK()}', 's3.secret_key'='${getS3SK()}')"""
        // OUTFILE writes PLAIN booleans, so use a fixture whose BOOLEAN values require dictionary
        // decoding. Compare with SQL literals to check NULL alignment without hand-written output.
        String expected = """(
                SELECT 0 AS id, CAST(TRUE AS BOOLEAN) AS b
                UNION ALL SELECT 1, FALSE
                UNION ALL SELECT 2, CAST(NULL AS BOOLEAN)
                UNION ALL SELECT 3, TRUE
                UNION ALL SELECT 4, TRUE
                UNION ALL SELECT 5, FALSE
                UNION ALL SELECT 6, CAST(NULL AS BOOLEAN)
                UNION ALL SELECT 7, FALSE
                UNION ALL SELECT 8, TRUE
                UNION ALL SELECT 9, TRUE
            ) expected_boolean_dictionary"""
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        for (boolean lazyMaterialization : [false, true]) {
            sql "SET enable_parquet_lazy_materialization = ${lazyMaterialization}"
            for (String predicate : ['', 'WHERE b = TRUE', 'WHERE b = FALSE', 'WHERE b IS NULL',
                    'WHERE b <> TRUE OR b IS NULL', 'WHERE id IN (1, 2, 5, 6, 9)']) {
                quickTest("boolean_dictionary_rows",
                        "SELECT id, b FROM ${source} ${predicate} ORDER BY id",
                        "SELECT id, b FROM ${expected} ${predicate} ORDER BY id")
            }
            quickTest("boolean_dictionary_aggregates",
                    """SELECT COUNT(*), COUNT(b), SUM(IF(b, 1, 0)), SUM(IF(b IS NULL, 1, 0))
                       FROM ${source}""",
                    """SELECT COUNT(*), COUNT(b), SUM(IF(b, 1, 0)), SUM(IF(b IS NULL, 1, 0))
                       FROM ${expected}""")
        }
    } finally {
        try {
            originalSettings.each { variable, value -> sql "SET ${variable} = ${value}" }
        } finally {
            if (uploaded) {
                if (blobClient != null) {
                    blobClient.delete()
                } else {
                    s3Client.deleteObject(bucket, key)
                }
            }
        }
    }
}
