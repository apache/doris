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
import org.apache.doris.regression.action.ProfileAction

suite("test_orc_condition_cache_seek", "p0,external") {
    String provider = getS3Provider()
    String endpoint = getS3Endpoint()
    if (provider.equalsIgnoreCase("AZURE")) {
        if (endpoint == "azure-endpoint") {
            endpoint = "https://${getS3AK()}.blob.core.windows.net"
        } else if (!endpoint.contains("://")) {
            endpoint = "https://${endpoint}"
        }
    }
    String bucket = context.config.otherConfigs.get("s3BucketName")
    String key = "orc_condition_cache_seek/${UUID.randomUUID()}/values.orc"
    File fixture = new File(context.dataPath, "test_data/condition_cache_seek.orc")
    def blobClient = null
    def s3Client = null
    boolean uploaded = false
    Map<String, String> originalSettings = [:]
    for (String variable : ["enable_condition_cache", "enable_orc_lazy_materialization",
                            "enable_orc_filter_by_min_max", "batch_size", "enable_sql_cache",
                            "enable_query_cache", "enable_profile", "profile_level", "parallel_pipeline_task_num",
                            "max_file_scanners_concurrency"]) {
        originalSettings[variable] = sql("SHOW VARIABLES LIKE '${variable}'")[0][1].toString()
    }
    try {
        if (provider.equalsIgnoreCase("AZURE")) {
            blobClient = new BlobClientBuilder().endpoint(endpoint).containerName(bucket)
                    .blobName(key).credential(new StorageSharedKeyCredential(getS3AK(), getS3SK()))
                    .buildClient()
            blobClient.uploadFromFile(fixture.absolutePath)
        } else {
            s3Client = getS3Client()
            s3Client.putObject(bucket, key, fixture)
        }
        uploaded = true
        String source = """S3('uri'='s3://${bucket}/${key}', 'format'='orc',
                'provider'='${provider}', 's3.endpoint'='${endpoint}', 's3.region'='${getS3Region()}',
                's3.access_key'='${getS3AK()}', 's3.secret_key'='${getS3SK()}')"""
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET max_file_scanners_concurrency = 1"
        sql "SET batch_size = 2048"
        // A full LRU-K shard needs one scan for ghost history and another for admission.
        // Allow a third visit to one backend even when scans are spread across all backends.
        int attempts = 2 * sql("SHOW BACKENDS").size() + 1
        for (boolean minMax : [false, true]) {
            sql "SET enable_orc_filter_by_min_max = ${minMax}"
            for (boolean lazy : [false, true]) {
                sql "SET enable_orc_lazy_materialization = ${lazy}"
                sql "SET enable_condition_cache = false"
                quickTest("uncached", "SELECT id, payload FROM ${source} WHERE id > 5000 ORDER BY id",
                        "SELECT 10000, CAST(0 AS BIGINT) UNION ALL SELECT 10001, CAST(20000 AS BIGINT) ORDER BY 1")
                sql "SET enable_condition_cache = true"
                boolean cacheHit = false
                for (int attempt = 0; attempt < attempts && !cacheHit; ++attempt) {
                    String token = "orc_seek_${UUID.randomUUID().toString().replace('-', '')}"
                    // The first match populates granule 0. The next stripe's match populates
                    // granule 9, whose start (18432) lies in the first stripe's pruned suffix.
                    quickTest("cached",
                            "SELECT id, payload FROM (SELECT id, payload, '${token}' AS marker " +
                                    "FROM ${source} WHERE id > 5000) t ORDER BY id",
                            "SELECT 10000, CAST(0 AS BIGINT) UNION ALL " +
                                    "SELECT 10001, CAST(20000 AS BIGINT) ORDER BY 1")
                    String profile = new ProfileAction(context).getProfileBySql(token,
                            ["ConditionCacheHit", "ConditionCacheFilteredRows"])
                    cacheHit = (profile =~ /(?m)ConditionCacheHit:\s*[1-9]/).find()
                    if (cacheHit) {
                        assertTrue((profile =~ /(?m)ConditionCacheFilteredRows:\s*[1-9]/).find(), profile)
                    }
                }
                assertTrue(cacheHit, "No condition cache hit after ${attempts} scans")
            }
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
