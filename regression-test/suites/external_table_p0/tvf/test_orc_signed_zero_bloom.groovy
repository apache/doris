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

suite("test_orc_signed_zero_bloom", "p0,external") {
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
    String prefix = "orc_signed_zero_bloom/${UUID.randomUUID()}"
    Map<String, String> originalSettings = [:]
    for (String variable : ["enable_orc_filter_by_min_max", "enable_orc_lazy_materialization",
                            "enable_sql_cache", "enable_query_cache", "enable_condition_cache"]) {
        originalSettings[variable] = sql("SHOW VARIABLES LIKE '${variable}'")[0][1].toString()
    }
    try {
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        for (String layout : ["row_groups", "stripes"]) {
            String key = "${prefix}/${layout}.orc"
            File fixture = new File(context.dataPath, "test_data/signed_zero_bloom_${layout}.orc")
            def blobClient = null
            def s3Client = null
            boolean uploaded = false
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
                for (boolean sarg : [false, true]) {
                    sql "SET enable_orc_filter_by_min_max = ${sarg}"
                    for (boolean lazy : [false, true]) {
                        sql "SET enable_orc_lazy_materialization = ${lazy}"
                        for (String column : ["f", "d"]) {
                            String type = column == "f" ? "FLOAT" : "DOUBLE"
                            String positive = "CAST('0.0' AS ${type})"
                            String negative = "CAST('-0.0' AS ${type})"
                            // IDs are an independent oracle: negative zeros [0,1000), positive
                            // zeros [1000,2000), alternating 3/1 [2000,3000), NULLs [3000,4000).
                            def cases = [
                                    ["${column} = ${positive}", "id < 2000"],
                                    ["${column} = ${negative}", "id < 2000"],
                                    ["${negative} = ${column}", "id < 2000"],
                                    ["${column} <=> ${positive}", "id < 2000"],
                                    ["${negative} <=> ${column}", "id < 2000"],
                                    ["${column} IN (${negative})", "id < 2000"],
                                    ["${column} IN (${positive}, 2.0)", "id < 2000"],
                                    ["${column} IN (2.0, ${negative})", "id < 2000"],
                                    ["${column} IN (${positive}, ${negative}, NULL)", "id < 2000"],
                                    ["${column} != ${negative}", "id >= 2000 AND id < 3000"],
                                    ["NOT (${column} = ${positive})", "id >= 2000 AND id < 3000"],
                                    ["${column} NOT IN (${negative}, 2.0)", "id >= 2000 AND id < 3000"],
                                    ["${column} NOT IN (${positive}, NULL)", "FALSE"],
                                    ["(${column} = ${negative} AND id >= 1000) OR ${column} = 3.0",
                                     "(id >= 1000 AND id < 2000) OR (id >= 2000 AND id < 3000 AND id % 2 = 0)"],
                                    ["(${column} IN (${positive}, 2.0) OR ${column} IS NULL)",
                                     "id < 2000 OR id >= 3000"],
                                    ["(${column} <=> ${negative}) OR id = 3999", "id < 2000 OR id = 3999"],
                                    ["CAST(${column} AS DOUBLE) = ${positive}", "id < 2000"],
                                    ["${column} = 1.0", "id >= 2000 AND id < 3000 AND id % 2 = 1"],
                                    ["${column} IN (2.0, 2.5)", "FALSE"],
                                    ["${column} <= ${negative}", "id < 2000"],
                                    ["${column} <=> CAST(NULL AS ${type})", "id >= 3000"]
                            ]
                            cases.eachWithIndex { testCase, index ->
                                quickTest("${layout}_${sarg}_${lazy}_${column}_${index}",
                                        "SELECT id FROM ${source} WHERE ${testCase[0]} ORDER BY id",
                                        "SELECT id FROM (SELECT number AS id FROM numbers('number'='4000')) ids " +
                                                "WHERE ${testCase[1]} ORDER BY id")
                            }
                        }
                    }
                }
                order_qt_fixture "SELECT id, f, d FROM ${source} WHERE id IN (0, 1000, 2000, 3000)"
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
    } finally {
        originalSettings.each { variable, value -> sql "SET ${variable} = ${value}" }
    }
}
