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

import com.amazonaws.services.s3.model.ObjectMetadata
import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

suite("test_tvf_explicit_compression_splits", "p0,external") {
    String provider = getS3Provider()
    String endpoint = getS3Endpoint()
    boolean azure = provider.equalsIgnoreCase("AZURE")
    if (azure) {
        endpoint = endpoint == "azure-endpoint" ? "https://${getS3AK()}.blob.core.windows.net" : endpoint
        if (!endpoint.contains("://")) {
            endpoint = "https://${endpoint}"
        }
    }
    String bucket = context.config.otherConfigs.get("s3BucketName")
    String prefix = "tvf_compression_split/${UUID.randomUUID()}/"
    def s3Client = azure ? null : getS3Client()
    def blob = { String key ->
        new BlobClientBuilder().endpoint(endpoint).containerName(bucket).blobName(key)
                .credential(new StorageSharedKeyCredential(getS3AK(), getS3SK())).buildClient()
    }
    List<String> keys = []
    def hdfs = null
    boolean hdfsDirectoryCreated = false
    String hdfsUri = null
    String hdfsUser = context.config.otherConfigs.get("hdfsUser") ?: "hadoop"
    Path hdfsDirectory = new Path("/tmp/${prefix}")
    Map<String, String> original = [:]
    for (String variable : ["file_split_size", "enable_sql_cache", "enable_query_cache"]) {
        original[variable] = sql("SHOW VARIABLES LIKE '${variable}'")[0][1].toString()
    }
    try {
        // The external lane enables Hive's HDFS, while its standalone enableHdfs flag is false.
        if (context.config.otherConfigs.get("enableHiveTest")?.toString()?.equalsIgnoreCase("true")) {
            hdfsUri = "hdfs://${context.config.otherConfigs.get('externalEnvIp')}:" +
                    context.config.otherConfigs.get("hive2HdfsPort")
            Configuration configuration = new Configuration()
            configuration.set("fs.defaultFS", hdfsUri)
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
            hdfs = FileSystem.newInstance(new URI(hdfsUri), configuration, hdfsUser)
            assertTrue(hdfs.mkdirs(hdfsDirectory))
            hdfsDirectoryCreated = true
        }
        sql "SET file_split_size = 1024"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        String expected = """SELECT number AS id, concat('payload_', CAST(number AS STRING), '_汉字') AS payload
                FROM numbers("number"="4096")"""
        for (String format : ["csv", "json", "hive_text"]) {
            StringBuilder contents = new StringBuilder()
            for (int i = 0; i < 4096; ++i) {
                String payload = "payload_${i}_汉字"
                contents.append(format == "json" ? "{\"id\":${i},\"payload\":\"${payload}\"}\n"
                        : "${i},${payload}\n")
            }
            byte[] plain = contents.toString().getBytes(StandardCharsets.UTF_8)
            ByteArrayOutputStream output = new ByteArrayOutputStream()
            new GZIPOutputStream(output).withCloseable { it.write(plain) }
            byte[] gzip = output.toByteArray()
            // A small gzip fixture can accidentally stay below the split threshold and miss the defect.
            assertTrue(gzip.length > 1024)
            def variants = [
                    [name: "gzip_no_suffix", bytes: gzip, suffix: "", compression: "GZ"],
                    [name: "gzip_inferred", bytes: gzip, suffix: ".gz", compression: null],
                    [name: "plain_gzip_suffix", bytes: plain, suffix: ".gz", compression: "PLAIN"],
                    [name: "plain_inferred", bytes: plain, suffix: "", compression: null]]
            String canonical = null
            for (def variant : variants) {
                String key = "${prefix}${format}/${variant.name}${variant.suffix}"
                byte[] bytes = variant.bytes
                keys.add(key)
                if (azure) {
                    new ByteArrayInputStream(bytes).withCloseable {
                        blob(key).upload(it, (long) bytes.length, true)
                    }
                } else {
                    ObjectMetadata metadata = new ObjectMetadata()
                    metadata.setContentLength(bytes.length)
                    new ByteArrayInputStream(bytes).withCloseable { s3Client.putObject(bucket, key, it, metadata) }
                }
                if (hdfs != null) {
                    hdfs.create(new Path(hdfsDirectory, "${format}_${variant.name}${variant.suffix}"), true)
                            .withCloseable { it.write(bytes) }
                }
                String options = format == "json" ? '"read_json_by_line"="true"'
                        : '"column_separator"=",", "csv_schema"="id:int;payload:string"'
                if (variant.compression != null) {
                    options += ", \"compress_type\"=\"${variant.compression}\""
                }
                String source = """S3("uri"="s3://${bucket}/${key}", "format"="${format}", ${options},
                        "provider"="${provider}", "s3.endpoint"="${endpoint}", "s3.region"="${getS3Region()}",
                        "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}")"""
                def sources = [s3: source]
                if (hdfs != null) {
                    sources.hdfs = """HDFS("uri"="${hdfsUri}${hdfsDirectory}/${format}_${variant.name}${variant.suffix}",
                            "hadoop.username"="${hdfsUser}", "format"="${format}", ${options})"""
                }
                sources.each { storage, table ->
                    // Project every row, without LIMIT: COUNT(*) alone may suppress file splitting.
                    quickTest("${storage}_${format}_${variant.name}",
                            "SELECT CAST(id AS BIGINT), payload FROM ${table} ORDER BY 1",
                            "SELECT id, payload FROM (${expected}) expected ORDER BY 1")
                    quickTest("${storage}_${format}_${variant.name}_counts",
                            "SELECT count(*), count(id), sum(CAST(id AS BIGINT)) FROM ${table}",
                            "SELECT count(*), count(id), sum(id) FROM (${expected}) expected")
                }
                if (variant.name == "gzip_inferred") {
                    canonical = source
                }
            }
            qt_count "SELECT count(*), count(id), sum(CAST(id AS BIGINT)) FROM ${canonical}"
        }
    } finally {
        try {
            original.each { variable, value -> sql "SET ${variable} = ${value}" }
        } finally {
            try {
                keys.each { key ->
                    if (azure) {
                        blob(key).deleteIfExists()
                    } else {
                        s3Client.deleteObject(bucket, key)
                    }
                }
            } finally {
                if (s3Client != null) {
                    s3Client.shutdown()
                }
                if (hdfs != null) {
                    try {
                        if (hdfsDirectoryCreated) {
                            assertTrue(hdfs.delete(hdfsDirectory, true))
                        }
                    } finally {
                        hdfs.close()
                    }
                }
            }
        }
    }
}
