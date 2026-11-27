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
        def writeFixture = { String key, byte[] bytes, String hdfsName ->
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
                hdfs.create(new Path(hdfsDirectory, hdfsName), true).withCloseable { it.write(bytes) }
            }
        }
        def fixtureSources = { String key, String format, String options, String hdfsName ->
            String source = """S3("uri"="s3://${bucket}/${key}", "format"="${format}", ${options},
                    "provider"="${provider}", "s3.endpoint"="${endpoint}", "s3.region"="${getS3Region()}",
                    "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}")"""
            def sources = [s3: source]
            if (hdfs != null) {
                sources.hdfs = """HDFS("uri"="${hdfsUri}${hdfsDirectory}/${hdfsName}",
                        "hadoop.username"="${hdfsUser}", "format"="${format}", ${options})"""
            }
            return sources
        }
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
                String hdfsName = "${format}_${variant.name}${variant.suffix}"
                writeFixture(key, bytes, hdfsName)
                String options = format == "json" ? '"read_json_by_line"="true"'
                        : '"column_separator"=",", "csv_schema"="id:int;payload:string"'
                if (variant.compression != null) {
                    options += ", \"compress_type\"=\"${variant.compression}\""
                }
                def sources = fixtureSources(key, format, options, hdfsName)
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
                    canonical = sources.s3
                }
            }
            qt_count "SELECT count(*), count(id), sum(CAST(id AS BIGINT)) FROM ${canonical}"
        }

        String csvOptions = '"column_separator"=",", "csv_schema"="id:int;payload:string"'
        String stringCsvOptions = '"column_separator"=",", "csv_schema"="id:string;payload:string"'
        String quotedPayload = "a" * 1100 + "\n1,phantom\n2,tail"
        String skippedPrefix = (0..<20).collect { "${1000 + it}," + "p" * 58 + "\n" }.join("")
        String longHeader = "n" * 1300 + ",payload\n"
        String singlePrefix = "1000," + "p" * 1400 + "\n"
        def documentRows = (0..<128).collect { [id: it, payload: "doc_${it}_界".toString()] }
        String document = groovy.json.JsonOutput.toJson(documentRows)
        String singleDocument = groovy.json.JsonOutput.toJson([id: 7, payload: "d" * 2048])
        String lineArrays = documentRows.collect { groovy.json.JsonOutput.toJson([it]) + "\n" }.join("")
        String documentExpected = """SELECT number AS id, concat('doc_',CAST(number AS STRING),'_界') AS payload
                FROM numbers("number"="128")"""
        def modes = [
                [name: "quoted_csv", format: "csv", contents: "0,\"${quotedPayload}\"\n3,real\n",
                 options: csvOptions + ', "enclose"=\'"\'',
                 expected: """SELECT 0 AS id, concat(repeat('a',1100),char(10),'1,phantom',char(10),'2,tail') AS payload
                         UNION ALL SELECT 3,'real'"""],
                [name: "json_array_explicit", format: "json", contents: document,
                 options: '"read_json_by_line"="false", "strip_outer_array"="true"', expected: documentExpected],
                [name: "json_array_implicit", format: "json", contents: document,
                 options: '"strip_outer_array"="true"', expected: documentExpected],
                [name: "json_document_explicit", format: "json", contents: singleDocument,
                 options: '"read_json_by_line"="false"', expected: "SELECT 7 AS id,repeat('d',2048) AS payload"],
                [name: "json_document_implicit", format: "json", contents: singleDocument,
                 options: '"strip_outer_array"="false"', expected: "SELECT 7 AS id,repeat('d',2048) AS payload"],
                [name: "json_line_arrays", format: "json", contents: lineArrays,
                 options: '"read_json_by_line"="true", "strip_outer_array"="true"', expected: documentExpected],
                [name: "csv_prefix", format: "csv", contents: skippedPrefix + "7,real\n",
                 options: csvOptions + ', "skip_lines"="20"', expected: "SELECT 7 AS id,'real' AS payload"],
                [name: "text_prefix", format: "hive_text", contents: skippedPrefix + "7,real\n",
                 options: csvOptions + ', "skip_lines"="20"', expected: "SELECT 7 AS id,'real' AS payload"],
                [name: "csv_type_header", format: "csv_with_names_and_types",
                 contents: longHeader + "string,string\n7,real\n", options: stringCsvOptions,
                 expected: "SELECT 7 AS id,'real' AS payload"],
                [name: "csv_name_header", format: "csv_with_names", contents: longHeader + "7,real\n",
                 options: stringCsvOptions + ', "skip_lines"="20"', expected: "SELECT 7 AS id,'real' AS payload"],
                [name: "csv_single_prefix", format: "csv", contents: singlePrefix + "7,real\n",
                 options: csvOptions + ', "skip_lines"="1"', expected: "SELECT 7 AS id,'real' AS payload"],
                [name: "text_single_prefix", format: "hive_text", contents: singlePrefix + "7,real\n",
                 options: csvOptions + ', "skip_lines"="1"', expected: "SELECT 7 AS id,'real' AS payload"]]
        for (def mode : modes) {
            for (String suffix : ["", ".gz"]) {
                String tag = "${mode.name}_${suffix.isEmpty() ? 'no_suffix' : 'gzip_suffix'}"
                String key = "${prefix}modes/${mode.name}${suffix}"
                byte[] bytes = mode.contents.getBytes(StandardCharsets.UTF_8)
                assertTrue(bytes.length > 1126, "Fixture must produce multiple 1-KiB ranges before the fix")
                writeFixture(key, bytes, tag)
                def sources = fixtureSources(key, mode.format, mode.options + ', "compress_type"="PLAIN"', tag)
                sources.each { storage, table ->
                    // Full rows detect phantom quoted/preamble records; COUNT(*) may suppress splitting.
                    quickTest("${storage}_${tag}",
                            "SELECT CAST(id AS BIGINT),payload FROM ${table} ORDER BY 1",
                            "SELECT id,payload FROM (${mode.expected}) expected ORDER BY 1")
                }
                "qt_${tag}" "SELECT count(*),sum(CAST(id AS BIGINT)) FROM ${sources.s3}"
            }
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
