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

suite("test_export_single_file_url", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()

    def waitForExport = { label ->
        for (int retry = 0; retry < 120; ++retry) {
            def result = sql """SHOW EXPORT WHERE LABEL = '${label}'"""
            if (result[0][2] == "FINISHED") {
                return parseJson(result[0][11]).flatten()
            }
            if (result[0][2] == "CANCELLED") {
                throw new IllegalStateException("Export failed: ${result[0][10]}")
            }
            sleep(1000)
        }
        throw new IllegalStateException("Export did not finish: ${label}")
    }

    sql "DROP TABLE IF EXISTS test_export_single_file_url"
    sql """CREATE TABLE test_export_single_file_url (id INT, payload STRING)
           DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO test_export_single_file_url VALUES (1, 'one'), (2, 'two'), (3, 'three')"
    def cases = [
        ["csv", "plain", "csv"],
        ["csv", "gz", "csv.gz"],
        ["csv", "bz2", "csv.bz2"],
        ["csv", "snappyblock", "csv.snappy"],
        ["csv", "lz4block", "csv.lz4"],
        ["csv", "zstd", "csv.zstd"],
        ["csv_with_names", "gz", "csv.gz"],
        ["csv_with_names_and_types", "gz", "csv.gz"],
        ["parquet", "snappy", "parquet"],
        ["orc", "zlib", "orc"]
    ]
    cases.each { format, compression, suffix ->
        def label = "single_file_${UUID.randomUUID().toString().replace('-', '')}"
        def prefix = "s3://${bucket}/export/test_export_single_file_url/${label}/part_"
        sql """EXPORT TABLE test_export_single_file_url TO "${prefix}"
               PROPERTIES("label" = "${label}", "format" = "${format}",
                          "compress_type" = "${compression}", "parallelism" = "1")
               WITH S3("s3.endpoint" = "${endpoint}", "s3.region" = "${region}",
                       "s3.access_key" = "${ak}", "s3.secret_key" = "${sk}",
                       "provider" = "${getS3Provider()}")"""
        def infos = waitForExport(label)
        def info = infos[0]
        // The rest of the name is the fragment ID and zero-based file number.
        def filename = info.url.substring(prefix.length())
        order_qt_url_metadata """SELECT '${format}', '${compression}',
                ${infos.size()}, '${info.fileNumber}', ${info.url.contains("*")},
                ${info.url.startsWith(prefix)}, ${info.url.endsWith("_0.${suffix}")},
                ${filename ==~ /[0-9a-f]+-[0-9a-f]+_0\..+/}"""
        def readCompression = format.startsWith("csv")
                ? '"compress_type" = "' + compression + '",' : ""
        order_qt_read_file """SELECT '${format}', '${compression}', COUNT(*) FROM S3(
                "uri" = "${info.url}", "format" = "${format}",
                ${readCompression}
                "s3.endpoint" = "${endpoint}", "s3.region" = "${region}",
                "s3.access_key" = "${ak}", "s3.secret_key" = "${sk}",
                "provider" = "${getS3Provider()}")"""
    }
}
