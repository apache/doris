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

suite("test_result_file_name_padding", "p0,external") {
    def bucket = context.config.otherConfigs.get("s3BucketName")
    def prefix = "regression/result_file_name_padding/${UUID.randomUUID()}/"
    def client = getS3Client()
    def storage = """
        "s3.endpoint" = "${getS3Endpoint()}",
        "s3.region" = "${getS3Region()}",
        "s3.access_key" = "${getS3AK()}",
        "s3.secret_key" = "${getS3SK()}"
    """
    def listKeys = { String path ->
        def keys = []
        def listing = client.listObjects(bucket, path)
        while (true) {
            keys.addAll(listing.objectSummaries.collect { it.key })
            if (!listing.isTruncated()) {
                return keys.sort()
            }
            listing = client.listNextBatchOfObjects(listing)
        }
    }
    def writeQuery = { String writer, String path, String format, String extra, String query ->
        if (writer == "outfile") {
            return """${query} INTO OUTFILE "s3://${bucket}/${path}" FORMAT AS ${format}
                PROPERTIES (${storage} ${extra})"""
        } else {
            return """INSERT INTO s3("file_path" = "s3://${bucket}/${path}",
                "format" = "${format}", ${storage} ${extra}) ${query}"""
        }
    }
    def readFiles = { String path, String format, String projection ->
        sql """SELECT ${projection} FROM s3("uri" = "s3://${bucket}/${path}",
            "format" = "${format}", ${storage})"""
    }

    sql "set parallel_pipeline_task_num = 1"
    sql "set enable_parallel_outfile = false"
    sql "set batch_size = 512"
    try {
        ["outfile", "tvf"].each { writer ->
            ["csv", "parquet", "orc"].each { format ->
                [null, 0, 1, 5, 10].each { padding ->
                    def path = "${prefix}${writer}_${format}_${padding}/part_"
                    def extra = padding == null ? "" : ", \"file_name_padding\" = \"${padding}\""
                    sql writeQuery(writer, path, format, extra, "SELECT 7 AS id")
                    def keys = listKeys(path)
                    assertEquals(1, keys.size())
                    def index = "0".padLeft(padding == null ? 0 : padding, '0')
                    assertTrue(keys[0].endsWith("_${index}.${format}"), keys[0])
                    assertEquals("7", readFiles(keys[0], format, "*")[0][0].toString())
                }
            }
            ["-1", "11", "abc", "2147483648"].each { padding ->
                test {
                    sql writeQuery(writer, "${prefix}invalid/part_", "csv",
                        ", \"file_name_padding\" = \"${padding}\"", "SELECT 7 AS id")
                    exception "file_name_padding"
                }
            }
        }

        def suffixPath = "${prefix}suffix/part_"
        sql writeQuery("outfile", suffixPath, "csv",
            ', "file_name_padding" = "5", "file_suffix" = "txt"', "SELECT 7 AS id")
        def suffixKeys = listKeys(suffixPath)
        assertEquals(1, suffixKeys.size())
        assertTrue(suffixKeys[0].endsWith("_00000.txt"))
        assertEquals("7", readFiles(suffixKeys[0], "csv", "*")[0][0].toString())

        // Cross index 9 using real file rotation: width is a minimum, never a truncation limit.
        ["outfile": 1, "tvf": 5].each { writer, padding ->
            def path = "${prefix}${writer}_rotation/part_"
            def maxFileSize = writer == "outfile" ? "5MB" : "5242880"
            sql writeQuery(writer, path, "csv",
                ", \"file_name_padding\" = \"${padding}\", \"max_file_size\" = \"${maxFileSize}\"",
                "SELECT number, repeat('x', 4096) FROM numbers(\"number\" = \"18000\") ORDER BY number")
            def keys = listKeys(path)
            assertTrue(keys.size() > 10, "Expected rotation past index 9: ${keys}")
            def indices = keys.collect { key ->
                def digits = key.substring(key.lastIndexOf('_') + 1, key.lastIndexOf('.'))
                def index = digits.toInteger()
                assertEquals(index.toString().padLeft(padding, '0'), digits)
                index
            }
            assertEquals((0..<keys.size()).toList(), indices.sort(false))
            if (padding == 5) {
                assertEquals(indices.sort(false), indices)
            }
            assertEquals("18000", readFiles("${path}*", "csv", "count(*)")[0][0].toString())
        }
    } finally {
        listKeys(prefix).each { client.deleteObject(bucket, it) }
    }
}
