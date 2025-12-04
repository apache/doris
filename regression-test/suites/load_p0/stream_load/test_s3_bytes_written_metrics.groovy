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

suite("test_s3_bytes_written_metrics", "p0") {
    if (!context.config.isCloudMode()) {
        return
    }

    def tableName = "test_s3_bytes_written_metrics"
    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            def ret = matcher[0][1] as long
            logger.info("getBrpcMetrics, ${url}, name:${name}, value:${ret}")
            return ret
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }

    def getTotalS3BytesWritten = {
        def backends = sql """SHOW BACKENDS"""
        def totalBytes = 0L
        for (def backend : backends) {
            def ip = backend[1]
            def httpPort = backend[5]
            def bytes = getBrpcMetrics(ip, httpPort, "s3_file_writer_bytes_written")
            totalBytes += bytes
        }
        return totalBytes
    }

    def initialBytes = getTotalS3BytesWritten()
    logger.info("before load s3_file_writer_bytes_written: ${initialBytes}")

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` string NULL,
            `v1` date  NULL,
            `v2` string  NULL,
            `v3` datetime  NULL,
            `v4` string  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','

        file "empty_field_as_null.csv"
    }

    def afterLoadBytes = getTotalS3BytesWritten()
    def loadBytes = afterLoadBytes - initialBytes
    logger.info("after load s3_file_writer_bytes_written: ${afterLoadBytes}, written: ${loadBytes}")
    assertTrue(loadBytes > 0, "s3_file_writer_bytes_written should increase")
}

