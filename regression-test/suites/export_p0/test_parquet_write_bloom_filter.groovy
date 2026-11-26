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

import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.common.StorageSharedKeyCredential
import org.apache.parquet.format.BloomFilterHeader
import org.apache.parquet.format.Encoding
import org.apache.parquet.format.FileMetaData
import org.apache.parquet.format.PageType
import org.apache.parquet.format.Type
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.TMemoryInputTransport
import java.nio.ByteBuffer
import java.nio.ByteOrder

suite("test_parquet_write_bloom_filter", "p0,external") {
    String provider = getS3Provider()
    String endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName")
    String prefix = "regression/parquet_write_bloom/${UUID.randomUUID()}/"
    boolean azure = provider.equalsIgnoreCase("AZURE")
    def container = null
    def client = null
    if (azure) {
        endpoint = endpoint == "azure-endpoint" ? "https://${getS3AK()}.blob.core.windows.net" : endpoint
        if (!endpoint.contains("://")) {
            endpoint = "https://${endpoint}"
        }
        container = new BlobServiceClientBuilder().endpoint(endpoint)
                .credential(new StorageSharedKeyCredential(getS3AK(), getS3SK()))
                .buildClient().getBlobContainerClient(bucket)
    } else {
        client = getS3Client()
    }
    def listKeys = { String path ->
        if (azure) {
            return container.listBlobs(new ListBlobsOptions().setPrefix(path), null)
                    .collect { it.name }.sort()
        }
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
    def download = { String key ->
        if (azure) {
            return container.getBlobClient(key).downloadContent().toBytes()
        }
        def object = client.getObject(bucket, key)
        try {
            return object.objectContent.bytes
        } finally {
            object.close()
        }
    }
    String storage = """
            "provider"="${provider}", "s3.endpoint"="${endpoint}",
            "s3.region"="${getS3Region()}", "s3.access_key"="${getS3AK()}",
            "s3.secret_key"="${getS3SK()}"
            """
    def inspect = { byte[] bytes, boolean enabled, String mode ->
        assertEquals("PAR1", new String(bytes, bytes.length - 4, 4, "US-ASCII"))
        int footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
                .order(ByteOrder.LITTLE_ENDIAN).getInt()
        int footerOffset = bytes.length - 8 - footerLength
        assertTrue(footerOffset >= 4)
        def metadata = new FileMetaData()
        metadata.read(new TCompactProtocol(new TMemoryInputTransport(bytes, footerOffset, footerLength)))
        assertTrue(!metadata.row_groups.isEmpty())
        metadata.row_groups.each { group ->
            group.columns.each { chunk ->
                def column = chunk.meta_data
                boolean expected = enabled && column.type != Type.BOOLEAN && column.type != Type.INT96
                assertEquals(expected, column.isSetBloomFilterOffset())
                assertEquals(expected, column.isSetBloomFilterLength())
                if (expected) {
                    int offset = Math.toIntExact(column.bloom_filter_offset)
                    int length = column.bloom_filter_length
                    assertTrue(offset >= 4 && length > 32 && (long) offset + length <= footerOffset)
                    def transport = new TMemoryInputTransport(bytes, offset, length)
                    def header = new BloomFilterHeader()
                    header.read(new TCompactProtocol(transport))
                    assertTrue(header.algorithm.isSetBLOCK())
                    assertTrue(header.hash.isSetXXHASH())
                    assertTrue(header.compression.isSetUNCOMPRESSED())
                    assertTrue(header.numBytes >= 32 && header.numBytes % 32 == 0)
                    assertEquals(header.numBytes, transport.getBytesRemainingInBuffer())
                }
                if (column.path_in_schema == ["k"]) {
                    if (mode == "overflow") {
                        assertTrue(column.encoding_stats.any {
                            it.page_type in [PageType.DATA_PAGE, PageType.DATA_PAGE_V2] &&
                                    it.encoding == Encoding.PLAIN && it.count > 0
                        }, "The high-cardinality string must actually fall back from dictionary encoding")
                    } else {
                        assertEquals(mode == "retained", column.encoding_stats.any {
                            it.page_type in [PageType.DATA_PAGE, PageType.DATA_PAGE_V2] &&
                                    it.encoding in [Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY] && it.count > 0
                        })
                    }
                }
            }
        }
    }
    Map<String, String> original = [:]
    for (String name : ["enable_parquet_write_bloom_filter", "enable_parquet_filter_by_bloom_filter",
                        "enable_profile", "enable_condition_cache", "enable_sql_cache", "enable_query_cache",
                        "parallel_pipeline_task_num", "enable_parallel_outfile"]) {
        original[name] = sql("SHOW VARIABLES LIKE '${name}'")[0][1].toString()
    }
    try {
        sql "SET enable_profile = true"
        sql "SET enable_condition_cache = false"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET enable_parallel_outfile = false"
        for (boolean enabled : [false, true]) {
            sql "SET enable_parquet_write_bloom_filter = ${enabled}"
            qt_switch "SHOW VARIABLES LIKE 'enable_parquet_write_bloom_filter'"
            for (String writer : ["outfile", "tvf"]) {
                def modes = writer == "outfile" ? ["retained", "overflow", "disabled"] : ["retained", "overflow"]
                for (String mode : modes) {
                    int rows = mode == "overflow" ? 40000 : 2048
                    String keyExpression = mode == "retained" ? "number % 8" : "number"
                    String source = """SELECT CAST(number * 2 AS INT) AS id,
                            CASE WHEN number % 29 = 0 THEN CAST(NULL AS STRING)
                                 ELSE md5(CAST(${keyExpression} AS STRING)) END AS k,
                            CAST('-0.0' AS FLOAT) AS f, CAST('-0.0' AS DOUBLE) AS d,
                            number % 2 = 0 AS flag, CAST(NULL AS STRING) AS all_null,
                            array(CAST(number AS INT), CAST(NULL AS INT)) AS items
                            FROM numbers("number"="${rows}")"""
                    // Keep signed-zero literal folding out of the oracle; both signs must match in the reader.
                    String expectedSource = source.replace("CAST('-0.0' AS", "CAST('0.0' AS")
                    String path = "${prefix}${writer}_${enabled}_${mode}/part_"
                    if (writer == "outfile") {
                        sql """${source} INTO OUTFILE "s3://${bucket}/${path}" FORMAT AS PARQUET
                                PROPERTIES (${storage}, "parquet.disable_dictionary"="${mode == 'disabled'}")"""
                    } else {
                        sql """INSERT INTO s3("file_path"="s3://${bucket}/${path}",
                                "format"="parquet", ${storage}) ${source}"""
                    }
                    def keys = listKeys(path)
                    assertTrue(!keys.isEmpty())
                    keys.each { inspect(download(it), enabled, mode) }
                    String file = """s3("uri"="s3://${bucket}/${path}*", "format"="parquet", ${storage})"""
                    def predicates = ["id >= 0", "id = 10001", "id IN (0, 200, ${(rows - 1) * 2})",
                            "f = CAST('0.0' AS FLOAT)", "d = CAST('0.0' AS DOUBLE)",
                            "all_null <=> CAST(NULL AS STRING)", "k IS NULL", "k = md5('3')",
                            "(k = md5('3') OR id = 0) AND f = CAST('0.0' AS FLOAT)"]
                    for (boolean readBloom : [false, true]) {
                        sql "SET enable_parquet_filter_by_bloom_filter = ${readBloom}"
                        predicates.eachWithIndex { predicate, index ->
                            quickTest("${writer}_${enabled}_${mode}_${readBloom}_${index}",
                                    "SELECT id FROM ${file} WHERE ${predicate} ORDER BY id",
                                    "SELECT id FROM (${expectedSource}) expected WHERE ${predicate} ORDER BY id")
                        }
                        String token = "parquet_bloom_${UUID.randomUUID()}"
                        String missing = "k = '80000000000000000000000000000000'"
                        quickTest(token, "SELECT /* ${token} */ id FROM ${file} WHERE ${missing} ORDER BY id",
                                "SELECT id FROM (${expectedSource}) expected WHERE ${missing} ORDER BY id")
                        if (!enabled || !readBloom) {
                            checkProfileCounters(token, [], ["RowGroupsFilteredByBloomFilter"])
                        } else if (mode != "retained") {
                            checkProfileCounters(token, ["RowGroupsFilteredByBloomFilter"], [])
                        }
                    }
                    qt_roundtrip "SELECT count(*), min(id), max(id), count(all_null), count(items) FROM ${file}"
                }
            }
        }
        test {
            sql "SET enable_parquet_write_bloom_filter = 'invalid'"
            exception "enable_parquet_write_bloom_filter"
        }
    } finally {
        original.each { name, value -> sql "SET ${name} = ${value}" }
        listKeys(prefix).each { key ->
            if (azure) {
                container.getBlobClient(key).deleteIfExists()
            } else {
                client.deleteObject(bucket, key)
            }
        }
    }
}
