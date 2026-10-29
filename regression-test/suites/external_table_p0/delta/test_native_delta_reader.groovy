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

suite("test_native_delta_reader", "p0,external") {
    def dorisHome = System.getenv("DORIS_HOME") ?: System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.trim().isEmpty()) {
        return
    }
    sql "SET enable_file_scanner_v2 = true"

    def sample = new File(dorisHome, "samples/datalake/deltalake_and_kudu/data/customer").toPath()
    def parser = new groovy.json.JsonSlurper()
    def actions = sample.resolve("_delta_log/00000000000000000000.json").toFile()
            .readLines("UTF-8").collect { parser.parseText(it) }
    def metadata = actions.find { it.metaData != null }.metaData
    def dataFiles = actions.findAll { it.add != null }.take(2)

    // Native RoaringBitmapArray for physical rows 0 and 2, also checked by
    // DeltaDeletionVectorReaderTest.DecodesSqlRegressionFixture.
    def inlineVector = "^9>0=0rr916AwakiXQKl0rr91000315c8Xg00062"
    def payload = java.util.Base64.decoder.decode("0NM5ZAEAAAAUAAAAOjAAAAEAAAAAAAEAEAAAAAAAAgA=")
    def crc = new java.util.zip.CRC32()
    crc.update(payload)
    def sidecar = java.nio.ByteBuffer.allocate(payload.length + 8)
            .putInt(payload.length).put(payload).putInt((int) crc.value).array()
    def vectorFile = new byte[sidecar.length + 5]
    vectorFile[0] = 1
    System.arraycopy(sidecar, 0, vectorFile, 5, sidecar.length)

    def createFixture = { String suffix, String storageType, long cardinality ->
        def root = java.nio.file.Files.createTempDirectory("doris-delta-reader-${suffix}-")
        def logDir = java.nio.file.Files.createDirectories(root.resolve("_delta_log"))
        def descriptor = [storageType: storageType, pathOrInlineDv: inlineVector,
                          sizeInBytes: payload.length, cardinality: cardinality]
        if (storageType != "i") {
            def sidecarPath = storageType == "p" ? root.resolve("vector.bin")
                    : root.resolve("prefix/deletion_vector_00000000-0000-0000-0000-000000000000.bin")
            java.nio.file.Files.createDirectories(sidecarPath.parent)
            java.nio.file.Files.write(sidecarPath, vectorFile)
            descriptor.pathOrInlineDv = storageType == "p" ? sidecarPath.toUri().toString()
                    : "prefix00000000000000000000"
            descriptor.offset = 5
        }
        def logActions = [
                [protocol: [minReaderVersion: 3, minWriterVersion: 7,
                            readerFeatures: ["deletionVectors"], writerFeatures: ["deletionVectors"]]],
                [metaData: metadata]
        ]
        dataFiles.each { action ->
            java.nio.file.Files.copy(sample.resolve(action.add.path), root.resolve(action.add.path))
            def add = new LinkedHashMap(action.add)
            add.deletionVector = descriptor
            logActions.add([add: add])
        }
        logDir.resolve("00000000000000000000.json").toFile().setText(
                logActions.collect { groovy.json.JsonOutput.toJson(it) }.join("\n") + "\n", "UTF-8")

        def catalog = "test_native_delta_reader_${suffix}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """
            CREATE CATALOG ${catalog} PROPERTIES (
                'type' = 'delta',
                'delta.catalog.type' = 'path',
                'delta.database' = 'default',
                'delta.table' = 'customer',
                'delta.table.path' = '${root.toUri()}',
                'test_connection' = 'false'
            )
        """
    }

    createFixture("inline", "i", 2)
    order_qt_inline_count "SELECT COUNT(*) FROM test_native_delta_reader_inline.`default`.customer"
    order_qt_inline_rows """
        SELECT c_custkey, c_name FROM test_native_delta_reader_inline.`default`.customer
        WHERE c_custkey <= 100
    """

    createFixture("absolute", "p", 2)
    order_qt_absolute_count "SELECT COUNT(*) FROM test_native_delta_reader_absolute.`default`.customer"
    order_qt_absolute_rows """
        SELECT c_custkey, c_name FROM test_native_delta_reader_absolute.`default`.customer
        WHERE c_custkey <= 100
    """

    createFixture("uuid", "u", 2)
    order_qt_uuid_count "SELECT COUNT(*) FROM test_native_delta_reader_uuid.`default`.customer"
    order_qt_uuid_rows """
        SELECT c_custkey, c_name FROM test_native_delta_reader_uuid.`default`.customer
        WHERE c_custkey <= 100
    """

    createFixture("invalid", "i", 3)
    test {
        sql "SELECT COUNT(*) FROM test_native_delta_reader_invalid.`default`.customer"
        exception "Delta deletion vector cardinality mismatch"
    }
}
