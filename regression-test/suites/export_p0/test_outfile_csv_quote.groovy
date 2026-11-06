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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request

import java.net.URI
import java.nio.charset.StandardCharsets

class CsvQuoteS3Scope {
    private static Throwable recordFailure(Throwable primary, Throwable cleanup) {
        if (primary == null) {
            return cleanup
        }
        if (!primary.is(cleanup)) {
            primary.addSuppressed(cleanup)
        }
        return primary
    }

    static void run(Closure initialize, Closure body, Closure listKeys, Closure deleteKey,
            Closure shutdown) {
        def client = null
        Throwable failure = null
        try {
            client = initialize.call()
            body.call(client)
        } catch (Throwable t) {
            failure = t
        } finally {
            if (client != null) {
                try {
                    listKeys.call(client).each { key ->
                        try {
                            deleteKey.call(client, key)
                        } catch (Throwable t) {
                            failure = recordFailure(failure, t)
                        }
                    }
                } catch (Throwable t) {
                    failure = recordFailure(failure, t)
                }
                try {
                    shutdown.call(client)
                } catch (Throwable t) {
                    failure = recordFailure(failure, t)
                }
            }
        }
        if (failure != null) {
            throw failure
        }
    }
}

suite("test_outfile_csv_quote", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()
    [
            "ak": ak,
            "sk": sk,
            "s3Endpoint": s3Endpoint,
            "s3Region": region,
            "s3BucketName": bucket
    ].each { name, value ->
        assertTrue(value != null && !value.trim().isEmpty(),
                "test_outfile_csv_quote requires non-empty ${name}")
    }

    String endpointUri = s3Endpoint.contains("://") ? s3Endpoint : "https://${s3Endpoint}"
    String endpointHost = new URI(endpointUri).host ?: ""
    String provider = getS3Provider()
    boolean usePathStyle = (provider != null && provider.equalsIgnoreCase("MINIO"))
            || endpointHost.equalsIgnoreCase("localhost")
            || endpointHost.toLowerCase().contains("minio")
            || endpointHost.matches("\\d{1,3}(\\.\\d{1,3}){3}")

    def executeSensitiveSql = { String statement ->
        def jdbcStatement = context.getConnection().createStatement()
        try {
            List<List<Object>> rows = []
            if (jdbcStatement.execute(statement)) {
                jdbcStatement.resultSet.withCloseable { resultSet ->
                    int columnCount = resultSet.metaData.columnCount
                    while (resultSet.next()) {
                        List<Object> row = []
                        for (int column = 1; column <= columnCount; column++) {
                            row.add(resultSet.getObject(column))
                        }
                        rows.add(row)
                    }
                }
            }
            return rows
        } finally {
            jdbcStatement.close()
        }
    }

    def listKeys = { AmazonS3 s3Client, String prefix ->
        List<String> keys = []
        String continuationToken = null
        while (true) {
            def request = new ListObjectsV2Request()
                    .withBucketName(bucket)
                    .withPrefix(prefix)
            if (continuationToken != null) {
                request.setContinuationToken(continuationToken)
            }
            def listing = s3Client.listObjectsV2(request)
            listing.objectSummaries.each { summary -> keys.add(summary.key) }
            if (!listing.truncated) {
                break
            }
            continuationToken = listing.nextContinuationToken
            assertTrue(continuationToken != null,
                    "truncated S3 listing must provide a continuation token")
        }
        keys.sort()
        return keys
    }

    def readUtf8Object = { AmazonS3 s3Client, String key ->
        def object = s3Client.getObject(bucket, key)
        try {
            def bytes = new ByteArrayOutputStream()
            object.objectContent.withCloseable { input ->
                byte[] buffer = new byte[8192]
                int count
                while ((count = input.read(buffer)) != -1) {
                    bytes.write(buffer, 0, count)
                }
            }
            return new String(bytes.toByteArray(), StandardCharsets.UTF_8)
        } finally {
            object.close()
        }
    }

    String uuid = UUID.randomUUID().toString().replace("-", "")
    String rootPrefix = "regression/export_p0/test_outfile_csv_quote/${uuid}/"
    String outfilePrefix = "${rootPrefix}outfile/result_"
    String exportPrefix = "${rootPrefix}export/"
    String exportLabel = "test_outfile_csv_quote_label_${uuid}"
    String expected = "\"id\",\"text_value\",\"nullable_value\"\n" +
            "\"1\",\"comma,\"\"quote\"\"\nnext\",\"\\N\"\n"

    CsvQuoteS3Scope.run({
        def credentials = new BasicAWSCredentials(ak, sk)
        def endpointConfiguration = new EndpointConfiguration(endpointUri, region)
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withPathStyleAccessEnabled(usePathStyle)
                .build()
    }, { AmazonS3 s3Client ->
        sql """DROP VIEW IF EXISTS test_outfile_csv_quote_view"""
        sql """DROP TABLE IF EXISTS test_outfile_csv_quote"""
        sql """
        CREATE TABLE test_outfile_csv_quote (
            id INT NOT NULL,
            text_value STRING NOT NULL,
            nullable_value STRING NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
        """
        sql """
        INSERT INTO test_outfile_csv_quote
        SELECT 1, concat('comma,"quote"', unhex('0a'), 'next'), CAST(NULL AS STRING)
        """
        sql """
        CREATE VIEW test_outfile_csv_quote_view AS
        SELECT id, text_value, nullable_value FROM test_outfile_csv_quote
        """

        def outfileResult = executeSensitiveSql("""
        SELECT id, text_value, nullable_value
        FROM test_outfile_csv_quote
        ORDER BY id
        INTO OUTFILE "s3://${bucket}/${outfilePrefix}"
        FORMAT AS CSV_WITH_NAMES
        PROPERTIES (
            "column_separator" = ",",
            "line_delimiter" = "\\n",
            "csv_quote" = "true",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${region}",
            "s3.secret_key" = "${sk}",
            "s3.access_key" = "${ak}",
            "use_path_style" = "${usePathStyle}"
        )
        """)
        assertEquals(1, outfileResult.size())
        assertEquals("1", outfileResult[0][0].toString())
        List<String> outfileKeys = listKeys(s3Client, outfilePrefix)
        assertEquals(1, outfileKeys.size())
        assertEquals(expected, readUtf8Object(s3Client, outfileKeys[0]))
        logger.info("CSV_QUOTE_P0_OUTFILE_BYTES_ASSERTED files=1 bytes=${expected.getBytes(StandardCharsets.UTF_8).length}")

        executeSensitiveSql("""
        EXPORT TABLE test_outfile_csv_quote_view
        TO "s3://${bucket}/${exportPrefix}"
        PROPERTIES (
            "label" = "${exportLabel}",
            "format" = "csv_with_names",
            "column_separator" = ",",
            "line_delimiter" = "\\n",
            "csv_quote" = "true",
            "parallelism" = "1"
        )
        WITH S3 (
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${region}",
            "s3.secret_key" = "${sk}",
            "s3.access_key" = "${ak}",
            "use_path_style" = "${usePathStyle}"
        )
        """)

        def exportRow = null
        for (int attempt = 0; attempt < 120; attempt++) {
            def rows = sql """SHOW EXPORT WHERE LABEL = "${exportLabel}" ORDER BY CreateTime DESC LIMIT 1"""
            assertEquals(1, rows.size())
            String state = rows[0][2]
            if (state == "FINISHED") {
                exportRow = rows[0]
                break
            }
            if (state == "CANCELLED") {
                throw new IllegalStateException("EXPORT failed: ${rows[0][10]}")
            }
            sleep(1000)
        }
        assertTrue(exportRow != null, "EXPORT did not reach FINISHED within 120 seconds")
        assertEquals("FINISHED", exportRow[2])
        def exportFiles = parseJson(exportRow[11])
        assertEquals("1", exportFiles.fileNumber[0][0].toString())

        List<String> exportKeys = listKeys(s3Client, exportPrefix)
        assertEquals(1, exportKeys.size())
        assertEquals(expected, readUtf8Object(s3Client, exportKeys[0]))
        logger.info("CSV_QUOTE_P0_EXPORT_BYTES_ASSERTED files=1 bytes=${expected.getBytes(StandardCharsets.UTF_8).length}")

        // Validation fails before any I/O, so this negative case uses inert credentials.
        test {
            sql """
            SELECT 1
            INTO OUTFILE "s3://${bucket}/${rootPrefix}invalid/result_"
            FORMAT AS CSV
            PROPERTIES (
                "column_separator" = "|\\\"",
                "csv_quote" = "true",
                "s3.endpoint" = "https://s3.invalid",
                "s3.region" = "us-east-1",
                "s3.secret_key" = "unused",
                "s3.access_key" = "unused",
                "use_path_style" = "true"
            )
            """
            exception "must not contain double quote when csv_quote is true"
        }
    }, { AmazonS3 s3Client ->
        List<String> actualKeys = listKeys(s3Client, rootPrefix)
        logger.info("CSV_QUOTE_P0_CLEANUP_KEYS count=${actualKeys.size()}")
        return actualKeys
    }, { AmazonS3 s3Client, String key ->
        s3Client.deleteObject(bucket, key)
    }, { AmazonS3 s3Client ->
        s3Client.shutdown()
        logger.info("CSV_QUOTE_P0_S3_CLIENT_SHUTDOWN")
    })
}
