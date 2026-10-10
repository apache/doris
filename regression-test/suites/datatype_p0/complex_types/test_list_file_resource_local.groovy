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

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.URI
import java.net.URLDecoder
import java.util.concurrent.CopyOnWriteArrayList

// The fixture runs in the regression JVM and requires a same-host FE/BE cluster.
suite("test_list_file_resource_local") {
    if (!context.config.otherConfigs.get("enableFileTypeLocalResource")?.toString()?.toBoolean()) {
        logger.info("Skip test_list_file_resource_local: enableFileTypeLocalResource requires a same-host cluster")
        return
    }
    def backends = sql_return_maparray("SHOW BACKENDS").findAll { it.Alive.toString().toBoolean() }
    if (URI.create(context.config.jdbcUrl.substring(5)).host != "127.0.0.1"
            || backends.isEmpty() || backends.any {
                NetworkInterface.getByInetAddress(InetAddress.getByName(it.Host.toString())) == null
            }) {
        throw new IllegalStateException("enableFileTypeLocalResource requires JDBC on 127.0.0.1 and all live BEs on local network interfaces")
    }

    def requests = new CopyOnWriteArrayList<Map>()
    def handlerErrors = new CopyOnWriteArrayList<String>()
    def originalTimeZone = sql("SELECT @@time_zone")[0][0]
    def objects = [
        "dir/": 0, "dir/a-empty.txt": 0, "dir/b.json": 7, "dir/c.PNG": 11,
        "dir/no_extension": 13, "dir/sub/": 0, "dir/sub/nested.csv": 17,
        "dir/sub/deep/": 0, "dir/sub/deep/payload.parquet": 19,
        "dir2/": 0, "dir2/neighbor.txt": 21, "root.txt": 23
    ]
    def heads = [
        "/list-file-local/dir/b.json": [size: objects["dir/b.json"], etag: "canonical-head-etag"],
        "/list-file-other/dir/b.json": [size: 31, etag: "other-bucket-head-etag"]
    ]
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", { HttpExchange exchange ->
        try {
            def query = [:]
            exchange.requestURI.rawQuery?.split("&")?.each { part ->
                def pair = part.split("=", 2)
                query[URLDecoder.decode(pair[0], "UTF-8")] =
                        pair.length == 2 ? URLDecoder.decode(pair[1], "UTF-8") : ""
            }
            requests.add([method: exchange.requestMethod, path: exchange.requestURI.rawPath, query: query])
            exchange.responseHeaders.set("x-amz-request-id", "list-file-local-request")
            if (exchange.requestMethod == "HEAD"
                    && heads.containsKey(exchange.requestURI.rawPath) && query.isEmpty()) {
                // The same key in different buckets has distinct metadata; routing must follow the URI.
                def metadata = heads[exchange.requestURI.rawPath]
                exchange.responseHeaders.set("Content-Length", metadata.size.toString())
                exchange.responseHeaders.set("Content-Type", "application/json")
                exchange.responseHeaders.set("ETag", '"' + metadata.etag + '"')
                exchange.sendResponseHeaders(200, -1)
                return
            }
            if (exchange.requestMethod != "GET"
                    || !(exchange.requestURI.rawPath in ["/list-file-local", "/list-file-local/"])
                    || query["list-type"] != "2") {
                handlerErrors.add("Unexpected request: ${exchange.requestMethod} ${exchange.requestURI}".toString())
                exchange.sendResponseHeaders(400, -1)
                return
            }

            def prefix = query["prefix"] ?: ""
            def delimiter = query["delimiter"] ?: ""
            def entries = [:]
            objects.each { key, size ->
                if (key.startsWith(prefix)) {
                    def relative = key.substring(prefix.length())
                    int separator = delimiter ? relative.indexOf(delimiter) : -1
                    if (separator >= 0) {
                        def commonPrefix = prefix + relative.substring(0, separator + delimiter.length())
                        entries[commonPrefix] = [key: commonPrefix, common: true]
                    } else {
                        entries[key] = [key: key, size: size, common: false]
                    }
                }
            }
            def ordered = entries.values().sort { a, b -> a.key <=> b.key }
            def token = query["continuation-token"] ?: "page:0"
            int offset = token.substring(token.indexOf(":") + 1).toInteger()
            int pageSize = Math.min(2, (query["max-keys"] ?: "1000").toInteger())
            // An empty truncated page proves callers follow tokens rather than stopping on zero objects.
            def page = token.startsWith("empty:") ? [] : ordered.drop(offset).take(pageSize)
            int nextOffset = offset + page.size()
            boolean truncated = nextOffset < ordered.size()
            def nextToken = offset == 0 && !page.isEmpty() ? "empty:${nextOffset}" : "page:${nextOffset}"
            def xml = new StringBuilder('<?xml version="1.0" encoding="UTF-8"?>')
            xml.append('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">')
            xml.append("<Name>list-file-local</Name><Prefix>${prefix}</Prefix>")
            xml.append("<MaxKeys>${pageSize}</MaxKeys><KeyCount>${page.size()}</KeyCount>")
            xml.append("<IsTruncated>${truncated}</IsTruncated>")
            page.each { entry ->
                if (entry.common) {
                    xml.append("<CommonPrefixes><Prefix>${entry.key}</Prefix></CommonPrefixes>")
                } else {
                    xml.append("<Contents><Key>${entry.key}</Key><Size>${entry.size}</Size>")
                    // One object deliberately omits LastModified to exercise nullable timestamps.
                    if (entry.key != "dir/no_extension") {
                        xml.append('<LastModified>2024-01-01T00:00:00.123Z</LastModified>')
                    }
                    xml.append('<ETag>"ignored-list-etag"</ETag><StorageClass>STANDARD</StorageClass></Contents>')
                }
            }
            if (truncated) {
                xml.append("<NextContinuationToken>${nextToken}</NextContinuationToken>")
            }
            xml.append('</ListBucketResult>')
            byte[] body = xml.toString().getBytes("UTF-8")
            exchange.responseHeaders.set("Content-Type", "application/xml")
            exchange.sendResponseHeaders(200, body.length)
            exchange.responseBody.write(body)
        } catch (Exception e) {
            handlerErrors.add(e.toString())
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    server.start()

    try {
        sql "SET time_zone = '+00:00'"
        sql "DROP RESOURCE IF EXISTS 'list_file_local_s3'"
        sql "DROP RESOURCE IF EXISTS 'list_file_local_canonical_s3'"
        sql "DROP RESOURCE IF EXISTS 'list_file_local_hdfs'"
        sql """
            CREATE RESOURCE 'list_file_local_s3' PROPERTIES(
                "type"="s3", "AWS_ENDPOINT"="http://127.0.0.1:${server.address.port}",
                "AWS_REGION"="us-east-1", "AWS_BUCKET"="list-file-local", "AWS_ROOT_PATH"="unused-root",
                "AWS_ACCESS_KEY"="fake-list-file-key", "AWS_SECRET_KEY"="fake-list-file-secret",
                "AWS_REQUEST_TIMEOUT_MS"="2000", "AWS_CONNECTION_TIMEOUT_MS"="500",
                "use_path_style"="true", "s3_validity_check"="false")
        """
        sql """
            CREATE RESOURCE 'list_file_local_canonical_s3' PROPERTIES(
                "type"="s3", "s3.endpoint"="http://127.0.0.1:${server.address.port}",
                "s3.region"="us-east-1", "s3.bucket"="list-file-local", "s3.root.path"="unused-root",
                "s3.access_key"="fake-list-file-key", "s3.secret_key"="fake-list-file-secret",
                "s3.connection.request.timeout"="2000", "s3.connection.timeout"="500",
                "use_path_style"="true", "s3_validity_check"="false")
        """
        sql """
            CREATE RESOURCE 'list_file_local_hdfs' PROPERTIES(
                "type"="hdfs", "fs.defaultFS"="hdfs://127.0.0.1:1",
                "hadoop.username"="list_file_test", "ipc.client.connect.timeout"="100",
                "ipc.client.connect.max.retries"="0")
        """
        qt_creation_no_io "SELECT ${requests.size()}"
        qt_schema 'DESC FUNCTION list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")'
        qt_schema_no_io "SELECT ${requests.size()}"

        qt_default """
            SELECT * FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            ORDER BY path
        """
        qt_default_protocol """
            SELECT ${requests.size()}, ${requests.every { it.method == "GET"
                && it.path in ["/list-file-local", "/list-file-local/"]
                && it.query["list-type"] == "2" && it.query["prefix"] == "dir/"
                && it.query["delimiter"] == "/" && it.query["max-keys"].toInteger() > 0 }},
                ${requests.any { it.query["continuation-token"]?.startsWith("empty:") }}
        """

        qt_explicit_false """
            SELECT * FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir",
                                    "recursive"="false") ORDER BY path
        """
        qt_path_only """
            SELECT path FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            ORDER BY path
        """
        qt_size_only """
            SELECT size FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            ORDER BY size
        """
        qt_time_only """
            SELECT modification_time
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            ORDER BY modification_time
        """
        qt_file_only """
            SELECT file FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            ORDER BY ELEMENT_AT(file, 'uri')
        """
        requests.clear()
        qt_recursive_getters """
            SELECT path, size, modification_time, path = ELEMENT_AT(file, 'uri'), size = ELEMENT_AT(file, 'size'),
                   ELEMENT_AT(file, 'offset'), ELEMENT_AT(file, 'content_type'), ELEMENT_AT(file, 'checksum'), file IS NULL,
                   modification_time IS NULL,
                   modification_time = CAST('2024-01-01 00:00:00.123' AS DATETIMEV2(3))
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="true")
            ORDER BY path
        """
        qt_recursive_protocol """
            SELECT ${requests.size()}, ${requests.every { it.query["prefix"] == "dir/"
                && !it.query.containsKey("delimiter") }},
                ${requests.any { it.query["continuation-token"]?.startsWith("empty:") }}
        """
        qt_root """
            SELECT path, size, modification_time
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local")
            ORDER BY path
        """
        qt_root_slash """
            SELECT path, size, modification_time
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/")
            ORDER BY path
        """
        qt_empty_directory """
            SELECT * FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/missing/")
            ORDER BY path
        """
        qt_count """
            SELECT COUNT(*), COUNT(file), COUNT(modification_time), SUM(size)
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir", "recursive"="true")
        """

        def originalParallelSettings = sql("SELECT @@ignore_storage_data_distribution, @@parallel_pipeline_task_num")[0]
        try {
            sql "SET ignore_storage_data_distribution = true"
            sql "SET parallel_pipeline_task_num = 2"
            requests.clear()
            qt_parallel_count """
                SELECT COUNT(*), COUNT(DISTINCT path), SUM(size)
                FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="true")
            """
            qt_parallel_requests "SELECT ${requests.size()}"
            requests.clear()
            qt_parallel_count_repeat """
                SELECT COUNT(*), COUNT(DISTINCT path), SUM(size)
                FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="true")
            """
            qt_parallel_requests_repeat "SELECT ${requests.size()}"
            qt_parallel_paths """
                SELECT path FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="true")
                ORDER BY path
            """
        } finally {
            sql "SET ignore_storage_data_distribution = ${originalParallelSettings[0]}"
            sql "SET parallel_pipeline_task_num = ${originalParallelSettings[1]}"
        }

        requests.clear()
        // The first page contains one marker and one empty file. LIMIT must stop before fetching the next page.
        qt_limit """
            SELECT path, size, modification_time, file
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="true")
            LIMIT 1
        """
        qt_limit_requests "SELECT ${requests.size()}"
        requests.clear()
        qt_limit_zero """
            SELECT * FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            LIMIT 0
        """
        qt_limit_zero_requests "SELECT ${requests.size()}"

        sql "DROP TABLE IF EXISTS test_list_file_resource_values"
        sql """
            CREATE TABLE test_list_file_resource_values (
                path VARCHAR(65533) NOT NULL, size BIGINT NOT NULL, modification_time DATETIMEV2(3) NULL, f FILE NOT NULL)
            DUPLICATE KEY(path) DISTRIBUTED BY HASH(path) BUCKETS 1
            PROPERTIES("replication_num"="1")
        """
        sql """
            INSERT INTO test_list_file_resource_values
            SELECT path, size, modification_time, file
            FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="true")
        """
        qt_inserted """
            SELECT path, size, modification_time, f, path = ELEMENT_AT(f, 'uri'), size = ELEMENT_AT(f, 'size'),
                   ELEMENT_AT(f, 'offset'), ELEMENT_AT(f, 'checksum')
            FROM test_list_file_resource_values ORDER BY path
        """

        requests.clear()
        qt_canonical_list """
            SELECT path, size, file
            FROM list_file("resource"="list_file_local_canonical_s3", "uri"="s3://list-file-local/dir/")
            ORDER BY path
        """
        qt_canonical_list_protocol "SELECT ${requests.every { it.method == 'GET' }}"
        requests.clear()
        qt_canonical_to_file_head """
            SELECT TO_FILE('list_file_local_canonical_s3', 's3://list-file-local/dir/b.json')
        """
        qt_canonical_head_protocol """
            SELECT ${requests.size()}, ${requests.every { it.method == "HEAD"
                    && it.path == "/list-file-local/dir/b.json" && it.query.isEmpty() }}
        """

        sql "DROP TABLE IF EXISTS test_list_file_canonical_bucket_uris"
        sql """
            CREATE TABLE test_list_file_canonical_bucket_uris (id INT NOT NULL, uri STRING NOT NULL)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")
        """
        sql """
            INSERT INTO test_list_file_canonical_bucket_uris VALUES
            (1, 's3://list-file-local/dir/b.json'),
            (2, 's3://list-file-other/dir/b.json'),
            (3, 's3://list-file-local/dir/b.json')
        """
        requests.clear()
        // One TO_FILE expression reads primary -> other -> primary in the same batch.
        // Its returned FILE rows must retain the corresponding 7 -> 31 -> 7 sizes.
        qt_canonical_mixed_buckets """
            SELECT id, TO_FILE('list_file_local_canonical_s3', uri)
            FROM test_list_file_canonical_bucket_uris ORDER BY id
        """
        assertEquals(["/list-file-local/dir/b.json", "/list-file-other/dir/b.json",
                      "/list-file-local/dir/b.json"], requests.collect { it.path },
                "TO_FILE must HEAD each URI's bucket and safely return to the first bucket")
        assertTrue(requests.every { it.method == "HEAD" && it.query.isEmpty() },
                "Mixed-bucket TO_FILE must use only HEAD requests")

        requests.clear()
        for (def invalid in [
            [properties: '"uri"="s3://list-file-local/dir/"', message: "resource"],
            [properties: '"resource"="list_file_local_s3"', message: "uri"],
            [properties: '"resource"="missing_list_file_resource", "uri"="s3://list-file-local/dir/"', message: "Can not find resource"],
            [properties: '"resource"="list_file_local_hdfs", "uri"="s3://list-file-local/dir/"', message: "S3"],
            [properties: '"resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "unknown"="value"', message: "unknown"],
            [properties: '"resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/", "recursive"="yes"', message: "recursive"],
            [properties: '"resource"="list_file_local_s3", "uri"="s3://other-bucket/dir/"', message: "bucket"],
            [properties: '"resource"="list_file_local_s3", "uri"="relative/dir/"', message: "uri"]
        ]) {
            test {
                sql "SELECT * FROM list_file(${invalid.properties})"
                exception invalid.message
            }
        }
        qt_invalid_no_io "SELECT ${requests.size()}"

        sql "DROP USER IF EXISTS 'list_file_local_user'"
        sql "CREATE USER 'list_file_local_user' IDENTIFIED BY 'ListFile_123'"
        sql "GRANT SELECT_PRIV ON ${context.dbName}.* TO 'list_file_local_user'"
        def userJdbcUrl = context.connection.metaData.URL
        connect("list_file_local_user", "ListFile_123", userJdbcUrl) {
            test {
                sql 'SELECT * FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")'
                exception "denied"
            }
        }
        qt_denied_no_io "SELECT ${requests.size()}"
        sql "GRANT USAGE_PRIV ON RESOURCE list_file_local_s3 TO 'list_file_local_user'"
        connect("list_file_local_user", "ListFile_123", userJdbcUrl) {
            qt_granted_count """
                SELECT COUNT(*) FROM list_file("resource"="list_file_local_s3", "uri"="s3://list-file-local/dir/")
            """
        }
        qt_handler_errors "SELECT ${handlerErrors.size()}"
    } finally {
        server.stop(0)
        sql "SET time_zone = '${originalTimeZone}'"
        sql "DROP USER IF EXISTS 'list_file_local_user'"
        sql "DROP RESOURCE IF EXISTS 'list_file_local_hdfs'"
        sql "DROP RESOURCE IF EXISTS 'list_file_local_s3'"
        sql "DROP RESOURCE IF EXISTS 'list_file_local_canonical_s3'"
    }
}
