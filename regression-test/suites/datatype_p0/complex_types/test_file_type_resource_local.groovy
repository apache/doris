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

// Like test_http_tvf, the fixture runs inside the regression JVM. No external S3 service is used.
// JDBC and the mock use loopback; same-host BEs may advertise any local interface address.
suite("test_file_type_resource_local") {
    if (!context.config.otherConfigs.get("enableFileTypeLocalResource")?.toString()?.toBoolean()) {
        logger.info("Skip test_file_type_resource_local: enableFileTypeLocalResource requires a same-host cluster")
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
    def heads = [
        "/file-type-local/full.bin": [
            "Content-Length": "0", "Content-Type": "image/png", "ETag": '"ignored-etag"',
            "x-amz-checksum-type": "FULL_OBJECT",
            "x-amz-checksum-sha256": "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
            "x-amz-checksum-md5": "1B2M2Y8AsgTpgAmY7PhCfg=="],
        "/file-type-local/etag.txt": [
            "Content-Length": "7", "Content-Type": "text/plain", "ETag": '"Opaque-2"',
            "x-amz-checksum-type": "COMPOSITE",
            "x-amz-checksum-sha256": "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="],
        // S3URI keeps the literal percent sequence in the key; the SDK escapes '%' on the wire.
        "/file-type-local/raw/a%252Fb.bin": [
            "Content-Length": "11", "Content-Type": "application/octet-stream", "ETag": '"raw-etag"']
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
            def path = exchange.requestURI.rawPath
            requests.add([method: exchange.requestMethod, path: path, query: query,
                          checksumMode: exchange.requestHeaders.getFirst("x-amz-checksum-mode")])
            exchange.responseHeaders.set("x-amz-request-id", "file-type-local-request")
            if (exchange.requestMethod == "HEAD") {
                def metadata = heads[path]
                if (metadata == null) {
                    exchange.responseHeaders.set("x-amz-error-code", "NoSuchKey")
                    exchange.sendResponseHeaders(404, -1)
                } else {
                    metadata.each { key, value -> exchange.responseHeaders.set(key, value) }
                    exchange.responseHeaders.set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
                    exchange.sendResponseHeaders(200, -1)
                }
            } else {
                handlerErrors.add("Unexpected request: ${exchange.requestMethod} ${exchange.requestURI}".toString())
                exchange.sendResponseHeaders(400, -1)
            }
        } catch (Exception e) {
            handlerErrors.add(e.toString())
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    server.start()

    try {
        sql "DROP RESOURCE IF EXISTS 'file_type_local_s3'"
        sql """
            CREATE RESOURCE 'file_type_local_s3' PROPERTIES(
                "type"="s3", "AWS_ENDPOINT"="http://127.0.0.1:${server.address.port}",
                "AWS_REGION"="us-east-1", "AWS_BUCKET"="file-type-local", "AWS_ROOT_PATH"="unused-root",
                "AWS_ACCESS_KEY"="fake-file-type-key", "AWS_SECRET_KEY"="fake-file-type-secret",
                "AWS_REQUEST_TIMEOUT_MS"="2000", "AWS_CONNECTION_TIMEOUT_MS"="500",
                "use_path_style"="true", "s3_validity_check"="false")
        """
        qt_null "SELECT TO_FILE('file_type_local_s3', NULL)"
        assertEquals(0, requests.size(), "Resource creation and NULL must not send requests")

        qt_head_full "SELECT TO_FILE('file_type_local_s3', 's3://file-type-local/full.bin')"
        qt_head_etag "SELECT TO_FILE('file_type_local_s3', 's3://file-type-local/etag.txt')"
        qt_head_raw "SELECT TO_FILE('file_type_local_s3', 's3://file-type-local/raw/a%2Fb.bin?versionId=AbC')"
        assertTrue(handlerErrors.isEmpty(), handlerErrors.toString())
        assertEquals(heads.keySet().toList(), requests.collect { it.path })
        assertTrue(requests.every { it.method == "HEAD" && it.checksumMode == null
                && it.query.isEmpty() }, requests.toString())

        requests.clear()
        test {
            sql "SELECT TO_FILE('file_type_local_s3', 's3://file-type-local/missing.bin')"
            exception "failed to head s3 file"
        }
        assertEquals(1, requests.size(), requests.toString())
        assertEquals("HEAD", requests[0].method)
        assertEquals("/file-type-local/missing.bin", requests[0].path)
        assertEquals(null, requests[0].checksumMode)
        assertTrue(requests[0].query.isEmpty(), requests.toString())

    } finally {
        server.stop(0)
    }
}
