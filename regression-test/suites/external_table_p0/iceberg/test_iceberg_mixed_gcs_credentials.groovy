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
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.net.InetSocketAddress
import java.net.URLDecoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

// SHOW PARTITIONS reads both manifests on FE. The fixture has one category=alpha partition,
// one 123-byte data file and seven records; no data-file read or external cloud service is needed.
suite("test_iceberg_mixed_gcs_credentials", "p0,external") {
    def fixture = new File(context.config.dataPath, "external_table_p0/iceberg/mixed_gcs_credentials")
    Map metadata = new JsonSlurper().parse(new File(fixture, "metadata.json")) as Map
    Map<String, byte[]> objects = [
            "public/list.avro": new File(fixture, "list.avro").bytes,
            "private/manifest.avro": new File(fixture, "manifest.avro").bytes,
            "v1.metadata.json": new File(fixture, "metadata.json").bytes]
    def mode = new AtomicReference<String>("forward")
    def accepted = new ConcurrentHashMap<String, AtomicInteger>()
    def rejected = new AtomicInteger()
    def requests = new ConcurrentLinkedQueue<String>()
    def executor = Executors.newFixedThreadPool(4)
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)
    server.setExecutor(executor)
    // Like HTTP TVF fixtures, the advertised address must be reachable from FE.
    String host = context.config.otherConfigs.get("httpTvfHost")
            ?: context.config.otherConfigs.get("externalEnvIp") ?: "127.0.0.1"
    String endpoint = "http://${host}:${server.address.port}"
    def jsonResponse = { HttpExchange exchange, int status, Object value ->
        byte[] body = JsonOutput.toJson(value).getBytes("UTF-8")
        exchange.responseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, body.length)
        exchange.responseBody.write(body)
    }
    def sdkConfig = { String token ->
        ["gcs.project-id": "fixture-project", "gcs.service.host": endpoint, "gcs.oauth2.token": token]
    }
    server.createContext("/", { HttpExchange exchange ->
        try {
            String path = URLDecoder.decode(exchange.requestURI.rawPath, "UTF-8")
            requests.add(exchange.requestMethod + " " + path)
            if (path == "/v1/config") {
                jsonResponse(exchange, 200, [defaults: [:], overrides: [:]])
            } else if (path == "/v1/namespaces") {
                jsonResponse(exchange, 200, [namespaces: [["fixture_db"]]])
            } else if (path == "/v1/namespaces/fixture_db") {
                jsonResponse(exchange, 200, [namespace: ["fixture_db"], properties: [:]])
            } else if (path == "/v1/namespaces/fixture_db/tables") {
                jsonResponse(exchange, 200, [identifiers: ["forward", "reverse", "same_prefix", "legacy"].collect {
                    [namespace: ["fixture_db"], name: it]
                }])
            } else if (path.startsWith("/v1/namespaces/fixture_db/tables/")
                    && ["forward", "reverse", "same_prefix", "legacy"].contains(
                            path.substring(path.lastIndexOf('/') + 1))) {
                String table = path.substring(path.lastIndexOf('/') + 1)
                def broad = [prefix: "gs://fixture-bucket/", config: sdkConfig("broad-token")]
                // HadoopFileIO ignores scoped configs: this scope must retain the catalog's HMAC identity.
                def narrow = [prefix: "gs://fixture-bucket/private/", config: table == "legacy"
                        ? ["fs.gs.auth.type": "UNAUTHENTICATED"] : sdkConfig("narrow-token")]
                def unrelated = [prefix: "gs://other-bucket/", config: sdkConfig("unrelated-token")]
                def credentials = table == "reverse" ? [narrow, unrelated, broad] : [broad, unrelated, narrow]
                if (table == "same_prefix") {
                    credentials = [broad, [prefix: narrow.prefix, config: sdkConfig("wrong-token")],
                            unrelated, narrow]
                }
                jsonResponse(exchange, 200, ["metadata-location": "gs://fixture-bucket/v1.metadata.json",
                        metadata: metadata, "storage-credentials": credentials, config: [:]])
            } else {
                boolean nativeApi = path.startsWith("/storage/v1/b/fixture-bucket/o/")
                        || path.startsWith("/download/storage/v1/b/fixture-bucket/o/")
                String object = nativeApi ? path.substring(path.indexOf("/o/") + 3)
                        : path.startsWith("/fixture-bucket/") ? path.substring("/fixture-bucket/".length()) : ""
                byte[] body = objects.get(object)
                if (body == null) {
                    jsonResponse(exchange, 404, [error: [message: "Unknown fixture object", type: "NoSuchKey", code: 404]])
                    return
                }
                String authorization = exchange.requestHeaders.getFirst("Authorization")
                boolean hmac = mode.get() == "legacy" && object.startsWith("private/")
                String token = object.startsWith("private/") ? "narrow-token" : "broad-token"
                boolean authorized = hmac ? !nativeApi && authorization != null
                        && authorization.startsWith("AWS4-HMAC-SHA256 ")
                        && authorization.contains("Credential=fixture-storage-key/")
                        : nativeApi && authorization == "Bearer ${token}"
                if (!authorized) {
                    rejected.incrementAndGet()
                    exchange.sendResponseHeaders(403, -1)
                    return
                }
                accepted.computeIfAbsent(mode.get() + ":" + object, { new AtomicInteger() }).incrementAndGet()
                boolean media = exchange.requestURI.rawQuery?.split('&')?.contains("alt=media")
                if (nativeApi && !media) {
                    jsonResponse(exchange, 200, [bucket: "fixture-bucket", name: object,
                            size: body.length.toString(), generation: "1", metageneration: "1"])
                    return
                }
                exchange.responseHeaders.set("ETag", '"fixture-etag"')
                exchange.responseHeaders.set("Last-Modified", "Tue, 14 Nov 2023 22:13:20 GMT")
                String range = exchange.requestHeaders.getFirst("Range")
                int start = 0
                int end = body.length - 1
                if (range != null) {
                    def match = range =~ /^bytes=(\d+)-(\d*)$/
                    assertTrue(match.matches(), "Unexpected fixture range: ${range}")
                    start = match.group(1).toInteger()
                    if (!match.group(2).isEmpty()) {
                        end = Math.min(end, match.group(2).toInteger())
                    }
                    if (start > end) {
                        exchange.sendResponseHeaders(416, -1)
                        return
                    }
                    exchange.responseHeaders.set("Content-Range", "bytes ${start}-${end}/${body.length}")
                }
                exchange.responseHeaders.set("Content-Length", (end - start + 1).toString())
                boolean head = exchange.requestMethod == "HEAD"
                exchange.sendResponseHeaders(range == null ? 200 : 206, head ? -1 : end - start + 1)
                if (!head) {
                    exchange.responseBody.write(body, start, end - start + 1)
                }
            }
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    server.start()
    try {
        sql "DROP CATALOG IF EXISTS test_iceberg_mixed_gcs_credentials"
        // The catalog starts with Hadoop/HMAC settings; scoped-only gcs.* options arrive on table load.
        sql """CREATE CATALOG test_iceberg_mixed_gcs_credentials PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${endpoint}',
            'warehouse' = 'gs://fixture-bucket/warehouse', 'rest.auth.type' = 'none',
            'provider' = 'GCP', 'gs.endpoint' = '${endpoint}', 'use_path_style' = 'true',
            'gs.access_key' = 'fixture-storage-key', 'gs.secret_key' = 'fixture-storage-secret',
            'iceberg.rest.vended-credentials-enabled' = 'false',
            'fs.s3a.endpoint.region' = 'us-east-1', 'fs.s3a.connection.ssl.enabled' = 'false',
            'fs.s3a.retry.limit' = '0', 'fs.s3a.attempts.maximum' = '1', 'fs.s3a.bucket.probe' = '0'
        )"""
        order_qt_forward "SHOW PARTITIONS FROM test_iceberg_mixed_gcs_credentials.fixture_db.forward"
        mode.set("reverse")
        order_qt_reverse "SHOW PARTITIONS FROM test_iceberg_mixed_gcs_credentials.fixture_db.reverse"
        mode.set("same_prefix")
        order_qt_duplicate "SHOW PARTITIONS FROM test_iceberg_mixed_gcs_credentials.fixture_db.same_prefix"
        mode.set("legacy")
        order_qt_legacy "SHOW PARTITIONS FROM test_iceberg_mixed_gcs_credentials.fixture_db.legacy"
        ["forward", "reverse", "same_prefix", "legacy"].each { String table ->
            objects.keySet().each { String object ->
                assertTrue((accepted.get(table + ":" + object)?.get() ?: 0) > 0,
                        "No authenticated read for ${table}:${object}")
            }
        }
        assertEquals(0, rejected.get())
    } finally {
        logger.info("GCS fixture requests: ${requests}")
        server.stop(0)
        executor.shutdownNow()
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS))
    }
}
