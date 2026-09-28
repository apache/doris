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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;

import com.google.common.io.ByteStreams;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Reads a namespace-managed table on S3 through two S3 stubs that serve different data under one
 * key, standing in for two endpoints of a table. The namespace vends the endpoint in its own
 * spelling ({@code endpoint}), as a REST catalog may.
 *
 * <p>The FE must plan from the endpoint and credentials the BE is handed. That breaks if the SDK
 * reuses the object store another read still holds for the same table, or if the FE environment's
 * {@code AWS_ENDPOINT} takes the place of the vended endpoint.
 */
@Disabled("Re-enable after fixing Arrow C Data JNI compatibility: CI libstdc++ lacks CXXABI_1.3.9")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LanceManagedS3StoreTest {
    private static final String BEARER_TOKEN = "managed-s3-token";
    private static final String TABLE = "managed_s3";
    private static final String BUCKET = "bucket";
    private static final String DATASET_KEY = "table.lance";
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    /** Rows at version 3 behind each endpoint. */
    private static final long ROWS_A = 6;
    private static final long ROWS_B = 10;

    private Path tempDir;
    private S3Stub storeA;
    private S3Stub storeB;
    private HttpServer namespace;
    private String restUri;
    private volatile Map<String, String> vendedOptions = Collections.emptyMap();
    private final AtomicInteger describes = new AtomicInteger();

    /** libc, to set the environment Lance reads natively; Java cannot change its own. */
    private interface CLibrary extends Library {
        int setenv(String name, String value, int overwrite);

        int unsetenv(String name);
    }

    @BeforeAll
    public void setUp() throws Exception {
        LanceJniTestSupport.assumeJniBindingsLoadable();
        tempDir = Files.createTempDirectory("lance_managed_s3");
        Path rootA = tempDir.resolve("a");
        Path rootB = tempDir.resolve("b");
        writeThreeVersions(rootA.resolve(DATASET_KEY), 3);
        writeThreeVersions(rootB.resolve(DATASET_KEY), 5);
        storeA = new S3Stub(rootA);
        storeB = new S3Stub(rootB);
        namespace = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        namespace.createContext("/", this::handleNamespaceRequest);
        namespace.start();
        restUri = "http://127.0.0.1:" + namespace.getAddress().getPort() + "/";
    }

    @AfterAll
    public void tearDown() throws IOException {
        for (HttpServer server : new HttpServer[] {namespace, storeA == null ? null : storeA.server,
                storeB == null ? null : storeB.server}) {
            if (server != null) {
                server.stop(0);
            }
        }
        if (tempDir != null) {
            try (Stream<Path> paths = Files.walk(tempDir)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    private void vendEndpoint(String endpoint, String... extra) {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", endpoint);
        options.put("region", "us-east-1");
        options.put("access_key_id", "ak");
        options.put("secret_access_key", "sk");
        options.put("virtual_hosted_style_request", "false");
        for (int i = 0; i < extra.length; i += 2) {
            options.put(extra[i], extra[i + 1]);
        }
        vendedOptions = options;
    }

    /**
     * Gabriel39's case on apache/doris#68453: Q1 holds a store for endpoint A when the namespace
     * moves the table to endpoint B. Q2 must read B, which is what its BE is handed, and Q1 A.
     */
    @Test
    public void testOverlappingReadsKeepTheirOwnEndpoint() throws Exception {
        LanceExternalCatalog catalog = newCatalog(400, "lance_managed_s3_overlap");
        try {
            vendEndpoint(storeA.endpoint());
            storeA.holdNextRequest();
            CompletableFuture<LanceTableMetadata> q1 = CompletableFuture.supplyAsync(
                    () -> catalog.loadTableMetadata("default", TABLE));
            Assertions.assertTrue(storeA.awaitHeld(), "Q1 never reached endpoint A");

            vendEndpoint(storeB.endpoint());
            LanceTableMetadata q2 = catalog.loadTableMetadata("default", TABLE);
            Assertions.assertEquals(storeB.endpoint(), q2.getLanceStorageOptions().get("aws_endpoint"));
            Assertions.assertEquals(ROWS_B, q2.getRowCount(), "Q2 must plan from endpoint B, where its BE reads");

            storeA.release();
            LanceTableMetadata q1Metadata = q1.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(storeA.endpoint(), q1Metadata.getLanceStorageOptions().get("aws_endpoint"));
            Assertions.assertEquals(ROWS_A, q1Metadata.getRowCount());
        } finally {
            storeA.release();
            catalog.onClose();
        }
    }

    /**
     * Q1 holds a store built with key ak-1 when the namespace rotates the key to ak-2 and revokes
     * ak-1. Without an expiry no store refreshes its credentials, so Q2 must build its own with
     * ak-2, which is what its BE is handed.
     */
    @Test
    public void testOverlappingReadsKeepTheirOwnCredentialsWithoutAnExpiry() throws Exception {
        LanceExternalCatalog catalog = newCatalog(404, "lance_managed_s3_rotation");
        try {
            vendEndpoint(storeA.endpoint(), "access_key_id", "ak-1");
            storeA.holdNextRequest();
            CompletableFuture<LanceTableMetadata> q1 = CompletableFuture.supplyAsync(
                    () -> catalog.loadTableMetadata("default", TABLE));
            Assertions.assertTrue(storeA.awaitHeld(), "Q1 never reached endpoint A");

            vendEndpoint(storeA.endpoint(), "access_key_id", "ak-2");
            storeA.revokedKey = "ak-1";
            LanceTableMetadata q2 = catalog.loadTableMetadata("default", TABLE);
            Assertions.assertEquals("ak-2", q2.getLanceStorageOptions().get("aws_access_key_id"));
            Assertions.assertEquals(ROWS_A, q2.getRowCount());

            storeA.revokedKey = null;
            storeA.release();
            Assertions.assertEquals(ROWS_A, q1.get(60, TimeUnit.SECONDS).getRowCount());
        } finally {
            storeA.revokedKey = null;
            storeA.release();
            catalog.onClose();
        }
    }

    /**
     * The same move with Q1 finished before Q2 starts, so Q1 has already loaded endpoint A's
     * manifest of version 3 into the catalog Session when Q2 reads that version from endpoint B.
     */
    @Test
    public void testReadAfterAnEndpointChangeReadsTheNewEndpoint() {
        LanceExternalCatalog catalog = newCatalog(402, "lance_managed_s3_sequential");
        try {
            vendEndpoint(storeA.endpoint());
            Assertions.assertEquals(ROWS_A, catalog.loadTableMetadata("default", TABLE).getRowCount());
            vendEndpoint(storeB.endpoint());
            LanceTableMetadata q2 = catalog.loadTableMetadata("default", TABLE);
            Assertions.assertEquals(storeB.endpoint(), q2.getLanceStorageOptions().get("aws_endpoint"));
            Assertions.assertEquals(ROWS_B, q2.getRowCount(), "Q2 must plan from endpoint B, where its BE reads");
        } finally {
            catalog.onClose();
        }
    }

    /**
     * Vended credentials that are already due make the store refresh them while it reads. The
     * refresh describes the table through the namespace the SDK opened with, called back through
     * JNI, and the read goes on with what that describe vends.
     */
    @Test
    public void testDueCredentialsAreRefreshedThroughTheSdkNamespace() {
        LanceExternalCatalog catalog = newCatalog(403, "lance_managed_s3_refresh");
        try {
            vendEndpoint(storeA.endpoint(), "expires_at_millis", String.valueOf(System.currentTimeMillis()));
            int before = describes.get();
            Assertions.assertEquals(ROWS_A, catalog.loadTableMetadata("default", TABLE).getRowCount());
            // The FE's describe, the SDK's own while it opens, and at least one refresh.
            Assertions.assertTrue(describes.get() - before >= 3, "describes: " + (describes.get() - before));
        } finally {
            catalog.onClose();
        }
    }

    /**
     * A vended {@code endpoint} with {@code AWS_ENDPOINT} in the FE environment: the FE must read the
     * vended endpoint, as the BE does. Each read vends one more option, so each builds its own
     * store and folds the options anew.
     */
    @Test
    public void testFeEnvironmentDoesNotOverrideTheVendedEndpoint() {
        CLibrary libc = Native.load("c", CLibrary.class);
        String previous = System.getenv("AWS_ENDPOINT");
        LanceExternalCatalog catalog = newCatalog(401, "lance_managed_s3_env");
        Assertions.assertEquals(0, libc.setenv("AWS_ENDPOINT", storeB.endpoint(), 1));
        try {
            for (int read = 0; read < 8; read++) {
                vendEndpoint(storeA.endpoint(), "doris_test_read", String.valueOf(read));
                LanceTableMetadata metadata = catalog.loadTableMetadata("default", TABLE);
                Assertions.assertEquals(storeA.endpoint(), metadata.getLanceStorageOptions().get("aws_endpoint"));
                Assertions.assertFalse(metadata.getLanceStorageOptions().containsKey("endpoint"));
                Assertions.assertEquals(ROWS_A, metadata.getRowCount(), "read " + read + " left endpoint A");
            }
        } finally {
            if (previous == null) {
                libc.unsetenv("AWS_ENDPOINT");
            } else {
                libc.setenv("AWS_ENDPOINT", previous, 1);
            }
            catalog.onClose();
        }
    }

    private LanceExternalCatalog newCatalog(long id, String name) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put(LanceExternalCatalog.REST_URI, restUri);
        properties.put(LanceExternalCatalog.REST_SECURITY_TYPE, "bearer");
        properties.put(LanceExternalCatalog.REST_BEARER_TOKEN, BEARER_TOKEN);
        return new LanceExternalCatalog(id, name, null, properties, "");
    }

    /** An empty create, then two one-fragment appends of {@code rowsPerAppend} rows each. */
    private static void writeThreeVersions(Path dir, int rowsPerAppend) throws Exception {
        String uri = dir.toUri().toString();
        Schema schema = new Schema(Collections.singletonList(
                new Field("row_id", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
        WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.0").build();
        try (BufferAllocator allocator = new RootAllocator()) {
            try (Dataset created = Dataset.create(allocator, uri, schema, params)) {
                Assertions.assertEquals(1, created.version());
            }
            for (long readVersion = 1; readVersion < 3; readVersion++) {
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    IntVector rowId = (IntVector) root.getVector("row_id");
                    rowId.allocateNew(rowsPerAppend);
                    for (int i = 0; i < rowsPerAppend; i++) {
                        rowId.set(i, i);
                    }
                    rowId.setValueCount(rowsPerAppend);
                    root.setRowCount(rowsPerAppend);
                    List<FragmentMetadata> fragments = Fragment.create(uri, allocator, root, params);
                    try (Dataset committed = new FragmentOperation.Append(fragments)
                            .commit(allocator, uri, Optional.of(readVersion), Collections.emptyMap())) {
                        Assertions.assertEquals(readVersion + 1, committed.version());
                    }
                }
            }
        }
    }

    /** Versions 1 to 3, recorded at the V2 manifest path under the bucket. */
    private static String tableVersionJson(long version) {
        return "{\"version\":" + version + ",\"manifest_path\":\"" + DATASET_KEY + "/_versions/"
                + U64_MAX.subtract(BigInteger.valueOf(version)) + ".manifest\"}";
    }

    private void handleNamespaceRequest(HttpExchange exchange) throws IOException {
        byte[] body = ByteStreams.toByteArray(exchange.getRequestBody());
        String path = exchange.getRequestURI().getPath();
        String query = String.valueOf(exchange.getRequestURI().getQuery());
        int status = 200;
        String response;
        if (!("Bearer " + BEARER_TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
            status = 401;
            response = "{\"error\":\"unauthorized\",\"code\":16}";
        } else if (path.equals("/v1/table/" + TABLE + "/version/list")) {
            boolean descending = query.contains("descending=true");
            Matcher limitMatcher = Pattern.compile("(?:^|&)limit=(\\d+)").matcher(query);
            int limit = limitMatcher.find() ? Integer.parseInt(limitMatcher.group(1)) : 3;
            StringBuilder entries = new StringBuilder();
            for (int i = 0; i < Math.min(limit, 3); i++) {
                entries.append(entries.length() > 0 ? "," : "").append(tableVersionJson(descending ? 3 - i : i + 1));
            }
            response = "{\"versions\":[" + entries + "]}";
        } else if (path.equals("/v1/table/" + TABLE + "/version/describe")) {
            long version = JsonUtil.readTree(new String(body, StandardCharsets.UTF_8)).get("version").asLong();
            response = "{\"version\":" + tableVersionJson(version) + "}";
        } else if (path.equals("/v1/table/" + TABLE + "/describe")) {
            describes.incrementAndGet();
            StringBuilder options = new StringBuilder();
            vendedOptions.forEach((key, value) -> options.append(options.length() > 0 ? "," : "")
                    .append('"').append(key).append("\":\"").append(value).append('"'));
            String location = "s3://" + BUCKET + "/" + DATASET_KEY;
            response = "{\"table\":\"" + TABLE + "\",\"namespace\":[],\"location\":\"" + location
                    + "\",\"table_uri\":\"" + location + "\",\"storage_options\":{" + options
                    + "},\"managed_versioning\":true}";
        } else if (path.endsWith("/table/list")) {
            response = "{\"tables\":[\"" + TABLE + "\"]}";
        } else if (path.endsWith("/list")) {
            response = "{\"namespaces\":[]}";
        } else {
            status = 404;
            response = "{\"error\":\"table not found\",\"code\":4}";
        }
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.close();
    }

    /**
     * Just enough of S3 for Lance to read one version of a dataset by path-style requests: HEAD and
     * whole-object GET, over one bucket kept in a local directory. It can hold the next request
     * until released, to keep a read, and the store it opened, in flight, and reject one access key.
     */
    private static final class S3Stub {
        private static final DateTimeFormatter HTTP_DATE = DateTimeFormatter.RFC_1123_DATE_TIME
                .withZone(ZoneOffset.UTC);

        private final Path root;
        private final HttpServer server;
        private final AtomicBoolean holdNext = new AtomicBoolean();
        private volatile CountDownLatch held = new CountDownLatch(1);
        private volatile CountDownLatch released = new CountDownLatch(1);
        /** The access key it rejects, as if revoked. */
        private volatile String revokedKey;

        private S3Stub(Path root) throws IOException {
            this.root = root;
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(Executors.newCachedThreadPool());
            server.createContext("/", this::handle);
            server.start();
        }

        private String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private void holdNextRequest() {
            held = new CountDownLatch(1);
            released = new CountDownLatch(1);
            holdNext.set(true);
        }

        private boolean awaitHeld() throws InterruptedException {
            return held.await(60, TimeUnit.SECONDS);
        }

        private void release() {
            released.countDown();
        }

        private void handle(HttpExchange exchange) throws IOException {
            try {
                if (holdNext.getAndSet(false)) {
                    CountDownLatch release = released;
                    held.countDown();
                    release.await(60, TimeUnit.SECONDS);
                }
                String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                String revoked = revokedKey;
                if (revoked != null && authorization != null && authorization.contains("Credential=" + revoked + "/")) {
                    exchange.sendResponseHeaders(403, -1);
                    return;
                }
                String path = exchange.getRequestURI().getPath();
                Path file = path.startsWith("/" + BUCKET + "/") ? root.resolve(path.substring(BUCKET.length() + 2))
                        : null;
                if (file == null || !Files.isRegularFile(file)) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                long size = Files.size(file);
                long modified = Files.getLastModifiedTime(file).toMillis();
                exchange.getResponseHeaders().set("Last-Modified", HTTP_DATE.format(Instant.ofEpochMilli(modified)));
                exchange.getResponseHeaders().set("ETag", "\"" + size + "-" + modified + "\"");
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().set("Content-Length", String.valueOf(size));
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                exchange.sendResponseHeaders(200, size);
                try (OutputStream output = exchange.getResponseBody()) {
                    Files.copy(file, output);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(503, -1);
            } finally {
                exchange.close();
            }
        }
    }
}
