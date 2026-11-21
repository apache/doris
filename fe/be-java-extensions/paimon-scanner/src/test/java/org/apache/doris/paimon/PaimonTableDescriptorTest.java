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

package org.apache.doris.paimon;

import org.apache.doris.thrift.TPaimonCatalogContext;
import org.apache.doris.thrift.TPaimonTableDesc;

import com.sun.net.httpserver.HttpServer;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonTableDescriptorTest {
    @TempDir
    Path directory;

    @Test
    public void testPinnedSchemaAndOverwriteOptionsWithoutSchemaFiles() throws Exception {
        TPaimonTableDesc descriptor = descriptor(false);
        TableSchema pinned = JsonSerdeUtil.fromJson(descriptor.getSchemaJson(), TableSchema.class);
        Map<String, String> options = new HashMap<>(pinned.options());
        options.put("scan.fallback-branch", "does-not-exist");
        descriptor.setSchemaJson(JsonSerdeUtil.toJson(pinned.copy(options)));
        FileStoreTable table = resolve(descriptor);
        try {
            Assertions.assertEquals(7, table.schema().id());
            Assertions.assertEquals("42", table.schema().fields().get(1).defaultValue());
            Assertions.assertEquals("false", table.options().get(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key()));
            Assertions.assertEquals("db", table.catalogEnvironment().identifier().getDatabaseName());
            Assertions.assertFalse(table.catalogEnvironment().supportsVersionManagement());
            // There is no schema file to reload: the JSON descriptor is the schema authority.
            Assertions.assertFalse(directory.resolve("schema").toFile().exists());
        } finally {
            table.fileIO().close();
        }
    }

    @Test
    public void testPartitionedPrimaryKeyAndBranchArePreserved() throws Exception {
        TPaimonTableDesc descriptor = descriptor(true).setBranch("dev");
        FileStoreTable table = resolve(descriptor);
        try {
            Assertions.assertEquals(Arrays.asList("id", "region"), table.primaryKeys());
            Assertions.assertEquals(Collections.singletonList("region"), table.partitionKeys());
            Assertions.assertEquals("dev", table.catalogEnvironment().identifier().getBranchName());
            Assertions.assertEquals("dev", CoreOptions.branch(table.options()));
            Assertions.assertEquals("uuid-7", table.catalogEnvironment().uuid());
        } finally {
            table.fileIO().close();
        }
    }

    @Test
    public void testCatalogServicesAreRestoredWithoutLoadingCatalogMetadata() throws Exception {
        TPaimonTableDesc descriptor = descriptor(false);
        descriptor.getCatalogContext().setCatalogLoaderEnabled(true).setSupportsVersionManagement(true)
                .setSupportsPartitionModification(true)
                .setOptions(Collections.singletonMap("metastore", "must-not-be-loaded"));
        FileStoreTable table = resolve(descriptor);
        try {
            Assertions.assertEquals(7, table.schema().id());
            Assertions.assertNotNull(table.catalogEnvironment().catalogLoader());
            Assertions.assertTrue(table.catalogEnvironment().supportsVersionManagement());
            Assertions.assertNotNull(table.catalogEnvironment().snapshotLoader());
            Assertions.assertNull(table.catalogEnvironment().lockFactory());
        } finally {
            table.fileIO().close();
        }
    }

    @Test
    public void testMissingAndCorruptDescriptorsReturnErrors() throws Exception {
        byte[] empty = new TSerializer(new TBinaryProtocol.Factory()).serialize(new TPaimonTableDesc());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonTableDescriptor.resolve(empty, Collections.emptyMap()));
        Assertions.assertThrows(Exception.class,
                () -> PaimonTableDescriptor.resolve(new byte[] {1, 2, 3}, Collections.emptyMap()));
    }

    @Test
    public void testLegacyBackendRejectsNonSerializedTable() {
        String marker = "Paimon table descriptor required: upgrade the BE";
        Assertions.assertThrows(RuntimeException.class, () -> PaimonUtils.deserialize(marker));
    }

    @Test
    public void testRestFileIORefreshesExpiredCredentialsBetweenFiles() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        List<String> paths = Collections.synchronizedList(new ArrayList<>());
        List<String> headers = Collections.synchronizedList(new ArrayList<>());
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            int generation = requests.incrementAndGet();
            paths.add(exchange.getRequestURI().getPath());
            headers.add(exchange.getRequestHeaders().getFirst("X-Writer"));
            GetTableTokenResponse response = new GetTableTokenResponse(
                    Collections.singletonMap("generation", Integer.toString(generation)),
                    System.currentTimeMillis() + 7_200_000L);
            byte[] bytes = RESTApi.toJson(response).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            TPaimonTableDesc descriptor = descriptor(false).setDatabaseName("RemoteDB").setTableName("RemoteTable");
            Map<String, String> restOptions = new HashMap<>();
            restOptions.put("uri", "http://127.0.0.1:" + server.getAddress().getPort());
            restOptions.put("prefix", "writer-test");
            restOptions.put("token.provider", "bear");
            restOptions.put("token", "test-token");
            restOptions.put(RESTTokenFileIO.DATA_TOKEN_ENABLED.key(), "true");
            restOptions.put("header.X-Writer", "rest-writer");
            descriptor.getCatalogContext().setOptions(restOptions);
            // Captured credentials must not replace freshly fetched REST tokens.
            descriptor.setStorageOptions(Collections.singletonMap("generation", "stale"));
            FileStoreTable table = resolve(descriptor);
            try {
                Assertions.assertInstanceOf(RESTTokenFileIO.class, table.fileIO());
                RESTTokenFileIO fileIO = (RESTTokenFileIO) table.fileIO();
                try (PositionOutputStream output = fileIO.newOutputStream(
                        new org.apache.paimon.fs.Path(directory.resolve("first").toUri()), false)) {
                    output.write(1);
                }
                Assertions.assertEquals("1", fileIO.validToken().token().get("generation"));
                // Force expiry without a timing-dependent sleep, as the SDK refreshes before each file.
                Field token = RESTTokenFileIO.class.getDeclaredField("token");
                token.setAccessible(true);
                token.set(fileIO, new RESTToken(fileIO.validToken().token(), 0));
                try (PositionOutputStream output = fileIO.newOutputStream(
                        new org.apache.paimon.fs.Path(directory.resolve("second").toUri()), false)) {
                    output.write(2);
                }
                Assertions.assertEquals("2", fileIO.validToken().token().get("generation"));
                Assertions.assertEquals(2, requests.get());
                Assertions.assertEquals(Arrays.asList("rest-writer", "rest-writer"), headers);
                Assertions.assertEquals(2, paths.size());
                for (String path : paths) {
                    Assertions.assertTrue(path.endsWith("/databases/RemoteDB/tables/RemoteTable/token"), path);
                }
                Assertions.assertArrayEquals(new byte[] {1}, Files.readAllBytes(directory.resolve("first")));
                Assertions.assertArrayEquals(new byte[] {2}, Files.readAllBytes(directory.resolve("second")));
                Assertions.assertEquals(7, table.schema().id());
                Assertions.assertFalse(directory.resolve("schema").toFile().exists());
            } finally {
                table.fileIO().close();
            }
        } finally {
            server.stop(0);
        }
    }

    private FileStoreTable resolve(TPaimonTableDesc descriptor) throws Exception {
        byte[] bytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(descriptor);
        return PaimonTableDescriptor.resolve(bytes, Collections.emptyMap());
    }

    private TPaimonTableDesc descriptor(boolean primaryKey) {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", primaryKey ? "1" : "-1");
        options.put("file.format", "parquet");
        options.put(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false");
        TableSchema schema = new TableSchema(7, Arrays.asList(
                DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "value", DataTypes.INT()).newDefaultValue("42"),
                DataTypes.FIELD(2, "region", DataTypes.INT().notNull())),
                2, primaryKey ? Collections.singletonList("region") : Collections.emptyList(),
                primaryKey ? Arrays.asList("id", "region") : Collections.emptyList(), options, null);
        return new TPaimonTableDesc().setTablePath(directory.toUri().toString())
                .setDatabaseName("db").setTableName("t").setBranch("main")
                .setSchemaJson(JsonSerdeUtil.toJson(schema)).setStorageOptions(Collections.emptyMap())
                .setCatalogContext(new TPaimonCatalogContext()
                        .setOptions(Collections.emptyMap()).setHadoopConfig(Collections.emptyMap())
                        .setTableUuid("uuid-7").setCatalogLoaderEnabled(false)
                        .setSupportsVersionManagement(false).setSupportsPartitionModification(false));
    }
}
