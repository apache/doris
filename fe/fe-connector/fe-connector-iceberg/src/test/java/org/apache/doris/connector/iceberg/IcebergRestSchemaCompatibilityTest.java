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

package org.apache.doris.connector.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class IcebergRestSchemaCompatibilityTest {
    private final ObjectMapper mapper = IcebergRestSchemaCompatibility.objectMapper();

    @Test
    void testDropHighestOfThreeFields() throws Exception {
        assertDropHighestField(3);
    }

    @Test
    void testDropHighestOfTwoFields() throws Exception {
        assertDropHighestField(2);
    }

    private void assertDropHighestField(int count) throws Exception {
        TableMetadata base = metadata(count);
        UpdateTableRequest request = update(base, schema(count - 1));
        String original = UpdateTableRequestParser.toJson(request);
        JsonNode expected = mapper.readTree(original);
        // This is the only wire change: preserve the historical high-water mark, not the surviving maximum ID.
        ((ObjectNode) expected.get("updates").get(0)).put("last-column-id", count);
        JsonNode actual = wire(request);
        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(original, UpdateTableRequestParser.toJson(request));
        Assertions.assertEquals(count - 1, actual.at("/updates/0/schema/fields").size());
        for (int index = 0; index < count - 1; index++) {
            Assertions.assertEquals(index + 1, actual.at("/updates/0/schema/fields/" + index + "/id").asInt());
        }
    }

    @Test
    void testRetainHistoricalIdAfterEarlierDrop() throws Exception {
        TableMetadata base = TableMetadata.buildFrom(metadata(3)).addSchema(schema(2)).build();
        Assertions.assertEquals(3, base.lastColumnId());
        Assertions.assertEquals(3, wire(update(base, schema(1))).at("/updates/0/last-column-id").asInt());
    }

    @Test
    void testAddThenDropWithinOneCommitDoesNotLowerId() throws Exception {
        JsonNode json = wire(update(metadata(3), schema(4), schema(2)));
        Assertions.assertEquals(4, json.at("/updates/0/last-column-id").asInt());
        Assertions.assertEquals(4, json.at("/updates/1/last-column-id").asInt());
    }

    @Test
    void testCreateTracksIdsAcrossSchemaUpdates() throws Exception {
        List<MetadataUpdate> updates = Arrays.asList(new MetadataUpdate.AddSchema(schema(4)),
                new MetadataUpdate.AddSchema(schema(2)));
        JsonNode json = wire(new UpdateTableRequest(UpdateRequirements.forCreateTable(updates), updates));
        Assertions.assertEquals(4, json.at("/updates/0/last-column-id").asInt());
        Assertions.assertEquals(4, json.at("/updates/1/last-column-id").asInt());
    }

    @Test
    void testReplaceRetainsBaseAssignedId() throws Exception {
        TableMetadata base = metadata(3);
        List<MetadataUpdate> updates = Collections.singletonList(new MetadataUpdate.AddSchema(schema(2)));
        UpdateTableRequest request = new UpdateTableRequest(UpdateRequirements.forReplaceTable(base, updates), updates);
        Assertions.assertEquals(3, wire(request).at("/updates/0/last-column-id").asInt());
    }

    @Test
    void testMultiTableCommitKeepsIndependentHighWaterMarks() throws Exception {
        UpdateTableRequest first = update(metadata(3), schema(4), schema(2));
        UpdateTableRequest second = update(metadata(2), schema(1));
        second = UpdateTableRequest.create(TableIdentifier.of("db", "other"), second.requirements(), second.updates());
        CommitTransactionRequest request = new CommitTransactionRequest(Arrays.asList(first, second));
        JsonNode expected = mapper.readTree(CommitTransactionRequestParser.toJson(request));
        ((ObjectNode) expected.at("/table-changes/0/updates/0")).put("last-column-id", 4);
        ((ObjectNode) expected.at("/table-changes/0/updates/1")).put("last-column-id", 4);
        ((ObjectNode) expected.at("/table-changes/1/updates/0")).put("last-column-id", 2);
        Assertions.assertEquals(expected, mapper.readTree(mapper.writeValueAsString(request)));
    }

    @Test
    void testNonSchemaCommitIsUnchanged() throws Exception {
        UpdateTableRequest request = UpdateTableRequest.create(TableIdentifier.of("db", "table"),
                Collections.singletonList(new UpdateRequirement.AssertTableUUID(metadata(3).uuid())),
                Collections.singletonList(new MetadataUpdate.SetProperties(Collections.singletonMap("key", "value"))));
        Assertions.assertEquals(mapper.readTree(UpdateTableRequestParser.toJson(request)), wire(request));
    }

    @Test
    void testConcurrentSchemaChangeStillFailsRequirements() throws Exception {
        UpdateTableRequest request = update(metadata(3), schema(2));
        UpdateTableRequest decoded = UpdateTableRequestParser.fromJson(mapper.writeValueAsString(request));
        TableMetadata concurrent = TableMetadata.buildFrom(metadata(3)).addSchema(schema(4)).build();
        UpdateRequirement fieldIdRequirement = decoded.requirements().stream()
                .filter(requirement -> requirement instanceof UpdateRequirement.AssertLastAssignedFieldId)
                .findFirst().orElseThrow(AssertionError::new);
        Assertions.assertThrows(CommitFailedException.class, () -> fieldIdRequirement.validate(concurrent));
    }

    @Test
    void testOtherRestResponsesKeepSdkCodecsAndUnknownFieldTolerance() throws Exception {
        ConfigResponse config = mapper.readValue("{\"defaults\":{\"key\":\"value\"},"
                + "\"overrides\":{},\"future-field\":true}", ConfigResponse.class);
        Assertions.assertEquals("value", config.defaults().get("key"));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void formatFenceIsAppliedAtomicallyWithRestUpdates(boolean upgrade, boolean envelope) throws Exception {
        assertRestFormatFence(3, upgrade, envelope, null, "append");
    }

    @Test
    void scopedFormatFencePreservesSchemaHighWaterCompatibility() throws Exception {
        assertRestFormatFence(3, false, true, null, "schema");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void procedureRestCommitsRetainTheFormatFence(boolean upgrade) throws Exception {
        assertRestFormatFence(3, upgrade, false, null, "procedure");
    }

    @Test
    void concurrentRestCommitScopesRemainIsolated() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = workers.submit(() -> {
                assertRestFormatFence(2, false, false, entered, "properties");
                return null;
            });
            Future<?> second = workers.submit(() -> {
                assertRestFormatFence(3, true, false, entered, "properties");
                return null;
            });
            first.get(20, TimeUnit.SECONDS);
            second.get(20, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private void assertRestFormatFence(int format, boolean upgrade, boolean envelope, CountDownLatch entered,
            String operation) throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("rest", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/warehouse"));
        catalog.createNamespace(Namespace.of("db"));
        Table stored = catalog.createTable(TableIdentifier.of("db", "table"), schema(3),
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, Integer.toString(format)));
        if ("procedure".equals(operation)) {
            for (int i = 0; i < 2; i++) {
                stored.newFastAppend().appendFile(DataFiles.builder(stored.spec())
                        .withPath("s3://bucket/data/source-" + i + ".parquet")
                        .withRecordCount(1).withFileSizeInBytes(100).build()).commit();
            }
        }
        long originalSnapshot = stored.currentSnapshot() == null ? -1 : stored.currentSnapshot().snapshotId();
        TableOperations real = ((HasTableOperations) stored).operations();
        // REST load responses carry metadata, not the client's previous update list.
        AtomicReference<TableMetadata> server = new AtomicReference<>(TableMetadataParser.fromJson(
                real.current().metadataFileLocation(), TableMetadataParser.toJson(real.current())));
        AtomicReference<UpdateTableRequest> original = new AtomicReference<>();
        AtomicReference<String> originalJson = new AtomicReference<>();
        AtomicReference<JsonNode> transmitted = new AtomicReference<>();
        AtomicBoolean attempted = new AtomicBoolean();
        UpdateTableRequest other = UpdateTableRequest.create(TableIdentifier.of("db", "other"),
                List.of(new UpdateRequirement.AssertTableUUID(metadata(1).uuid())),
                List.of(new MetadataUpdate.SetProperties(Map.of("other", "unchanged"))));
        TableOperations rest = new TableOperations() {
            @Override
            public TableMetadata current() {
                return server.get();
            }

            @Override
            public TableMetadata refresh() {
                return server.get();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata updated) {
                attempted.set(true);
                TableMetadata current = TableMetadata.buildFrom(server.get())
                        .upgradeFormatVersion(upgrade ? 4 : format)
                        .setProperties(Map.of("test.concurrent-commit", "true")).build();
                server.set(current);
                UpdateTableRequest request = new UpdateTableRequest(
                        UpdateRequirements.forUpdateTable(base, updated.changes()), updated.changes());
                original.set(request);
                originalJson.set(UpdateTableRequestParser.toJson(request));
                try {
                    if (entered != null) {
                        entered.countDown();
                        Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
                    }
                    String json;
                    if (envelope) {
                        request = UpdateTableRequest.create(TableIdentifier.of("db", "table"),
                                request.requirements(), request.updates());
                        json = mapper.writeValueAsString(new CommitTransactionRequest(List.of(request, other)));
                        Assertions.assertEquals(mapper.readTree(UpdateTableRequestParser.toJson(other)),
                                mapper.readTree(json).at("/table-changes/1"));
                        request = CommitTransactionRequestParser.fromJson(json).tableChanges().get(0);
                    } else {
                        json = mapper.writeValueAsString(request);
                        request = UpdateTableRequestParser.fromJson(json);
                    }
                    // Capture actual wire JSON: the newer SDK drops the deprecated high-water field on decode.
                    transmitted.set(envelope ? mapper.readTree(json).at("/table-changes/0") : mapper.readTree(json));
                    // Mirror the SDK reference REST server: validate, apply to current metadata, then publish.
                    for (UpdateRequirement requirement : request.requirements()) {
                        requirement.validate(current);
                    }
                    TableMetadata.Builder builder = TableMetadata.buildFrom(current);
                    request.updates().forEach(update -> update.applyTo(builder));
                    server.set(builder.build());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            }

            @Override
            public FileIO io() {
                return real.io();
            }

            @Override
            public String metadataFileLocation(String fileName) {
                return real.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider() {
                return real.locationProvider();
            }

            @Override
            public TableOperations temp(TableMetadata metadata) {
                return real.temp(metadata);
            }
        };
        Table table = new BaseTable(rest, stored.name());
        Runnable commit;
        if ("procedure".equals(operation)) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            ops.table = table;
            IcebergProcedureOps procedures = new IcebergProcedureOps(Map.of(), ops, null);
            commit = () -> procedures.execute(null, new IcebergTableHandle("db", "table"), "rewrite_manifests",
                    Map.of(), null, List.of());
        } else {
            Transaction transaction = IcebergConnectorTransaction.openTransaction(table, new RecordingConnectorContext(),
                    null, false, null, null);
            if ("append".equals(operation)) {
                transaction.newAppend().appendFile(DataFiles.builder(table.spec())
                        .withPath("s3://bucket/data/new.parquet").withRecordCount(1).withFileSizeInBytes(100).build())
                        .commit();
            } else if ("schema".equals(operation)) {
                transaction.updateSchema().deleteColumn("c3").commit();
            } else {
                transaction.updateProperties().set("test.writer", "committed").commit();
            }
            commit = transaction::commitTransaction;
        }
        if (upgrade) {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, commit::run);
            Assertions.assertTrue(failure.getMessage().contains("Cannot downgrade"), failure.getMessage());
            Assertions.assertEquals(originalSnapshot,
                    server.get().currentSnapshot() == null ? -1 : server.get().currentSnapshot().snapshotId());
            Assertions.assertNull(server.get().properties().get("test.writer"));
        } else {
            commit.run();
            if ("append".equals(operation) || "procedure".equals(operation)) {
                Assertions.assertNotNull(server.get().currentSnapshot());
                Assertions.assertNotEquals(originalSnapshot, server.get().currentSnapshot().snapshotId());
            } else if ("schema".equals(operation)) {
                Assertions.assertNull(server.get().schema().findField("c3"));
                Assertions.assertEquals(3, server.get().lastColumnId());
                boolean foundSchema = false;
                for (JsonNode update : transmitted.get().get("updates")) {
                    if ("add-schema".equals(update.path("action").asText())) {
                        foundSchema = true;
                        Assertions.assertEquals(3, update.path("last-column-id").asInt());
                    }
                }
                Assertions.assertTrue(foundSchema);
            } else {
                Assertions.assertEquals("committed", server.get().properties().get("test.writer"));
            }
        }
        Assertions.assertTrue(attempted.get());
        Assertions.assertEquals(upgrade ? 4 : format, server.get().formatVersion());
        Assertions.assertEquals("true", server.get().properties().get("test.concurrent-commit"));
        Assertions.assertEquals("upgrade-format-version", transmitted.get().at("/updates/0/action").asText());
        Assertions.assertEquals(format, transmitted.get().at("/updates/0/format-version").asInt());
        Assertions.assertEquals(originalJson.get(), UpdateTableRequestParser.toJson(original.get()),
                "Serialization must not mutate the original SDK request");
        Assertions.assertEquals(original.get().updates().size(), wire(original.get()).get("updates").size(),
                "The committing thread must not retain a completed or failed commit's format scope");
    }

    private JsonNode wire(UpdateTableRequest request) throws Exception {
        return mapper.readTree(mapper.writeValueAsString(request));
    }

    private static UpdateTableRequest update(TableMetadata base, Schema... schemas) {
        List<MetadataUpdate> updates = new ArrayList<>();
        for (Schema schema : schemas) {
            updates.add(new MetadataUpdate.AddSchema(schema));
        }
        return UpdateTableRequest.create(TableIdentifier.of("db", "table"),
                UpdateRequirements.forUpdateTable(base, updates), updates);
    }

    private static TableMetadata metadata(int count) {
        return TableMetadata.newTableMetadata(schema(count), PartitionSpec.unpartitioned(), "s3://bucket/table",
                Collections.emptyMap());
    }

    private static Schema schema(int count) {
        List<Types.NestedField> fields = new ArrayList<>();
        for (int id = 1; id <= count; id++) {
            fields.add(Types.NestedField.optional(id, "c" + id, Types.IntegerType.get()));
        }
        return new Schema(fields);
    }
}
