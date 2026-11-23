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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMongoScanDesc;
import org.apache.doris.thrift.TMongoSnapshot;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MongoRangePlannerTest {
    private static class Client extends MongoSnapshotPlanTest.Client {
        int probes;
        RuntimeException failure;
        final List<BsonValue> samples = new ArrayList<>();
        TMongoSnapshot sampledSnapshot;
        boolean indexed = true;
        long logicalBytes = 4L * 1024 * 1024 * 1024;
        long sampleBytes = 1024;

        Client() {
            for (int i = 0; i < 100; i++) {
                samples.add(new BsonInt32(i));
            }
        }

        @Override
        public MongoSplitProbe probeSplits(MongoTableHandle table, String field, String preference,
                TMongoSnapshot snapshot, int rows, long timeoutMs, Runnable checkCancelled) {
            probes++;
            sampledSnapshot = snapshot.deepCopy();
            checkCancelled.run();
            if (failure != null) {
                throw failure;
            }
            return new MongoSplitProbe(samples, logicalBytes, sampleBytes, indexed);
        }
    }

    private static MongoScanPlanTest.Session session(ConnectorStatementScope scope, Map<String, String> options) {
        return new MongoScanPlanTest.Session(options) {
            @Override
            public ConnectorStatementScope getStatementScope() {
                return scope;
            }
        };
    }

    private static MongoTableHandle table(MongoCatalogProperties properties, String kind) {
        return new MongoTableHandle("sample", new Document("name", "records").append("type", kind)
                .append("info", new Document("uuid", new Binary((byte) 4, new byte[16]))), properties);
    }

    private static MongoCatalogProperties properties() {
        return MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
    }

    private static Map<String, String> options(String strategy) {
        return Map.of("mongodb_read_concern", "snapshot", "mongodb_split_strategy", strategy,
                "file_split_size", "536870912", "max_file_split_num", "8");
    }

    private static TMongoScanDesc descriptor(org.apache.doris.connector.spi.scan.ConnectorScanRange range) {
        var result = new TTableFormatFileDesc();
        range.populateRangeParams(result, new TFileRangeDesc());
        return result.getMongodbParams();
    }

    @Test
    void familyClassifierPreservesExactTypesFrequencyAndDeterministicTies() {
        var integer = new org.bson.BsonInt64(Long.MIN_VALUE);
        var date = new org.bson.BsonDateTime(Long.MAX_VALUE);
        var objectId = new org.bson.BsonObjectId(new org.bson.types.ObjectId("800000000000000000000000"));
        Assertions.assertEquals("integer", MongoRangePlanner.family(new BsonInt32(1)));
        Assertions.assertEquals("integer", MongoRangePlanner.family(integer));
        Assertions.assertEquals("date", MongoRangePlanner.family(date));
        Assertions.assertEquals("objectId", MongoRangePlanner.family(objectId));
        for (BsonValue unsupported : List.of(org.bson.BsonNull.VALUE, new org.bson.BsonDouble(1.0),
                new org.bson.BsonDecimal128(org.bson.types.Decimal128.parse("1.000")),
                new org.bson.BsonString("value"), org.bson.BsonBoolean.TRUE, new org.bson.BsonArray(),
                new org.bson.BsonDocument(), new org.bson.BsonTimestamp(1, 1))) {
            Assertions.assertEquals("", MongoRangePlanner.family(unsupported));
        }
        Assertions.assertEquals("integer", MongoRangePlanner.family(List.of(integer, new BsonInt32(1), date)));
        Assertions.assertEquals("date", MongoRangePlanner.family(List.of(integer, date, objectId)));
        Assertions.assertEquals("date", MongoRangePlanner.family(List.of(objectId, date, integer)));
        Assertions.assertEquals("", MongoRangePlanner.family(List.<BsonValue>of(org.bson.BsonNull.VALUE)));
    }

    @Test
    void mutatingWireFilterDoesNotCorruptTheMemoizedPlan() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        var first = descriptor(provider.planScan(session, request).get(0));
        byte[] expected = first.getSplit().getFilterBson().clone();
        java.util.Arrays.fill(first.getSplit().getFilterBson(), (byte) 0);
        var fresh = descriptor(provider.planScan(session, request).get(0));
        Assertions.assertArrayEquals(expected, fresh.getSplit().getFilterBson());
        Assertions.assertEquals(1, client.probes);
    }

    @Test
    void rangePlanIsFrozenBeforeAnySplitAndExplainReusesIt() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var table = table(properties, "collection");
        var explanation = provider.getScanNodeProperties(session, table, List.of(), Optional.empty());
        var ranges = provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build());
        Assertions.assertEquals(8, ranges.size());
        Assertions.assertEquals("8", explanation.get("splits_actual"));
        Assertions.assertEquals(1, client.probes);
        for (int i = 0; i < ranges.size(); i++) {
            var d = descriptor(ranges.get(i));
            Assertions.assertEquals(client.sampledSnapshot, d.getSnapshot());
            Assertions.assertEquals(2, d.getVersion());
            Assertions.assertEquals(i, d.getSplit().getIndex());
            Assertions.assertEquals(8, d.getSplit().getActual());
            Assertions.assertEquals("integer", d.getSplit().getTypeFamily());
        }
        Assertions.assertEquals(8, ranges.stream().map(r -> r.getSplitIdentity()).distinct().count());
        client.samples.clear();
        Assertions.assertEquals(8, provider.planScan(session,
                ConnectorScanRequest.builder(table, List.of()).build()).size());
        Assertions.assertEquals(1, client.probes);
    }

    @Test
    void sourceErrorsAreStickyAndNeverBecomeFullRescans() {
        var properties = properties();
        var client = new Client();
        client.failure = new IllegalStateException("fixture planning failure");
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("auto"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        Assertions.assertThrows(IllegalStateException.class, () -> provider.planScan(session, request));
        client.failure = null;
        Assertions.assertThrows(IllegalStateException.class, () -> provider.planScan(session, request));
        Assertions.assertEquals(1, client.probes);
    }

    @Test
    void singleStrategyDoesNotProbeAndExplicitUnsupportedObjectsFail() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var single = session(new MongoSnapshotPlanTest.Scope(), Map.of("mongodb_split_strategy", "single"));
        Assertions.assertEquals(1, provider.planScan(single,
                ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build()).size());
        Assertions.assertEquals(0, client.probes);
        for (String kind : List.of("view", "timeseries")) {
            var explicit = session(new MongoSnapshotPlanTest.Scope(), options("range"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> provider.planScan(explicit,
                    ConnectorScanRequest.builder(table(properties, kind), List.of()).build()));
        }
    }

    @Test
    void explicitRangeRequiresSnapshotAndLiveScope() {
        var properties = properties();
        var provider = new MongoScanPlanProvider(properties, Client::new);
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        Assertions.assertThrows(IllegalArgumentException.class, () -> provider.planScan(
                session(new MongoSnapshotPlanTest.Scope(), Map.of("mongodb_split_strategy", "range")), request));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.planScan(new MongoScanPlanTest.Session(options("range")), request));
    }

    @Test
    void autoFallbackAndExplicitRejectionRetainDecisionReasons() {
        for (String condition : List.of("missing_index", "unknown_complement_cost", "small_collection",
                "unsupported_type", "probe_budget", "empty_sample")) {
            var properties = properties();
            var client = new Client();
            switch (condition) {
                case "missing_index" -> client.indexed = false;
                case "unknown_complement_cost" -> client.logicalBytes = -1;
                case "small_collection" -> client.logicalBytes = 0;
                case "unsupported_type" -> {
                    client.samples.clear();
                    client.samples.add(org.bson.BsonNull.VALUE);
                }
                case "probe_budget" -> client.failure = new MongoRangePlanner.PlanningLimit();
                case "empty_sample" -> client.samples.clear();
                default -> throw new AssertionError(condition);
            }
            var provider = new MongoScanPlanProvider(properties, () -> client);
            var table = table(properties, "collection");
            var request = ConnectorScanRequest.builder(table, List.of()).build();
            var auto = session(new MongoSnapshotPlanTest.Scope(), options("auto"));
            var range = descriptor(provider.planScan(auto, request).get(0));
            Assertions.assertEquals("single", range.getSplit().getStrategy());
            Assertions.assertEquals(condition, range.getSplit().getReason());
            if (!List.of("unknown_complement_cost", "small_collection", "empty_sample").contains(condition)) {
                var explicit = session(new MongoSnapshotPlanTest.Scope(), options("range"));
                Assertions.assertThrows(IllegalArgumentException.class, () -> provider.planScan(explicit, request));
            }
        }
    }

    @Test
    void budgetsBoundAllScansOfOneStatementAndSourceAndRefundUnusedRanges() {
        var budget = new MongoRangePlanner.Budget();
        Assertions.assertEquals(255, budget.admit("source-a", 255, 1));
        Assertions.assertEquals(1, budget.admit("source-a", 8, 1));
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> budget.admit("source-a", 1, 1));
        budget.refund("source-a", 7, 1);
        Assertions.assertEquals(7, budget.admit("source-a", 8, 1));
        for (int i = 1; i < 16; i++) {
            Assertions.assertEquals(256, budget.admit("source-" + i, 256, 1));
        }
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> budget.admit("source-next", 1, 1));
        budget.sampled(1024 * 1024);
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> budget.sampled(1024 * 1024 + 1));
        for (int i = 1; i < 16; i++) {
            budget.sampled(1024 * 1024);
        }
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> budget.sampled(1));
    }

    @Test
    void streamingFreezesOnceAndNeverReprobesAfterPublishing() throws Exception {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        try (var source = provider.streamSplits(session, request)) {
            Assertions.assertEquals(0, client.probes);
            Assertions.assertTrue(source.hasNext());
            var first = descriptor(source.next());
            client.failure = new IllegalStateException("must never re-probe");
            Assertions.assertTrue(source.hasNext());
            var second = descriptor(source.next());
            Assertions.assertEquals(first.getSnapshot(), second.getSnapshot());
            Assertions.assertEquals(1, client.probes);
            source.close();
            Assertions.assertFalse(source.hasNext());
            Assertions.assertThrows(java.util.NoSuchElementException.class, source::next);
        }
    }

    @Test
    void customFieldDoesNotSilentlyApplyToMultipleScans() {
        var properties = properties();
        var client = new Client();
        var scope = new MongoSnapshotPlanTest.Scope();
        var session = session(scope, Map.of("mongodb_read_concern", "snapshot", "mongodb_split_strategy", "range",
                "max_file_split_num", "8", "mongodb_split_field", "sequence"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        var first = new MongoScanPlanProvider(properties, () -> client);
        first.planScan(session, request);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MongoScanPlanProvider(properties, () -> client).planScan(session, request));
    }

    @Test
    void unsafeFieldsAndMalformedPoliciesFailBeforeProbing() {
        var properties = properties();
        var client = new Client();
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        for (var setting : Map.of("file_split_size", "invalid", "max_file_split_size", "0",
                "max_file_split_num", "not-a-number", "max_initial_file_split_size", "-1",
                "mongodb_split_field", "nested.value", "mongodb_split_strategy", "invalid").entrySet()) {
            var options = new java.util.HashMap<>(options("range"));
            options.put(setting.getKey(), setting.getValue());
            if (setting.getKey().equals("max_file_split_size")
                    || setting.getKey().equals("max_initial_file_split_size")) {
                options.put("file_split_size", "0");
            }
            Assertions.assertThrows(IllegalArgumentException.class, () -> new MongoScanPlanProvider(properties,
                    () -> client).planScan(session(new MongoSnapshotPlanTest.Scope(), options), request));
        }
        Assertions.assertEquals(0, client.probes);
    }

    @Test
    void oneProviderStillPlansDistinctNamespacesIndependently() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        provider.planScan(session, ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build());
        var another = new MongoTableHandle("sample", new Document("name", "another_records")
                .append("type", "collection").append("info", new Document("uuid", new Binary((byte) 4, new byte[16]))),
                properties);
        provider.planScan(session, ConnectorScanRequest.builder(another, List.of()).build());
        Assertions.assertEquals(2, client.probes);
    }

    @Test
    void autoCanFallBackBeforePublicationWhenAllProbeSlotsAreBusy() throws Exception {
        var properties = properties();
        var entered = new java.util.concurrent.CountDownLatch(2);
        var release = new java.util.concurrent.CountDownLatch(1);
        var blocked = new Client() {
            @Override
            public MongoSplitProbe probeSplits(MongoTableHandle table, String field, String preference,
                    TMongoSnapshot snapshot, int rows, long timeoutMs, Runnable checkCancelled) {
                entered.countDown();
                try {
                    if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new AssertionError("Probe fixture was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException();
                }
                return super.probeSplits(table, field, preference, snapshot, rows, timeoutMs, checkCancelled);
            }
        };
        var scope = new MongoSnapshotPlanTest.Scope();
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> new MongoScanPlanProvider(properties, () -> blocked)
                    .planScan(session(scope, options("range")), request));
            var second = executor.submit(() -> new MongoScanPlanProvider(properties, () -> blocked)
                    .planScan(session(scope, options("range")), request));
            Assertions.assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
            var policy = new java.util.HashMap<>(options("auto"));
            // A short injected budget checks contention without waiting out the production two-second cap.
            var plan = new MongoScanPlanProvider(properties, Client::new, 25).planScan(session(scope, policy), request);
            Assertions.assertEquals("probe_budget", descriptor(plan.get(0)).getSplit().getReason());
            release.countDown();
            Assertions.assertEquals(8, first.get(5, java.util.concurrent.TimeUnit.SECONDS).size());
            Assertions.assertEquals(8, second.get(5, java.util.concurrent.TimeUnit.SECONDS).size());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test
    void explicitFieldRestrictionAlsoCountsAutoFallbackScans() {
        var properties = properties();
        var scope = new MongoSnapshotPlanTest.Scope();
        var session = session(scope, Map.of("mongodb_read_concern", "majority", "mongodb_split_strategy", "auto",
                "max_file_split_num", "8", "mongodb_split_field", "sequence"));
        new MongoScanPlanProvider(properties, Client::new).planScan(session,
                ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MongoScanPlanProvider(properties, Client::new).planScan(session,
                        ConnectorScanRequest.builder(table(properties, "view"), List.of()).build()));
    }

    @Test
    void explicitIdRetainsTheDefaultFieldContractAcrossMultipleScans() {
        for (String strategy : List.of("auto", "range")) {
            var properties = properties();
            var policy = new java.util.HashMap<>(options(strategy));
            policy.put("mongodb_split_field", "_id");
            var session = session(new MongoSnapshotPlanTest.Scope(), policy);
            var provider = new MongoScanPlanProvider(properties, Client::new);
            for (String name : List.of("records", "other_records")) {
                var table = new MongoTableHandle("sample", new Document("name", name).append("type", "collection")
                        .append("info", new Document("uuid", new Binary((byte) 4, new byte[16]))), properties);
                var ranges = provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build());
                Assertions.assertTrue(ranges.size() > 1);
                Assertions.assertEquals("_id", descriptor(ranges.get(0)).getSplit().getField());
            }
        }
    }

    @Test
    void negativeExplicitSplitSizeCannotSelectAutomaticSizing() {
        var session = session(new MongoSnapshotPlanTest.Scope(), Map.of("file_split_size", "-1"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> MongoRangePlanner.targetBytes(session, -1));
    }

    @Test
    void completedRangeMemoPreservesItsOutcomeAfterTheStatementDeadline() throws Exception {
        var properties = properties();
        var scope = new MongoSnapshotPlanTest.Scope();
        var session = session(scope, options("range"));
        MongoSnapshot.prepare(session, new ConnectorScanContext(), properties, table(properties, "collection"),
                Client::new, "snapshot", "primary", 30000);
        Object state = scope.getIfPresent("mongodb.statement.snapshot");
        var deadline = state.getClass().getDeclaredField("deadlineNanos");
        deadline.setAccessible(true);
        deadline.setLong(state, System.nanoTime() - TimeUnit.SECONDS.toNanos(1));
        var type = Class.forName(MongoRangePlanner.class.getName() + "$Memo");
        var constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        var get = type.getDeclaredMethod("get", org.apache.doris.connector.spi.ConnectorSession.class,
                ConnectorScanContext.class, java.util.function.Supplier.class);
        get.setAccessible(true);
        var plan = MongoSplitPlan.single(1, 1, "fixture");
        var context = new ConnectorScanContext();
        var failure = new org.apache.doris.connector.spi.DorisConnectorException("fixture source failure");
        Assertions.assertAll(
                () -> Assertions.assertSame(plan, get.invoke(constructor.newInstance(), session, context,
                        (java.util.function.Supplier<MongoSplitPlan>) () -> plan)),
                () -> {
                    var wrapper = Assertions.assertThrows(java.lang.reflect.InvocationTargetException.class,
                            () -> get.invoke(constructor.newInstance(), session, context,
                                    (java.util.function.Supplier<MongoSplitPlan>) () -> {
                                        throw failure;
                                    }));
                    Assertions.assertSame(failure, wrapper.getCause());
                });
    }

    @Test
    void wideDescriptorsCannotMultiplyIntoAnOversizedSplitRpc() throws Exception {
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.username", "fixture_reader", "mongodb.password", "fixture测".repeat(200000)));
        var client = new Client();
        var policy = new java.util.HashMap<>(options("range"));
        policy.put("max_file_split_num", "256");
        policy.put("file_split_size", "1");
        var ranges = new MongoScanPlanProvider(properties, () -> client).planScan(
                session(new MongoSnapshotPlanTest.Scope(), policy),
                ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build());
        var first = descriptor(ranges.get(0));
        int encoded = new org.apache.thrift.TSerializer(new org.apache.thrift.protocol.TBinaryProtocol.Factory())
                .serialize(first).length;
        Assertions.assertTrue((long) encoded * ranges.size() <= 8L * 1024 * 1024,
                "Repeated descriptor metadata must fit within the per-plan RPC budget");
        Assertions.assertEquals(256, first.getSplit().getRequested());
        Assertions.assertTrue(first.getSplit().getAdmitted() < 256);
    }

    @Test
    void descriptorByteAdmissionIsSharedBySourceAndStatementAndRefundedExactly() {
        var budget = new MongoRangePlanner.Budget();
        long megabyte = 1024 * 1024;
        Assertions.assertEquals(32, budget.admit("source-a", 256, megabyte));
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> budget.admit("source-a", 1, 1));
        budget.refund("source-a", 16, megabyte);
        Assertions.assertEquals(16, budget.admit("source-a", 32, megabyte));
        Assertions.assertEquals(32, budget.admit("source-b", 256, megabyte));
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> budget.admit("source-c", 1, 1));
        budget.refund("source-b", 1, megabyte);
        Assertions.assertEquals(1, budget.admit("source-c", 8, megabyte));
    }

    @Test
    void independentScanNodesAdmitTheirFinalCandidates() {
        var column = new Document("name", "text").append("path", List.of("text"))
                .append("doris_type", "STRING").append("bson_types", List.of("string"));
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.type_mismatch", "null", "mongodb.schema",
                new Document("sample.records", new Document("columns", List.of(column))).toJson()));
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var policy = new java.util.HashMap<>(options("range"));
        policy.put("max_file_split_num", "32");
        policy.put("file_split_size", "1");
        var session = session(new MongoSnapshotPlanTest.Scope(), policy);
        var table = table(properties, "collection");
        provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build()).get(0);
        var filter = new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.EQ,
                new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("text",
                        org.apache.doris.connector.spi.ConnectorType.of("STRING")),
                org.apache.doris.connector.spi.pushdown.ConnectorLiteral.ofString("v".repeat(60000)));
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET,
                new MongoPredicateTranslator(table.getSchema(), true).translate(filter).kind());
        var independent = new MongoScanPlanProvider(properties, () -> client);
        var range = descriptor(independent.planScan(session,
                ConnectorScanRequest.builder(table, List.of()).filter(Optional.of(filter)).build()).get(0));
        Assertions.assertTrue(range.getFilterBson().length > 60000);
        Assertions.assertEquals(2, client.probes);
    }

    @Test
    void cancelledPlanningPublishesNothing() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var context = new ConnectorScanContext();
        context.cancel();
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of())
                .scanContext(context).build();
        Assertions.assertThrows(CancellationException.class, () -> provider.planScan(
                session(new MongoSnapshotPlanTest.Scope(), options("range")), request));
        Assertions.assertEquals(0, client.probes);
    }

    @Test
    void commonSplitSizeAndNumberLimitDriveRangePlanning() {
        var properties = properties();
        var client = new Client();
        var policy = Map.of("mongodb_read_concern", "snapshot", "mongodb_split_strategy", "range",
                "file_split_size", "536870912", "max_file_split_num", "3");
        var ranges = new MongoScanPlanProvider(properties, () -> client).planScan(
                session(new MongoSnapshotPlanTest.Scope(), policy),
                ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build());
        Assertions.assertEquals(3, ranges.size());
        Assertions.assertEquals(3, descriptor(ranges.get(0)).getSplit().getRequested());
    }

    @Test
    void propertiesDoNotSelectSnapshotOrProbeBeforeTheFinalRequest() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var table = table(properties, "collection");
        var explanation = provider.getScanNodePropertiesResult(session, table, List.of(), Optional.empty())
                .getProperties();
        Assertions.assertEquals(0, client.identities);
        Assertions.assertEquals(0, client.selections);
        Assertions.assertEquals(0, client.probes);
        provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build()).get(0);
        Assertions.assertEquals("1700000000:7", explanation.get("snapshot"));
        Assertions.assertEquals(1, client.probes);
    }

    @Test
    void staticAndAbsentPropertiesDoNotReadStatementDiagnostics() {
        var reads = new AtomicInteger();
        var scope = new MongoSnapshotPlanTest.Scope() {
            @Override
            public <T> T getIfPresent(String key) {
                reads.incrementAndGet();
                return super.getIfPresent(key);
            }
        };
        var properties = properties();
        var provider = new MongoScanPlanProvider(properties, Client::new);
        var session = session(scope, options("range"));
        var table = table(properties, "collection");
        var values = provider.getScanNodeProperties(session, table, List.of(), Optional.empty());
        Assertions.assertEquals("snapshot", values.get("read_concern"));
        Assertions.assertEquals("collection", values.get("object_kind"));
        Assertions.assertNull(values.get("file_format_type"));
        Assertions.assertNull(values.get(null));
        Assertions.assertEquals(0, reads.get());
        provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build());
        for (var entry : values.entrySet()) {
            Assertions.assertEquals(entry.getValue(), values.get(entry.getKey()), entry.getKey());
        }
    }

    @Test
    void ordinaryLargePredicateIsAdmittedAfterEarlyProperties() {
        var column = new Document("name", "text").append("path", List.of("text"))
                .append("doris_type", "STRING").append("bson_types", List.of("string"));
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.type_mismatch", "null", "mongodb.schema",
                new Document("sample.records", new Document("columns", List.of(column))).toJson()));
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var table = table(properties, "collection");
        var earlyProperties = provider.getScanNodeProperties(session, table, List.of(), Optional.empty());
        var filter = new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.EQ,
                new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("text",
                        org.apache.doris.connector.spi.ConnectorType.of("STRING")),
                org.apache.doris.connector.spi.pushdown.ConnectorLiteral.ofString("v".repeat(60000)));
        Assertions.assertEquals(MongoPredicateTranslator.Kind.SUPERSET,
                new MongoPredicateTranslator(table.getSchema(), true).translate(filter).kind());
        var ranges = provider.planScan(session,
                ConnectorScanRequest.builder(table, List.of()).filter(Optional.of(filter)).build());
        Assertions.assertTrue(descriptor(ranges.get(0)).getFilterBson().length > 60000);
        Assertions.assertEquals("SUPERSET", earlyProperties.get("predicate_pushdown"));
        Assertions.assertEquals(1, client.probes);
    }

    @Test
    void cachedPropertiesUseTheFinalDocumentProjection() {
        var properties = properties();
        var provider = new MongoScanPlanProvider(properties, Client::new);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var table = table(properties, "collection");
        var early = provider.getScanNodeProperties(session, table, List.of(), Optional.empty());
        Assertions.assertEquals("true", early.get("projection_pushdown"));
        var column = new MongoColumnHandle(table.getSchema().getBindings().get(0), table.getSchema().getVersion());
        var range = descriptor(provider.planScan(session, ConnectorScanRequest.builder(table, List.of(column)).build())
                .get(0));
        Assertions.assertFalse(range.isSetProjectionBson());
        Assertions.assertEquals("false", early.get("projection_pushdown"));
    }

    @Test
    void explainReadsOneConsistentPropertiesSnapshot() {
        var properties = properties();
        var provider = new MongoScanPlanProvider(properties, Client::new);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var base = new java.util.LinkedHashMap<>(provider.getScanNodeProperties(session,
                table(properties, "collection"), List.of(), Optional.empty()));
        var reads = new AtomicInteger();
        var changing = new java.util.AbstractMap<String, String>() {
            @Override
            public java.util.Set<Entry<String, String>> entrySet() {
                var current = new java.util.LinkedHashMap<>(base);
                String generation = Integer.toString(reads.incrementAndGet());
                current.put("read_concern", generation);
                current.put("split_reason", generation);
                return current.entrySet();
            }
        };
        var output = new StringBuilder();
        provider.appendExplainInfo(output, "", changing);
        Assertions.assertEquals(1, reads.get());
        Assertions.assertTrue(output.toString().contains("read concern: 1;"));
        Assertions.assertTrue(output.toString().contains("reason: 1"));
    }

    @Test
    void emptySampleFallbacksCannotEscapePinnedSourcePartitionLimit() {
        var properties = properties();
        var client = new Client();
        client.samples.clear();
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        for (int index = 0; index < 256; index++) {
            var ranges = new MongoScanPlanProvider(properties, () -> client).planScan(session, request);
            Assertions.assertEquals(1, ranges.size());
        }
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> new MongoScanPlanProvider(properties, () -> client).planScan(session, request));
    }

    @Test
    void successfulFallbackDescriptorsStillConsumeSourceByteBudget() {
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.username", "fixture_reader", "mongodb.password", "p".repeat(1024 * 1024)));
        var client = new Client();
        client.samples.clear();
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        for (int index = 0; index < 31; index++) {
            Assertions.assertEquals(1, new MongoScanPlanProvider(properties, () -> client)
                    .planScan(session, request).size());
        }
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> new MongoScanPlanProvider(properties, () -> client).planScan(session, request));
    }

    @Test
    void sharedSizeHeuristicsAndNonpositiveUserCapsKeepTheirExistingMeaning() {
        var scope = new MongoSnapshotPlanTest.Scope();
        var defaults = session(scope, Map.of());
        Assertions.assertEquals(32L * 1024 * 1024, MongoRangePlanner.targetBytes(defaults, 1024));
        Assertions.assertEquals(64L * 1024 * 1024,
                MongoRangePlanner.targetBytes(defaults, 64L * 1024 * 1024 * 200));
        Assertions.assertEquals(64L * 1024 * 1024, MongoRangePlanner.targetBytes(defaults, -1));
        Assertions.assertEquals(256, MongoRangePlanner.splitLimit(defaults));
        for (String unlimited : List.of("0", "-1")) {
            Assertions.assertEquals(256, MongoRangePlanner.splitLimit(
                    session(scope, Map.of("max_file_split_num", unlimited))));
        }
        var huge = session(scope, Map.of("file_split_size", Long.toString(Long.MAX_VALUE)));
        Assertions.assertEquals(Long.MAX_VALUE, MongoRangePlanner.targetBytes(huge, Long.MAX_VALUE));
        var threshold = session(scope, Map.of("max_file_split_size", Long.toString(Long.MAX_VALUE),
                "max_initial_file_split_size", "1048576", "max_initial_file_split_num", "200"));
        Assertions.assertEquals(1048576, MongoRangePlanner.targetBytes(threshold, Long.MAX_VALUE));
    }

    @Test
    void singleStrategyRemainsExemptButEveryAutoFallbackIsAdmitted() {
        var properties = properties();
        var client = new Client();
        var scope = new MongoSnapshotPlanTest.Scope();
        var single = session(scope, Map.of("mongodb_split_strategy", "single"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        for (int index = 0; index < 300; index++) {
            Assertions.assertEquals(1, new MongoScanPlanProvider(properties, () -> client)
                    .planScan(single, request).size());
        }
        var auto = session(scope, Map.of("mongodb_split_strategy", "auto", "mongodb_read_concern", "majority"));
        for (int index = 0; index < 256; index++) {
            Assertions.assertEquals(1, new MongoScanPlanProvider(properties, () -> client)
                    .planScan(auto, request).size());
        }
        Assertions.assertThrows(org.apache.doris.connector.spi.DorisConnectorException.class,
                () -> new MongoScanPlanProvider(properties, () -> client).planScan(auto, request));
        Assertions.assertEquals(0, client.probes);
    }

    @Test
    void offlineAutoFallbackNeedsNoPinnedSnapshotOrQueryDeadline() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var session = session(ConnectorStatementScope.NONE,
                Map.of("mongodb_split_strategy", "auto", "mongodb_read_concern", "majority"));
        var ranges = provider.planScan(session,
                ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build());
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("snapshot_required", descriptor(ranges.get(0)).getSplit().getReason());
        Assertions.assertEquals(0, client.probes);
    }

    @Test
    void repeatedTimeseriesFallbacksAdmitTheirCompleteSnapshotPayload() throws Exception {
        var columns = new ArrayList<Document>();
        for (int i = 0; i < 900; i++) {
            columns.add(new Document("name", String.format("c%04d", i)).append("path", List.of("v"))
                    .append("bson_types", List.of("long")).append("doris_type", "BIGINT"));
        }
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.schema", new Document("sample.records", new Document("columns", columns)).toJson()));
        var table = new MongoTableHandle("sample", new Document("name", "records").append("type", "timeseries")
                .append("options", new Document("timeseries", new Document("timeField", "t".repeat(15000)))),
                properties);
        var session = session(new MongoSnapshotPlanTest.Scope(), options("auto"));
        var request = ConnectorScanRequest.builder(table, List.of()).build();
        var serializer = new org.apache.thrift.TSerializer(new org.apache.thrift.protocol.TBinaryProtocol.Factory());
        long wireBytes = 0;
        boolean limited = false;
        for (int i = 0; i < 256; i++) {
            try {
                var ranges = new MongoScanPlanProvider(properties, Client::new).planScan(session, request);
                wireBytes += serializer.serialize(descriptor(ranges.get(0))).length;
                Assertions.assertTrue(wireBytes <= 32L * 1024 * 1024,
                        "Serialized fallback descriptors exceeded the pinned-source byte budget");
            } catch (org.apache.doris.connector.spi.DorisConnectorException limit) {
                Assertions.assertTrue(limit.getMessage().contains("budget exhausted"));
                limited = true;
                break;
            }
        }
        Assertions.assertTrue(limited);
    }

    @Test
    void customFieldWithoutInternalBoundsPublishesOneSnapshotDescriptor() {
        var properties = properties();
        var client = new Client();
        var policy = new java.util.HashMap<>(options("range"));
        policy.put("max_file_split_num", "2");
        policy.put("mongodb_split_field", "sequence");
        var session = session(new MongoSnapshotPlanTest.Scope(), policy);
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var table = table(properties, "collection");
        var ranges = provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build());
        Assertions.assertEquals(1, ranges.size());
        var scan = descriptor(ranges.get(0));
        Assertions.assertEquals(1, scan.getVersion());
        Assertions.assertEquals("single", scan.getSplit().getStrategy());
        Assertions.assertEquals("insufficient_boundaries", scan.getSplit().getReason());
        Assertions.assertEquals(client.time.getTime(), scan.getSnapshot().getTimestampSeconds());
        Assertions.assertEquals("1", provider.getScanNodeProperties(session, table, List.of(), Optional.empty())
                .get("splits_actual"));
        Assertions.assertEquals(1, client.probes);
    }

    @Test
    void assertionErrorCompletesTheSharedRangePlanForEveryWaiter() throws Exception {
        assertFatalRangeFailure(new AssertionError("controlled range failure"));
    }

    @Test
    void outOfMemoryErrorCompletesTheSharedRangePlanForEveryWaiter() throws Exception {
        assertFatalRangeFailure(new OutOfMemoryError("controlled range failure"));
    }

    private static void assertFatalRangeFailure(Error fatal) throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var waiting = new CountDownLatch(1);
        var loads = new AtomicInteger();
        var client = new Client() {
            @Override
            public MongoSplitProbe probeSplits(MongoTableHandle table, String field, String preference,
                    TMongoSnapshot snapshot, int rows, long timeoutMs, Runnable checkCancelled) {
                loads.incrementAndGet();
                entered.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException();
                }
                throw fatal;
            }
        };
        var properties = properties();
        var session = session(new MongoSnapshotPlanTest.Scope(), options("range"));
        var request = ConnectorScanRequest.builder(table(properties, "collection"), List.of()).build();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var owner = executor.submit(() -> provider.planScan(session, request));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            var waiter = executor.submit(() -> {
                waiting.countDown();
                return provider.planScan(session, request);
            });
            Assertions.assertTrue(waiting.await(5, TimeUnit.SECONDS));
            release.countDown();
            Assertions.assertSame(fatal, Assertions.assertThrows(ExecutionException.class,
                    () -> owner.get(10, TimeUnit.SECONDS)).getCause());
            Assertions.assertSame(fatal, Assertions.assertThrows(ExecutionException.class,
                    () -> waiter.get(10, TimeUnit.SECONDS)).getCause());
            Assertions.assertSame(fatal, Assertions.assertThrows(Error.class,
                    () -> provider.planScan(session, request)));
            Assertions.assertEquals(1, loads.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

}
