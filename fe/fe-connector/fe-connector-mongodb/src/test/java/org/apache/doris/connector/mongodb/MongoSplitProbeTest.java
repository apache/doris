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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.thrift.TMongoSnapshot;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

class MongoSplitProbeTest {
    private static final long PROBE_TIMEOUT_MS = 1000;

    private static final class Driver {
        final ConnectorScanContext context = new ConnectorScanContext();
        final List<TimeoutMode> modes = new ArrayList<>();
        final List<Long> timeouts = new ArrayList<>();
        List<Document> indexes = List.of(new Document("name", "sequence_1")
                .append("key", new Document("sequence", 1)));
        Document projection;
        Document aggregate;
        BsonTimestamp pin;
        Document statistics = new Document("size", 4L * 1024 * 1024 * 1024);
        int limit;
        int batchSize;
        int indexBatchSize;
        int dataCommands;
        int statisticsCommands;
        int indexesRead;
        int indexesReceived;
        int initialIndexBatchLength = Integer.MAX_VALUE;
        int indexBatchLength = Integer.MAX_VALUE;
        int indexReplies;
        int indexHasNextCalls;
        int getMoreCalls;
        int expireAfterIndexReply;
        int cancelAfterIndexReply;
        int cursorsClosed;
        long chargedBytes;
        int sessionsStarted;
        boolean cancelAfterFirst;
        boolean sourceFailure;
        boolean closeQueryAfterFirst;
        final java.util.concurrent.atomic.AtomicBoolean queryClosed = new java.util.concurrent.atomic.AtomicBoolean();

        void receiveIndexes(int count) throws InterruptedException {
            indexesReceived += Math.min(count, indexes.size() - indexesReceived);
            indexReplies++;
            if (indexReplies == cancelAfterIndexReply) {
                context.cancel();
            }
            if (indexReplies == expireAfterIndexReply) {
                // The reply is buffered before this pause; oversleeping still preserves the trigger.
                Thread.sleep(PROBE_TIMEOUT_MS + 100);
            }
        }

        @SuppressWarnings("unchecked")
        <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
        }

        MongoClient client() {
            MongoCursor<?> indexCursor = proxy(MongoCursor.class, (p, method, args) -> switch (method.getName()) {
                case "hasNext" -> {
                    indexHasNextCalls++;
                    if (indexesRead == indexesReceived && indexesReceived < indexes.size()) {
                        getMoreCalls++;
                        receiveIndexes(indexBatchLength);
                    }
                    yield indexesRead < indexesReceived;
                }
                case "next" -> {
                    Assertions.assertTrue(indexesRead < indexesReceived);
                    yield indexes.get(indexesRead++);
                }
                case "available" -> indexesReceived - indexesRead;
                case "close" -> {
                    cursorsClosed++;
                    yield null;
                }
                default -> throw new UnsupportedOperationException(method.getName());
            });
            ListIndexesIterable<?> list = proxy(ListIndexesIterable.class, (p, method, args) -> {
                if ("iterator".equals(method.getName())) {
                    receiveIndexes(initialIndexBatchLength);
                    return indexCursor;
                }
                if ("timeoutMode".equals(method.getName())) {
                    modes.add((TimeoutMode) args[0]);
                }
                if ("batchSize".equals(method.getName())) {
                    indexBatchSize = (int) args[0];
                }
                return p;
            });
            MongoCollection<?> collection = proxy(MongoCollection.class, (p, method, args) -> {
                if (method.getName().equals("listIndexes")) {
                    return list;
                }
                throw new UnsupportedOperationException(method.getName());
            });
            MongoDatabase database = proxy(MongoDatabase.class, (p, method, args) -> {
                if (method.getName().startsWith("withRead")) {
                    return p;
                }
                return switch (method.getName()) {
                    case "getCollection" -> collection;
                    case "withTimeout" -> {
                        timeouts.add((long) args[0]);
                        yield p;
                    }
                    case "runCommand" -> {
                        var command = (Document) args[0];
                        if (command.containsKey("collStats")) {
                            statisticsCommands++;
                            yield statistics;
                        }
                        aggregate = command;
                        dataCommands++;
                        var stages = command.getList("pipeline", Document.class);
                        limit = stages.get(0).get("$sample", Document.class).getInteger("size");
                        projection = stages.get(1).get("$project", Document.class);
                        batchSize = command.get("cursor", Document.class).getInteger("batchSize");
                        pin = command.get("readConcern", Document.class).get("atClusterTime", BsonTimestamp.class);
                        if (sourceFailure) {
                            throw new MongoCommandException(new Document("ok", 0).append("code", 239)
                                    .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
                        }
                        if (closeQueryAfterFirst) {
                            queryClosed.set(true);
                        }
                        if (cancelAfterFirst) {
                            context.cancel();
                        }
                        yield new BsonDocument("cursor", new BsonDocument("id", new BsonInt64(0))
                                .append("firstBatch", new BsonArray(List.of(new BsonDocument("values",
                                        new BsonArray(List.of(new BsonInt64(9007199254740993L))))))));
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            });
            return proxy(MongoClient.class, (p, method, args) -> switch (method.getName()) {
                case "getDatabase" -> database;
                case "startSession" -> {
                    sessionsStarted++;
                    throw new AssertionError("The sample carries T in its single exhausted command");
                }
                case "close" -> null;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }
    }

    private static MongoSplitProbe run(Driver driver) {
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
        var table = new MongoTableHandle("sample", new Document("name", "records")
                .append("type", "collection"), properties);
        var snapshot = new TMongoSnapshot();
        snapshot.setTimestampSeconds(123);
        snapshot.setTimestampIncrement(456);
        try (var catalog = new MongoDriverCatalogClient(driver.client(), properties)) {
            var session = new MongoScanPlanTest.Session() {
                @Override
                public org.apache.doris.connector.spi.ConnectorTaskScheduler getTaskScheduler(
                        org.apache.doris.connector.spi.ConnectorTaskScheduler.Stage stage) {
                    return driver.proxy(org.apache.doris.connector.spi.ConnectorTaskScheduler.class,
                            (proxy, method, args) -> {
                                if ("isClosed".equals(method.getName())) {
                                    return driver.queryClosed.get();
                                }
                                throw new UnsupportedOperationException(method.getName());
                            });
                }
            };
            return catalog.probeSplits(table, "sequence", "primary", snapshot, 16, PROBE_TIMEOUT_MS,
                    size -> true, bytes -> driver.chargedBytes += bytes,
                    () -> MongoRangePlanner.checkCancelled(session, driver.context));
        }
    }

    @Test
    void firstIndexBatchIsChargedBeforeItsDeadlineFallback() {
        var driver = new Driver();
        driver.indexes = List.of(new Document("name", "sequence_1").append("key", new Document("sequence", 1)),
                new Document("name", "partial_other").append("key", new Document("other", 1))
                        .append("partialFilterExpression", new Document("text", "x".repeat(75000))));
        driver.expireAfterIndexReply = 1;
        long expected = driver.indexes.stream().mapToLong(d -> MongoSplitPlan.encode(d.toBsonDocument()).length).sum();
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertEquals(expected, driver.chargedBytes);
        Assertions.assertEquals(2, driver.indexesRead);
        Assertions.assertEquals(1, driver.indexReplies);
        Assertions.assertEquals(0, driver.getMoreCalls);
        Assertions.assertEquals(0, driver.statisticsCommands);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void expiredProbeCannotFetchAnUnbufferedIndexBatch() {
        var driver = new Driver();
        driver.initialIndexBatchLength = 0;
        driver.expireAfterIndexReply = 1;
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertEquals(0, driver.chargedBytes);
        Assertions.assertEquals(0, driver.indexesRead);
        Assertions.assertEquals(0, driver.indexHasNextCalls);
        Assertions.assertEquals(0, driver.getMoreCalls);
        Assertions.assertEquals(0, driver.statisticsCommands);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.indexReplies);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void lateGetMoreBatchIsChargedBeforeDeadlineFallback() {
        var driver = new Driver();
        driver.indexes = List.of(new Document("name", "other_1").append("key", new Document("other", 1)),
                new Document("name", "sequence_1").append("key", new Document("sequence", 1)));
        driver.initialIndexBatchLength = 1;
        driver.indexBatchLength = 1;
        driver.expireAfterIndexReply = 2;
        long expected = driver.indexes.stream().mapToLong(d -> MongoSplitPlan.encode(d.toBsonDocument()).length).sum();
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertEquals(expected, driver.chargedBytes);
        Assertions.assertEquals(2, driver.indexesRead);
        Assertions.assertEquals(1, driver.getMoreCalls);
        Assertions.assertEquals(2, driver.indexReplies);
        Assertions.assertEquals(0, driver.statisticsCommands);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void cancelledProbeCannotFetchAnUnbufferedIndexBatch() {
        var driver = new Driver();
        driver.initialIndexBatchLength = 0;
        driver.cancelAfterIndexReply = 1;
        Assertions.assertThrows(CancellationException.class, () -> run(driver));
        Assertions.assertEquals(0, driver.chargedBytes);
        Assertions.assertEquals(0, driver.indexesRead);
        Assertions.assertEquals(0, driver.indexHasNextCalls);
        Assertions.assertEquals(0, driver.getMoreCalls);
        Assertions.assertEquals(0, driver.statisticsCommands);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void cancelledProbeDoesNotConsumeBufferedIndexBatch() {
        var driver = new Driver();
        driver.cancelAfterIndexReply = 1;
        Assertions.assertThrows(CancellationException.class, () -> run(driver));
        Assertions.assertEquals(0, driver.chargedBytes);
        Assertions.assertEquals(0, driver.indexesRead);
        Assertions.assertEquals(0, driver.getMoreCalls);
        Assertions.assertEquals(0, driver.statisticsCommands);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void qualifyingFirstIndexDoesNotHideOversizedMetadataInItsBatch() {
        var driver = new Driver();
        driver.indexes = List.of(new Document("name", "sequence_1").append("key", new Document("sequence", 1)),
                new Document("name", "partial_other").append("key", new Document("other", 1))
                        .append("partialFilterExpression", new Document("text", "x".repeat(75000))));
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertEquals(2, driver.indexesRead);
        Assertions.assertTrue(driver.chargedBytes > 75000);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void fullStatisticsReplyIsIncludedInProbeBytes() {
        var driver = new Driver();
        driver.statistics.append("indexDetails", new Document("metadata", "x".repeat(55000)));
        var result = run(driver);
        Assertions.assertTrue(result.sampleBytes() >= 55000);
        Assertions.assertEquals(result.sampleBytes(), driver.chargedBytes);
    }

    @Test
    void oversizedStatisticsAreChargedBeforeProbeFailure() {
        var driver = new Driver();
        driver.statistics.append("indexDetails", new Document("metadata", "x".repeat(1024 * 1024)));
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertTrue(driver.chargedBytes > 1024 * 1024);
        Assertions.assertEquals(0, driver.dataCommands);
    }

    @Test
    void autoSmallCollectionDoesNotSendSamplingAggregate() {
        var driver = new Driver();
        driver.statistics = new Document("size", 10000);
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
        var table = new MongoTableHandle("sample", new Document("name", "records").append("type", "collection")
                .append("info", new Document("uuid", new org.bson.types.Binary((byte) 4, new byte[16]))), properties);
        var scope = new MongoSnapshotPlanTest.Scope();
        var session = new MongoScanPlanTest.Session(Map.of("mongodb_read_concern", "snapshot",
                "mongodb_split_strategy", "auto", "mongodb_split_field", "sequence", "file_split_size", "33554432")) {
            @Override
            public org.apache.doris.connector.spi.ConnectorStatementScope getStatementScope() {
                return scope;
            }
        };
        try (var catalog = new MongoDriverCatalogClient(driver.client(), properties)) {
            var client = new MongoSnapshotPlanTest.Client() {
                @Override
                public MongoSplitProbe probeSplits(MongoTableHandle handle, String field, String preference,
                        TMongoSnapshot snapshot, int rows, long timeoutMs,
                        LongPredicate shouldSample, LongConsumer accountBytes, Runnable checkCancelled) {
                    return catalog.probeSplits(handle, field, preference, snapshot, rows, timeoutMs,
                            shouldSample, accountBytes, checkCancelled);
                }
            };
            var provider = new MongoScanPlanProvider(properties, () -> client);
            var request = org.apache.doris.connector.spi.scan.ConnectorScanRequest.builder(table, List.of()).build();
            Assertions.assertEquals(1, provider.planScan(session, request).size());
            Assertions.assertEquals("small_collection", provider.getScanNodeProperties(session, table,
                    List.of(), java.util.Optional.empty()).get("split_reason"));
            Assertions.assertEquals(0, driver.dataCommands);
        }
    }

    @Test
    void probesPinTheTimestampAndUseBoundedScalarAggregation() {
        var driver = new Driver();
        var result = run(driver);
        Assertions.assertTrue(result.indexed());
        Assertions.assertEquals(9007199254740993L, result.samples().get(0).asInt64().getValue());
        Assertions.assertEquals(4L * 1024 * 1024 * 1024, result.logicalBytes());
        Assertions.assertEquals(new BsonTimestamp(123, 456), driver.pin);
        Assertions.assertEquals(List.of(TimeoutMode.CURSOR_LIFETIME), driver.modes);
        Assertions.assertEquals(16, driver.limit);
        Assertions.assertEquals(2, driver.batchSize);
        Assertions.assertEquals(128, driver.indexBatchSize);
        Assertions.assertFalse(driver.aggregate.containsKey("hint"));
        Assertions.assertEquals(0, driver.projection.getInteger("_id"));
        Assertions.assertTrue(driver.projection.get("value", Document.class).containsKey("$cond"));
        Assertions.assertTrue(driver.timeouts.stream().allMatch(value -> value > 0 && value <= 1000));
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void cancellationStopsAfterTheExhaustedReplyWithoutAnotherCommand() {
        var driver = new Driver();
        driver.cancelAfterFirst = true;
        Assertions.assertThrows(CancellationException.class, () -> run(driver));
        Assertions.assertEquals(1, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void wildcardBeforeOrdinaryIndexIsSkippedWithoutHintingTheSample() {
        for (int direction : List.of(1, -1)) {
            var driver = new Driver();
            driver.indexes = List.of(new Document("name", "compound_wildcard")
                            .append("key", new Document("sequence", direction).append("payload.$**", 1)),
                    new Document("name", "regular_compound")
                            .append("key", new Document("sequence", direction).append("suffix", 1)));
            Assertions.assertTrue(run(driver).indexed());
            Assertions.assertEquals(2, driver.indexesRead);
            Assertions.assertEquals(1, driver.dataCommands);
            Assertions.assertFalse(driver.aggregate.containsKey("hint"));
            Assertions.assertEquals(0, driver.sessionsStarted);
        }
    }

    @Test
    void wildcardOnlyIndexDoesNotSampleData() {
        var driver = new Driver();
        driver.indexes = List.of(new Document("name", "compound_wildcard")
                .append("key", new Document("sequence", 1).append("payload.$**", 1)));
        Assertions.assertFalse(run(driver).indexed());
        Assertions.assertEquals(0, driver.sessionsStarted);
        Assertions.assertNull(driver.aggregate);
        Assertions.assertEquals(0, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void wildcardOnlyIndexUsesTheExistingAutoFallbackAndRangeRejection() {
        for (String strategy : List.of("auto", "range")) {
            var driver = new Driver();
            driver.indexes = List.of(new Document("name", "compound_wildcard")
                    .append("key", new Document("sequence", 1).append("payload.$**", 1)));
            var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
            var table = new MongoTableHandle("sample", new Document("name", "records")
                    .append("type", "collection").append("info", new Document("uuid",
                            new org.bson.types.Binary((byte) 4, new byte[16]))), properties);
            var scope = new MongoSnapshotPlanTest.Scope();
            var session = new MongoScanPlanTest.Session(Map.of("mongodb_read_concern", "snapshot",
                    "mongodb_split_strategy", strategy, "mongodb_split_field", "sequence")) {
                @Override
                public org.apache.doris.connector.spi.ConnectorStatementScope getStatementScope() {
                    return scope;
                }
            };
            try (var catalog = new MongoDriverCatalogClient(driver.client(), properties)) {
                var client = new MongoSnapshotPlanTest.Client() {
                    @Override
                    public MongoSplitProbe probeSplits(MongoTableHandle handle, String field, String preference,
                            TMongoSnapshot snapshot, int rows, long timeoutMs,
                            LongPredicate shouldSample, LongConsumer accountBytes, Runnable checkCancelled) {
                        return catalog.probeSplits(handle, field, preference, snapshot, rows, timeoutMs,
                                shouldSample, accountBytes, checkCancelled);
                    }
                };
                var provider = new MongoScanPlanProvider(properties, () -> client);
                var request = org.apache.doris.connector.spi.scan.ConnectorScanRequest.builder(table, List.of()).build();
                if ("auto".equals(strategy)) {
                    Assertions.assertEquals(1, provider.planScan(session, request).size());
                    Assertions.assertEquals("missing_index", provider.getScanNodeProperties(session, table,
                            List.of(), java.util.Optional.empty()).get("split_reason"));
                } else {
                    var error = Assertions.assertThrows(IllegalArgumentException.class,
                            () -> provider.planScan(session, request));
                    Assertions.assertTrue(error.getMessage().contains("missing_index"));
                }
                Assertions.assertEquals(0, driver.sessionsStarted);
                Assertions.assertEquals(0, driver.dataCommands);
            }
        }
    }

    @Test
    void queryTaskGroupClosureStopsAfterTheReplyEvenWithAnUnregisteredContext() {
        var driver = new Driver();
        driver.closeQueryAfterFirst = true;
        Assertions.assertThrows(CancellationException.class, () -> run(driver));
        Assertions.assertFalse(driver.context.isCancelled());
        Assertions.assertEquals(1, driver.dataCommands);
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void historyFailureClosesMetadataCursorWithoutEchoingSourceData() {
        var driver = new Driver();
        driver.sourceFailure = true;
        var error = Assertions.assertThrows(DorisConnectorException.class, () -> run(driver));
        Assertions.assertTrue(error.getMessage().contains("239"));
        Assertions.assertFalse(error.getMessage().contains("fixture-secret"));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(1, driver.cursorsClosed);
    }

    @Test
    void oversizedIndexMetadataStopsBeforeSampling() {
        var driver = new Driver();
        driver.indexes = List.of(new Document("key", new Document("other", 1))
                .append("partialFilterExpression", new Document("text", "x".repeat(70000))));
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertEquals(1, driver.cursorsClosed);
    }
}
