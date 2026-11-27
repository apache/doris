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
import org.apache.doris.thrift.TMongoSnapshot;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

class MongoSplitSamplingTest {
    private static final class Driver {
        final List<BsonValue> values = new ArrayList<>(List.of(new BsonInt32(1), new BsonInt64(100000)));
        final List<Document> commands = new ArrayList<>();
        Document aggregate;
        ReadPreference selectedPreference;
        String preference = "primary";
        long count = 1000000;
        long timeout;
        boolean indexed = true;
        RuntimeException failure;

        @SuppressWarnings("unchecked")
        <T> T proxy(Class<T> type, InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
        }

        <T> MongoCursor<T> cursor(List<T> batch) {
            int[] offset = {0};
            return proxy(MongoCursor.class, (p, method, args) -> switch (method.getName()) {
                case "hasNext" -> offset[0] < batch.size();
                case "available" -> batch.size() - offset[0];
                case "next" -> batch.get(offset[0]++);
                case "close" -> null;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }

        MongoClient client() {
            MongoCollection<?> collection = proxy(MongoCollection.class, (p, method, args) -> {
                if (method.getName().equals("listIndexes")) {
                    return proxy(ListIndexesIterable.class, (i, operation, parameters) -> {
                        if (operation.getName().equals("iterator")) {
                            return cursor(indexed ? List.of(new Document("name", "_id_")
                                    .append("key", new Document("_id", 1))) : List.of());
                        }
                        return i;
                    });
                }
                if (method.getName().equals("find")) {
                    return proxy(FindIterable.class, (f, operation, parameters) -> {
                        if (operation.getName().equals("iterator")) {
                            return cursor(values.stream().map(v -> new BsonDocument("value", v)).toList());
                        }
                        return f;
                    });
                }
                return p;
            });
            MongoDatabase database = proxy(MongoDatabase.class, (p, method, args) -> {
                if (method.getName().equals("getCollection")) {
                    return collection;
                }
                if (method.getName().equals("withTimeout")) {
                    timeout = (long) args[0];
                    return p;
                }
                if (method.getName().startsWith("withRead")) {
                    return p;
                }
                if (method.getName().equals("runCommand")) {
                    Document command = (Document) args[0];
                    commands.add(command);
                    if (command.containsKey("collStats")) {
                        return new Document("size", 100000000L).append("count", count);
                    }
                    aggregate = command;
                    selectedPreference = (ReadPreference) args[1];
                    if (failure != null) {
                        throw failure;
                    }
                    return new BsonDocument("cursor", new BsonDocument("id", new BsonInt64(0))
                            .append("firstBatch", values.isEmpty() ? new BsonArray() : new BsonArray(List.of(
                                    new BsonDocument("values", new BsonArray(values))))));
                }
                throw new UnsupportedOperationException(method.getName());
            });
            return proxy(MongoClient.class, (p, method, args) -> switch (method.getName()) {
                case "getDatabase" -> database;
                case "startSession" -> proxy(ClientSession.class, (s, operation, parameters) -> null);
                case "close" -> null;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }

        MongoSplitProbe probe(Runnable cancelled) {
            var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
            try (var client = new MongoDriverCatalogClient(client(), properties)) {
                var table = new MongoTableHandle("sample", new Document("name", "records")
                        .append("type", "collection"), properties);
                var snapshot = new TMongoSnapshot().setTimestampSeconds(42).setTimestampIncrement(3);
                return client.probeSplits(table, "_id", preference, snapshot, 4096, 1000,
                        size -> true, bytes -> { }, cancelled);
            }
        }
    }

    @Test
    void sampleUsesWholeCollectionAndOneBoundedSnapshotReply() {
        var driver = new Driver();
        var probe = driver.probe(() -> {});
        Assertions.assertNotNull(driver.aggregate, "Use source random sampling, not an index prefix");
        var stages = driver.aggregate.getList("pipeline", Document.class);
        Assertions.assertEquals(new Document("$sample", new Document("size", 4096)), stages.get(0));
        var projection = stages.get(1).get("$project", Document.class);
        Assertions.assertEquals(0, projection.getInteger("_id"));
        var choice = projection.get("value", Document.class).getList("$cond", Object.class);
        Assertions.assertEquals(new Document("$in", List.of(new Document("$type", "$_id"),
                List.of("int", "long", "objectId", "date"))), choice.get(0));
        Assertions.assertEquals("$_id", choice.get(1));
        Assertions.assertEquals(new Document("$literal", null), choice.get(2));
        Assertions.assertEquals(new Document("_id", null).append("values", new Document("$push", "$value")),
                stages.get(2).get("$group"));
        Assertions.assertEquals(new Document("batchSize", 2), driver.aggregate.get("cursor"));
        Assertions.assertFalse(driver.aggregate.containsKey("hint"));
        Assertions.assertEquals(new Document("level", "snapshot")
                .append("atClusterTime", new BsonTimestamp(42, 3)), driver.aggregate.get("readConcern"));
        Assertions.assertEquals(driver.values, probe.samples());
        Assertions.assertEquals(100000000L, probe.logicalBytes());
        Assertions.assertTrue(driver.timeout > 0 && driver.timeout <= 1000);
    }

    @Test
    void smallCollectionsKeepTheBoundedRequestInsteadOfForcingARandomCursor() {
        var driver = new Driver();
        driver.count = 601;
        driver.probe(() -> {});
        Assertions.assertNotNull(driver.aggregate);
        Assertions.assertEquals(4096, driver.aggregate.getList("pipeline", Document.class).get(0)
                .get("$sample", Document.class).getInteger("size"));
    }

    @Test
    void absentIndexSkipsTheDataProbe() {
        var driver = new Driver();
        driver.indexed = false;
        Assertions.assertFalse(driver.probe(() -> {}).indexed());
        Assertions.assertNull(driver.aggregate);
    }

    @Test
    void emptySampleDoesNotClaimThatTheCollectionIsEmpty() {
        var driver = new Driver();
        driver.values.clear();
        Assertions.assertTrue(driver.probe(() -> {}).samples().isEmpty());
        Assertions.assertNotNull(driver.aggregate);
    }

    @Test
    void sampleTimeoutCanFallBackButPermissionFailureCannot() {
        var driver = new Driver();
        driver.failure = new MongoExecutionTimeoutException(50, "fixture-secret");
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> driver.probe(() -> {}));
        driver.failure = new MongoCommandException(new Document("ok", 0).append("code", 13)
                .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
        var failure = Assertions.assertThrows(DorisConnectorException.class, () -> driver.probe(() -> {}));
        Assertions.assertFalse(failure.toString().contains("fixture-secret"));
        Assertions.assertNull(failure.getCause());
    }

    @Test
    void cancelledSamplingDoesNotIssueAnotherCommand() {
        var driver = new Driver();
        Assertions.assertThrows(CancellationException.class, () -> driver.probe(() -> {
            if (driver.aggregate != null) {
                throw new CancellationException();
            }
        }));
        Assertions.assertEquals(2, driver.commands.size());
    }

    @Test
    void excessiveReturnedSamplesConsumeTheProbeBudget() {
        var driver = new Driver();
        driver.values.clear();
        driver.values.addAll(Collections.nCopies(4097, new BsonInt32(1)));
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> driver.probe(() -> {}));
    }

    @Test
    void sampleUsesTheSelectedSnapshotReadPreference() {
        var driver = new Driver();
        driver.preference = "secondary";
        driver.probe(() -> {});
        Assertions.assertEquals(ReadPreference.secondary(), driver.selectedPreference);
    }

    @Test
    void cancellationBeforeProbingPerformsNoSourceWork() {
        var driver = new Driver();
        Assertions.assertThrows(CancellationException.class, () -> driver.probe(() -> {
            throw new CancellationException();
        }));
        Assertions.assertTrue(driver.commands.isEmpty());
    }
}
