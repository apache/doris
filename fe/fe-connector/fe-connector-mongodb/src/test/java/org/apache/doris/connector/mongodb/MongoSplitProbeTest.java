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
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
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

class MongoSplitProbeTest {
    private static final class Driver {
        final ConnectorScanContext context = new ConnectorScanContext();
        final List<TimeoutMode> modes = new ArrayList<>();
        final List<Long> timeouts = new ArrayList<>();
        List<Document> indexes = List.of(new Document("name", "sequence_1")
                .append("key", new Document("sequence", 1)));
        Document projection;
        String hint;
        BsonTimestamp pin;
        int limit;
        int dataHasNext;
        int cursorsClosed;
        int sessionsClosed;
        boolean cancelAfterFirst;
        boolean sourceFailure;

        @SuppressWarnings("unchecked")
        <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
        }

        MongoClient client() {
            ClientSession session = proxy(ClientSession.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "setSnapshotTimestamp":
                        pin = (BsonTimestamp) args[0];
                        return null;
                    case "close":
                        Assertions.assertEquals(2, cursorsClosed);
                        sessionsClosed++;
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
            int[] indexPosition = {0};
            MongoCursor<?> indexCursor = proxy(MongoCursor.class, (p, method, args) -> switch (method.getName()) {
                case "hasNext" -> indexPosition[0] < indexes.size();
                case "next" -> indexes.get(indexPosition[0]++);
                case "close" -> {
                    cursorsClosed++;
                    yield null;
                }
                default -> throw new UnsupportedOperationException(method.getName());
            });
            int[] position = {0};
            MongoCursor<?> dataCursor = proxy(MongoCursor.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "hasNext":
                        dataHasNext++;
                        return position[0] < (cancelAfterFirst ? 2 : 1);
                    case "next":
                        if (sourceFailure) {
                            throw new MongoCommandException(new Document("ok", 0).append("code", 239)
                                    .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
                        }
                        position[0]++;
                        if (cancelAfterFirst) {
                            context.cancel();
                        }
                        return new BsonDocument("value", new BsonInt64(9007199254740993L));
                    case "close":
                        cursorsClosed++;
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
            ListIndexesIterable<?> list = proxy(ListIndexesIterable.class, (p, method, args) -> {
                if ("iterator".equals(method.getName())) {
                    return indexCursor;
                }
                if ("timeoutMode".equals(method.getName())) {
                    modes.add((TimeoutMode) args[0]);
                }
                return p;
            });
            FindIterable<?> find = proxy(FindIterable.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "iterator": return dataCursor;
                    case "projection":
                        projection = (Document) args[0];
                        break;
                    case "hintString":
                        hint = (String) args[0];
                        break;
                    case "limit":
                        limit = (int) args[0];
                        break;
                    case "timeoutMode":
                        modes.add((TimeoutMode) args[0]);
                        break;
                    default: break;
                }
                return p;
            });
            MongoCollection<?> collection = proxy(MongoCollection.class, (p, method, args) -> switch (method.getName()) {
                case "listIndexes" -> list;
                case "find" -> {
                    Assertions.assertSame(session, args[0]);
                    yield find;
                }
                case "withReadPreference" -> p;
                default -> throw new UnsupportedOperationException(method.getName());
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
                    case "runCommand" -> new Document("size", 4L * 1024 * 1024 * 1024);
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            });
            return proxy(MongoClient.class, (p, method, args) -> switch (method.getName()) {
                case "getDatabase" -> database;
                case "startSession" -> session;
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
            return catalog.probeSplits(table, "sequence", "primary", snapshot, 16, 1000, driver.context);
        }
    }

    @Test
    void probesPinTheTimestampAndUseLifetimeDeadlinesAndFixedWidthProjection() {
        var driver = new Driver();
        var result = run(driver);
        Assertions.assertTrue(result.indexed());
        Assertions.assertEquals(9007199254740993L, result.samples().get(0).asInt64().getValue());
        Assertions.assertEquals(4L * 1024 * 1024 * 1024, result.logicalBytes());
        Assertions.assertEquals(new BsonTimestamp(123, 456), driver.pin);
        Assertions.assertEquals(List.of(TimeoutMode.CURSOR_LIFETIME, TimeoutMode.CURSOR_LIFETIME), driver.modes);
        Assertions.assertEquals(16, driver.limit);
        Assertions.assertEquals("sequence_1", driver.hint);
        Assertions.assertEquals(0, driver.projection.getInteger("_id"));
        Assertions.assertTrue(driver.projection.get("value", Document.class).containsKey("$cond"));
        Assertions.assertTrue(driver.timeouts.stream().allMatch(value -> value > 0 && value <= 1000));
        Assertions.assertEquals(2, driver.cursorsClosed);
        Assertions.assertEquals(1, driver.sessionsClosed);
    }

    @Test
    void cancellationStopsBeforeTheNextRemoteCursorAdvance() {
        var driver = new Driver();
        driver.cancelAfterFirst = true;
        Assertions.assertThrows(CancellationException.class, () -> run(driver));
        Assertions.assertEquals(1, driver.dataHasNext);
        Assertions.assertEquals(2, driver.cursorsClosed);
        Assertions.assertEquals(1, driver.sessionsClosed);
    }

    @Test
    void historyFailureClosesBothCursorsAndSessionWithoutEchoingSourceData() {
        var driver = new Driver();
        driver.sourceFailure = true;
        var error = Assertions.assertThrows(DorisConnectorException.class, () -> run(driver));
        Assertions.assertTrue(error.getMessage().contains("239"));
        Assertions.assertFalse(error.getMessage().contains("fixture-secret"));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(2, driver.cursorsClosed);
        Assertions.assertEquals(1, driver.sessionsClosed);
    }

    @Test
    void oversizedIndexMetadataStopsBeforeOpeningADataSession() {
        var driver = new Driver();
        driver.indexes = List.of(new Document("key", new Document("other", 1))
                .append("partialFilterExpression", new Document("text", "x".repeat(70000))));
        Assertions.assertThrows(MongoRangePlanner.PlanningLimit.class, () -> run(driver));
        Assertions.assertEquals(1, driver.cursorsClosed);
        Assertions.assertEquals(0, driver.sessionsClosed);
    }
}
