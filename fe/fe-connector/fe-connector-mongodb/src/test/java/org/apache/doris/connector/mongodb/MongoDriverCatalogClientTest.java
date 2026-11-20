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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class MongoDriverCatalogClientTest {
    private static final class RecordingDriver {
        final List<Document> commands = new ArrayList<>();
        int clientCloses;
        int cursorCloses;
        final List<ListCollectionsOperation<Document>> operations = new ArrayList<>();
        boolean failGetMore;
        boolean overflow;
        long remainingCursor = 123;

        @SuppressWarnings("unchecked")
        <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
        }

        MongoClient client() {
            MongoDatabase database = proxy(MongoDatabase.class, (p, method, args) -> {
                if (method.getName().equals("listCollections")) {
                    return proxy(ListCollectionsIterable.class, (iterable, operation, parameters) -> {
                        if (operation.getName().equals("iterator")) {
                            throw new MongoCommandException(new Document("ok", 0).append("code", 13)
                                    .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
                        }
                        return iterable;
                    });
                }
                if (method.getName().startsWith("withRead")) {
                    return p;
                }
                if (method.getName().equals("runCommand")) {
                    commands.add((Document) args[args.length - 1]);
                    throw new MongoCommandException(new Document("ok", 0).append("code", 43)
                            .append("errmsg", "cursor belongs to another router").toBsonDocument(), new ServerAddress());
                }
                throw new UnsupportedOperationException(method.getName());
            });
            return proxy(MongoClient.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "getDatabase":
                        return database;
                    case "close":
                        clientCloses++;
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
        }

        @SuppressWarnings("unchecked")
        BatchCursor<Document> execute(ListCollectionsOperation<Document> operation) {
            operations.add(operation);
            int[] batch = {0};
            return proxy(BatchCursor.class, (cursor, method, args) -> {
                switch (method.getName()) {
                    case "hasNext":
                        if (failGetMore && batch[0] == 1 && remainingCursor != 0) {
                            throw new MongoCommandException(new Document("ok", 0).append("code", 13)
                                    .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
                        }
                        return batch[0] < (remainingCursor == 0 ? 1 : 2);
                    case "next":
                        if (overflow) {
                            return Collections.nCopies(100001, new Document("name", "first"));
                        }
                        return List.of(new Document("name", batch[0]++ == 0 ? "first" : "second")
                                .append("type", batch[0] == 1 ? "collection" : "view"));
                    case "close":
                        cursorCloses++;
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
        }

        MongoDriverCatalogClient catalog() {
            return new MongoDriverCatalogClient(client(), MongoCatalogProperties.of(
                    Map.of("mongodb.uri", "mongodb://localhost:27017", "mongodb.databases", "sample")),
                    this::execute);
        }
    }

    @Test
    void authorizedListingDrainsOneServerBoundCursorAndClosesIt() {
        RecordingDriver driver = new RecordingDriver();
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            Assertions.assertEquals(List.of("sample"), catalog.databases());
            List<Document> objects = catalog.collections("sample");
            Assertions.assertEquals(2, objects.size());
            Assertions.assertEquals("view", objects.get(1).getString("type"));
            Assertions.assertEquals(1, driver.operations.size());
            Assertions.assertTrue(driver.operations.get(0).isAuthorizedCollections());
            Assertions.assertTrue(driver.operations.get(0).isNameOnly());
            Assertions.assertEquals(128, driver.operations.get(0).getBatchSize());
            Assertions.assertEquals(1, driver.cursorCloses);
            Assertions.assertTrue(driver.commands.isEmpty());
        }
        Assertions.assertEquals(1, driver.clientCloses);
    }

    @Test
    void failedGetMoreClosesOwningCursorAndRedactsSourceError() {
        RecordingDriver driver = new RecordingDriver();
        driver.failGetMore = true;
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> catalog.collections("sample"));
            Assertions.assertFalse(error.toString().contains("fixture-secret"));
            Assertions.assertNull(error.getCause());
            Assertions.assertEquals(1, driver.cursorCloses);
            Assertions.assertTrue(driver.commands.isEmpty());
        }
    }

    @Test
    void exhaustedCursorClosesWithoutReselectingAServer() {
        RecordingDriver driver = new RecordingDriver();
        driver.remainingCursor = 0;
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            Assertions.assertEquals(1, catalog.collections("sample").size());
            Assertions.assertTrue(driver.commands.isEmpty());
            Assertions.assertEquals(1, driver.cursorCloses);
        }
    }

    @Test
    void metadataLimitFailureClosesTheServerBoundCursor() {
        RecordingDriver driver = new RecordingDriver();
        driver.overflow = true;
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            Assertions.assertThrows(DorisConnectorException.class, () -> catalog.collections("sample"));
            Assertions.assertEquals(1, driver.cursorCloses);
        }
    }

    @Test
    void autoFallsBackWithoutValidatorPrivilegesButForcedValidatorFails() {
        RecordingDriver driver = new RecordingDriver();
        driver.remainingCursor = 0;
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            Document result = catalog.collection("sample", "first").orElseThrow();
            Assertions.assertFalse(result.getBoolean("metadata_available"));
            Assertions.assertEquals("collection", result.getString("type"));
            Assertions.assertFalse(result.containsKey("info"));
            Assertions.assertEquals("first", driver.operations.get(0).getFilter().getString("name").getValue());
            Assertions.assertEquals(1, driver.cursorCloses);
        }
        RecordingDriver strict = new RecordingDriver();
        try (MongoDriverCatalogClient catalog = new MongoDriverCatalogClient(strict.client(),
                MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                        "mongodb.schema.mode", "validator")), strict::execute)) {
            Assertions.assertThrows(DorisConnectorException.class, () -> catalog.collection("sample", "first"));
            Assertions.assertTrue(strict.operations.isEmpty());
        }
    }

    private static final class SnapshotDriver {
        final RecordingDriver proxies = new RecordingDriver();
        BsonTimestamp time = new BsonTimestamp(123, 456);
        Document projection;
        long timeout;
        int limit;
        int batchSize;
        int sessionsClosed;
        int cursorsClosed;
        boolean fail;
        boolean empty;

        MongoClient client() {
            ClientSession session = proxies.proxy(ClientSession.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "close":
                        Assertions.assertEquals(1, cursorsClosed);
                        sessionsClosed++;
                        return null;
                    case "getSnapshotTimestamp":
                        return time;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
            MongoCursor<?> cursor = proxies.proxy(MongoCursor.class, (p, method, args) -> {
                if (method.getName().equals("close")) {
                    cursorsClosed++;
                    return null;
                }
                if (method.getName().equals("hasNext")) {
                    if (fail) {
                        throw new MongoCommandException(new Document("ok", 0).append("code", 239)
                                .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
                    }
                    return !empty;
                }
                throw new UnsupportedOperationException(method.getName());
            });
            FindIterable<?> find = proxies.proxy(FindIterable.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "projection":
                        projection = (Document) args[0];
                        return p;
                    case "limit":
                        limit = (int) args[0];
                        return p;
                    case "batchSize":
                        batchSize = (int) args[0];
                        return p;
                    case "iterator":
                        return cursor;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
            MongoCollection<?> collection = proxies.proxy(MongoCollection.class, (p, method, args) -> {
                if (method.getName().equals("find")) {
                    Assertions.assertSame(session, args[0]);
                    return find;
                }
                if (method.getName().equals("withReadPreference")) {
                    return p;
                }
                throw new UnsupportedOperationException(method.getName());
            });
            MongoDatabase database = proxies.proxy(MongoDatabase.class, (p, method, args) -> {
                if (method.getName().startsWith("withRead")) {
                    return p;
                }
                if (method.getName().equals("withTimeout")) {
                    timeout = (long) args[0];
                    return p;
                }
                if (method.getName().equals("getCollection")) {
                    return collection;
                }
                throw new UnsupportedOperationException(method.getName());
            });
            return proxies.proxy(MongoClient.class, (p, method, args) -> {
                if (method.getName().equals("startSession")) {
                    var options = (ClientSessionOptions) args[0];
                    Assertions.assertFalse(options.isCausallyConsistent());
                    Assertions.assertTrue(options.isSnapshot());
                    return session;
                }
                if (method.getName().equals("getDatabase")) {
                    return database;
                }
                if (method.getName().equals("close")) {
                    return null;
                }
                throw new UnsupportedOperationException(method.getName());
            });
        }

        MongoDriverCatalogClient catalog() {
            return new MongoDriverCatalogClient(client(), MongoCatalogProperties.of(
                    Map.of("mongodb.uri", "mongodb://localhost:27017")), proxies::execute);
        }
    }

    @Test
    void snapshotProbeClosesServerBoundCursorBeforeItsSessionEvenForEmptyCollections() {
        for (boolean empty : List.of(false, true)) {
            SnapshotDriver driver = new SnapshotDriver();
            driver.empty = empty;
            try (var catalog = driver.catalog()) {
                Assertions.assertEquals(driver.time, catalog.selectSnapshot("sample", "records", "primary", 1000));
                Assertions.assertEquals(1, driver.limit);
                Assertions.assertEquals(1, driver.batchSize);
                Assertions.assertEquals(new Document("_id", 0).append("__doris_snapshot_probe__",
                        new Document("$literal", 1)), driver.projection);
                Assertions.assertTrue(driver.timeout > 0 && driver.timeout <= 1000);
                Assertions.assertEquals(1, driver.cursorsClosed);
                Assertions.assertEquals(1, driver.sessionsClosed);
            }
        }
    }

    @Test
    void snapshotSelectionFailureClosesCursorAndSessionAndKeepsOnlyStableErrorCode() {
        SnapshotDriver driver = new SnapshotDriver();
        driver.fail = true;
        try (var catalog = driver.catalog()) {
            var error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> catalog.selectSnapshot("sample", "records", "primary", 1000));
            Assertions.assertTrue(error.getMessage().contains("239"));
            Assertions.assertFalse(error.getMessage().contains("fixture-secret"));
            Assertions.assertNull(error.getCause());
            Assertions.assertEquals(1, driver.cursorsClosed);
            Assertions.assertEquals(1, driver.sessionsClosed);
        }
    }

    @Test
    void snapshotProbeRequiresTheDriverSnapshotTimeWithoutSubstitutingOtherClockFields() {
        SnapshotDriver driver = new SnapshotDriver();
        driver.time = null;
        try (var catalog = driver.catalog()) {
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> catalog.selectSnapshot("sample", "records", "primary", 1000));
            Assertions.assertEquals(1, driver.cursorsClosed);
            Assertions.assertEquals(1, driver.sessionsClosed);
        }
    }
}
