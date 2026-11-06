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

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class MongoDriverCatalogClientTest {
    private static final class RecordingDriver {
        final List<Document> commands = new ArrayList<>();
        int sessionCloses;
        int clientCloses;
        boolean failGetMore;
        long remainingCursor = 123;

        @SuppressWarnings("unchecked")
        <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
        }

        MongoClient client() {
            ClientSession session = proxy(ClientSession.class, (p, method, args) -> {
                if (method.getName().equals("close")) {
                    sessionCloses++;
                    return null;
                }
                throw new UnsupportedOperationException(method.getName());
            });
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
                    Assertions.assertSame(session, args[0]);
                    Document command = (Document) args[1];
                    commands.add(new Document(command));
                    if (command.containsKey("killCursors")) {
                        return new Document("ok", 1);
                    }
                    if (command.containsKey("getMore")) {
                        // Ordinary metadata cursors inherit the initial command deadline; MongoDB
                        // rejects maxTimeMS on getMore for a non-awaitData cursor.
                        Assertions.assertFalse(command.containsKey("maxTimeMS"));
                        if (failGetMore) {
                            throw new MongoCommandException(new Document("ok", 0).append("code", 13)
                                    .append("errmsg", "fixture-secret").toBsonDocument(), new ServerAddress());
                        }
                        return new Document("cursor", new Document("id", 0L).append("nextBatch",
                                List.of(new Document("name", "second").append("type", "view"))));
                    }
                    return new Document("cursor", new Document("id", remainingCursor).append("firstBatch",
                            List.of(new Document("name", "first").append("type", "collection"))));
                }
                throw new UnsupportedOperationException(method.getName());
            });
            return proxy(MongoClient.class, (p, method, args) -> {
                switch (method.getName()) {
                    case "getDatabase":
                        return database;
                    case "startSession":
                        return session;
                    case "close":
                        clientCloses++;
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            });
        }

        MongoDriverCatalogClient catalog() {
            return new MongoDriverCatalogClient(client(), MongoCatalogProperties.of(
                    Map.of("mongodb.uri", "mongodb://localhost:27017", "mongodb.databases", "sample")));
        }
    }

    @Test
    void authorizedListingDrainsEveryBatchAndClosesSession() {
        RecordingDriver driver = new RecordingDriver();
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            Assertions.assertEquals(List.of("sample"), catalog.databases());
            List<Document> objects = catalog.collections("sample");
            Assertions.assertEquals(2, objects.size());
            Assertions.assertEquals("view", objects.get(1).getString("type"));
            Assertions.assertTrue(driver.commands.get(0).getBoolean("authorizedCollections"));
            Assertions.assertTrue(driver.commands.get(0).getBoolean("nameOnly"));
            Assertions.assertEquals(1, driver.sessionCloses);
        }
        Assertions.assertEquals(1, driver.clientCloses);
    }

    @Test
    void failedGetMoreKillsCursorAndRedactsSourceError() {
        RecordingDriver driver = new RecordingDriver();
        driver.failGetMore = true;
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> catalog.collections("sample"));
            Assertions.assertFalse(error.toString().contains("fixture-secret"));
            Assertions.assertNull(error.getCause());
            Assertions.assertEquals("$cmd.listCollections", driver.commands.get(2).getString("killCursors"));
            Assertions.assertEquals(List.of(123L), driver.commands.get(2).getList("cursors", Long.class));
            Assertions.assertEquals(1, driver.sessionCloses);
        }
    }

    @Test
    void exhaustedCursorDoesNotSendKillCursors() {
        RecordingDriver driver = new RecordingDriver();
        driver.remainingCursor = 0;
        try (MongoDriverCatalogClient catalog = driver.catalog()) {
            Assertions.assertEquals(1, catalog.collections("sample").size());
            Assertions.assertEquals(1, driver.commands.size());
            Assertions.assertEquals(1, driver.sessionCloses);
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
        }
        RecordingDriver strict = new RecordingDriver();
        try (MongoDriverCatalogClient catalog = new MongoDriverCatalogClient(strict.client(),
                MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                        "mongodb.schema.mode", "validator")))) {
            Assertions.assertThrows(DorisConnectorException.class, () -> catalog.collection("sample", "first"));
            Assertions.assertTrue(strict.commands.isEmpty());
        }
    }

}
