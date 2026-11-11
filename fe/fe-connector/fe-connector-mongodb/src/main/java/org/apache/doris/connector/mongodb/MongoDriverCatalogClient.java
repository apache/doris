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
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Catalog-owned thread-safe driver; every metadata cursor/session is local and deterministically closed. */
final class MongoDriverCatalogClient implements MongoCatalogClient {
    private final MongoClient client;
    private final MongoCatalogProperties properties;
    private final Function<ListCollectionsOperation<Document>, BatchCursor<Document>> metadataCursor;

    MongoDriverCatalogClient(MongoCatalogProperties properties) {
        this(MongoClients.create(properties.clientSettings()), properties);
    }

    MongoDriverCatalogClient(MongoClient client, MongoCatalogProperties properties) {
        this(client, properties, operation -> {
            // Driver 5.14 exposes authorized name-only results as strings in its public iterable.
            // Use its pinned core operation to retain kind metadata AND the server-bound cursor.
            MongoClientImpl driver = (MongoClientImpl) client;
            return driver.getOperationExecutor().withTimeoutSettings(driver.getTimeoutSettings()
                    .withMaxTimeMS(properties.getServerSelectionTimeoutMs()))
                    .execute(operation, ReadPreference.primary(), ReadConcern.DEFAULT);
        });
    }

    MongoDriverCatalogClient(MongoClient client, MongoCatalogProperties properties,
            Function<ListCollectionsOperation<Document>, BatchCursor<Document>> metadataCursor) {
        this.properties = properties;
        this.client = client;
        this.metadataCursor = metadataCursor;
    }

    @Override
    public List<String> databases() {
        if (!properties.getDatabases().isEmpty()) {
            return List.copyOf(properties.getDatabases());
        }
        try (MongoCursor<String> cursor = client.listDatabaseNames().iterator()) {
            List<String> names = new ArrayList<>();
            cursor.forEachRemaining(names::add);
            return names;
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    @Override
    public List<Document> collections(String database) {
        return names(database, null);
    }

    @Override
    public Optional<Document> collection(String database, String name) {
        try (MongoCursor<Document> cursor = metadataDatabase(database).listCollections()
                .filter(new Document("name", name)).batchSize(1)
                .maxTime(properties.getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS).iterator()) {
            return cursor.hasNext() ? Optional.of(cursor.next()) : Optional.empty();
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == 13 && !"validator".equals(properties.getSchemaMode())) {
                // A view-only reader may enumerate authorized names but cannot read options/UUIDs.
                // Auto must fall back to document mode when validator metadata is inaccessible.
                // Preserve the returned kind and absence of identity; never inspect its base collection.
                return names(database, name).stream().findFirst();
            }
            throw failure(e);
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    private MongoDatabase metadataDatabase(String database) {
        // listCollections is metadata, not a data read; it cannot inherit a data-scan snapshot policy.
        return client.getDatabase(database).withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary());
    }

    private List<Document> names(String database, String name) {
        ListCollectionsOperation<Document> operation =
                new ListCollectionsOperation<>(database, new DocumentCodec(), null)
                        .nameOnly(true).authorizedCollections(true).batchSize(128);
        if (name != null) {
            operation.filter(new Document("name", name).toBsonDocument());
        }
        // An ordinary ClientSession does not pin a mongos. BatchCursor retains the original
        // connection source for getMore and killCursors, including failure and early-close paths.
        try (BatchCursor<Document> cursor = metadataCursor.apply(operation)) {
            List<Document> result = new ArrayList<>();
            while (cursor.hasNext()) {
                for (Document object : cursor.next()) {
                    result.add(new Document(object).append("metadata_available", false));
                    if (result.size() > 100000) {
                        throw new DorisConnectorException("MongoDB database exceeds metadata listing limit");
                    }
                }
            }
            return result;
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    private static DorisConnectorException failure(MongoException error) {
        // Source messages can contain document values or URI credentials. Only expose the stable code.
        return new DorisConnectorException("MongoDB metadata request failed (code " + error.getCode() + ")");
    }

    @Override
    public void close() {
        client.close();
    }
}
