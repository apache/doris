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
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.types.ObjectId;

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

    @Override
    public MongoSnapshot.Source snapshotSource(long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            // hello identifies replica-set versus mongos routing and reports protocol capabilities.
            Document hello = snapshotDatabase("admin", deadline).runCommand(new Document("hello", 1));
            int wireVersion = hello.getInteger("maxWireVersion", 0);
            if ("isdbgrid".equals(hello.getString("msg"))) {
                // config.version._id=1 holds the clusterId shared by all routers of this deployment.
                // Inclusion projection keeps only that identity; no data rows are used to identify a cluster.
                Document version = snapshotDatabase("config", deadline).getCollection("version")
                        .find(new Document("_id", 1)).projection(new Document("clusterId", 1)).limit(1).first();
                ObjectId identity = version == null ? null : version.getObjectId("clusterId");
                if (identity == null) {
                    throw new IllegalArgumentException("MongoDB snapshot cluster identity metadata is unavailable");
                }
                return new MongoSnapshot.Source("mongos", identity.toHexString(), 0, wireVersion);
            }
            if (!hello.containsKey("setName")) {
                throw new IllegalArgumentException("MongoDB snapshot reads require a replica set or mongos");
            }
            // Read the durable replica-set identity and config.version. Neither hello.setName nor
            // hello.topologyVersion alone identifies a deployment/configuration across connections.
            Document config = snapshotDatabase("admin", deadline).runCommand(new Document("replSetGetConfig", 1))
                    .get("config", Document.class);
            ObjectId identity = config == null ? null : config.get("settings", new Document())
                    .getObjectId("replicaSetId");
            Number version = config == null ? null : config.get("version", Number.class);
            if (identity == null || version == null) {
                throw new IllegalArgumentException("MongoDB snapshot replica-set identity metadata is unavailable");
            }
            return new MongoSnapshot.Source("replica_set", identity.toHexString(), version.longValue(), wireVersion);
        } catch (MongoException failure) {
            throw failure(failure);
        }
    }

    @Override
    public BsonTimestamp selectSnapshot(String database, String collection, String preference, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        // A constant projection bounds the probe payload independently of source document size.
        // MongoDB 7 time-series rewrites can ignore singleBatch. The driver's server-bound cursor
        // must close before its snapshot session, including when routers differ or selection fails.
        // snapshot(true) requests MongoDB-selected snapshot visibility; causal consistency is disabled
        // so this fresh probe does not add an afterClusterTime requirement from earlier operations.
        // find uses an empty filter on the public namespace. Exclude _id and return only $literal=1,
        // with limit=1/batchSize=1, to discover T without transferring a potentially large source row.
        // The driver may request an extra wire batch slot to exhaust a limit-one cursor.
        try (ClientSession session = client.startSession(ClientSessionOptions.builder()
                .causallyConsistent(false).snapshot(true).build());
                MongoCursor<Document> cursor = snapshotDatabase(database, deadline).getCollection(collection)
                        .withReadPreference(properties.readPreferenceForSnapshot(preference)).find(session)
                        .projection(new Document("_id", 0)
                                .append("__doris_snapshot_probe__", new Document("$literal", 1)))
                        .limit(1).batchSize(1).iterator()) {
            cursor.hasNext();
            // This is cursor.atClusterTime captured by the driver's snapshot-session protocol;
            // operationTime, $clusterTime and the local wall clock are not substitutes for T.
            BsonTimestamp timestamp = session.getSnapshotTimestamp();
            if (timestamp == null) {
                throw new DorisConnectorException("MongoDB snapshot probe did not return a snapshot timestamp");
            }
            return timestamp;
        } catch (MongoException failure) {
            throw failure(failure);
        }
    }

    private MongoDatabase snapshotDatabase(String database, long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new DorisConnectorException("MongoDB snapshot planning deadline exceeded");
        }
        // CSOT covers server selection/network/command time and supplies the wire maxTimeMS;
        // hand-adding maxTimeMS to a raw command would create a duplicate BSON field.
        return metadataDatabase(database).withTimeout(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)),
                TimeUnit.MILLISECONDS);
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
