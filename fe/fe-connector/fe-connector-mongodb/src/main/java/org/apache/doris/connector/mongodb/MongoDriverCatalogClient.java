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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

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
    public MongoSnapshot.Source snapshotSource(long timeoutMs, Runnable checkCancelled) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            // hello identifies replica-set versus mongos routing and reports protocol capabilities.
            Document hello = snapshotDatabase("admin", deadline, checkCancelled).runCommand(new Document("hello", 1));
            checkSnapshot(deadline, checkCancelled);
            int wireVersion = hello.getInteger("maxWireVersion", 0);
            if ("isdbgrid".equals(hello.getString("msg"))) {
                // config.version._id=1 holds the clusterId shared by all routers of this deployment.
                // Inclusion projection keeps only that identity; no data rows are used to identify a cluster.
                Document version = snapshotDatabase("config", deadline, checkCancelled).getCollection("version")
                        .find(new Document("_id", 1)).projection(new Document("clusterId", 1)).limit(1).first();
                checkSnapshot(deadline, checkCancelled);
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
            Document config = snapshotDatabase("admin", deadline, checkCancelled)
                    .runCommand(new Document("replSetGetConfig", 1))
                    .get("config", Document.class);
            checkSnapshot(deadline, checkCancelled);
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
    public BsonTimestamp selectSnapshot(String database, String collection, String preference,
            long timeoutMs, Runnable checkCancelled) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        checkSnapshot(deadline, checkCancelled);
        // A constant projection bounds the probe payload independently of source document size.
        // MongoDB 7 time-series rewrites can ignore singleBatch. The driver's server-bound cursor
        // must close before its snapshot session, including when routers differ or selection fails.
        // snapshot(true) requests MongoDB-selected snapshot visibility; causal consistency is disabled
        // so this fresh probe does not add an afterClusterTime requirement from earlier operations.
        // find uses an empty filter on the public namespace. Exclude _id and return only $literal=1,
        // with limit=1/batchSize=1, to discover T without transferring a potentially large source row.
        // The driver may request an extra wire batch slot to exhaust a limit-one cursor.
        try (ClientSession session = client.startSession(ClientSessionOptions.builder()
                .causallyConsistent(false).snapshot(true).build())) {
            // Each blocking step must observe scan cancellation before starting the next one;
            // an in-flight synchronous driver call still uses the original CSOT deadline.
            try (MongoCursor<Document> cursor = snapshotDatabase(database, deadline, checkCancelled)
                    .getCollection(collection).withReadPreference(properties.readPreferenceForSnapshot(preference))
                    .find(session).projection(new Document("_id", 0)
                            .append("__doris_snapshot_probe__", new Document("$literal", 1)))
                    .limit(1).batchSize(1).iterator()) {
                checkSnapshot(deadline, checkCancelled);
                cursor.hasNext();
                checkSnapshot(deadline, checkCancelled);
                // This is cursor.atClusterTime captured by the driver's snapshot-session protocol;
                // operationTime, $clusterTime and the local wall clock are not substitutes for T.
                BsonTimestamp timestamp = session.getSnapshotTimestamp();
                if (timestamp == null) {
                    throw new DorisConnectorException("MongoDB snapshot probe did not return a snapshot timestamp");
                }
                return timestamp;
            }
        } catch (MongoException failure) {
            throw failure(failure);
        }
    }

    @Override
    public MongoSplitProbe probeSplits(MongoTableHandle table, String field, String preference,
            TMongoSnapshot snapshot, int rows, long timeoutMs,
            LongPredicate shouldSample, LongConsumer accountBytes, Runnable checkCancelled) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String index = null;
        long bytes = 0;
        try {
            var collection = probeDatabase(table.getDatabase(), deadline, checkCancelled)
                    .getCollection(table.getName());
            // Fetch the bounded index inventory together: one RTT per index can exhaust the
            // optional probe deadline before a later qualifying ordinary index is reached.
            try (MongoCursor<Document> indexes = collection.listIndexes().batchSize(128)
                    .timeoutMode(TimeoutMode.CURSOR_LIFETIME).iterator()) {
                int count = 0;
                while (true) {
                    // hasNext may issue getMore. Cancellation must be checked before that request,
                    // not only after it returns; the lifetime timeout also covers all cursor batches.
                    checkProbe(deadline, checkCancelled);
                    if (!indexes.hasNext()) {
                        break;
                    }
                    long batchBytes = 0;
                    do {
                        checkCancelled.run();
                        Document value = indexes.next();
                        batchBytes += MongoSplitPlan.encode(value.toBsonDocument()).length;
                        count++;
                        if (index == null && eligibleIndex(value, field)) {
                            index = value.getString("name");
                        }
                        // The driver already received this batch. Finding a usable first index
                        // cannot hide later large specs from either probe or statement accounting.
                    } while (indexes.available() > 0);
                    bytes += batchBytes;
                    accountBytes.accept(batchBytes);
                    checkProbe(deadline, checkCancelled);
                    if (count > 128 || bytes > 65536) {
                        throw new MongoRangePlanner.PlanningLimit();
                    }
                    if (index != null) {
                        break;
                    }
                }
            }
            if (index == null) {
                return new MongoSplitProbe(List.of(), -1, bytes, false);
            }
            Document stats = probeDatabase(table.getDatabase(), deadline, checkCancelled)
                    .runCommand(new Document("collStats", table.getName()).append("scale", 1));
            checkCancelled.run();
            // collStats includes index details and other metadata, not just its scalar size field.
            // Charge consumed replies before a delayed planner observes its optional deadline.
            long statsBytes = MongoSplitPlan.encode(stats.toBsonDocument()).length;
            bytes += statsBytes;
            accountBytes.accept(statsBytes);
            checkProbe(deadline, checkCancelled);
            if (bytes > 1024 * 1024) {
                throw new MongoRangePlanner.PlanningLimit();
            }
            Number size = stats.get("size", Number.class);
            long logicalBytes = size == null ? -1 : size.longValue();
            // Auto can already choose a complete single cursor from these statistics. Avoid
            // a random-sampling COLLSCAN that cannot change that decision, including unknown size.
            if (!shouldSample.test(logicalBytes)) {
                return new MongoSplitProbe(List.of(), logicalBytes, bytes, true);
            }
            List<BsonValue> samples = new ArrayList<>();
            // Sampling an index prefix puts almost all increasing keys into the final range.
            // Keep $sample first so MongoDB can use its random cursor, then retain only bounded
            // fixed-width keys. Statistics tune cost only; they never narrow source coverage.
            // Unsupported large strings/arrays become null before they can reach the FE.
            Document type = new Document("$type", "$" + field);
            Document supported = new Document("$in", List.of(type, List.of("int", "long", "objectId", "date")));
            Document projection = new Document("_id", 0).append("value",
                    new Document("$cond", List.of(supported, "$" + field, new Document("$literal", null))));
            // Do not shrink small-table requests below MongoDB's 5% random-cursor threshold.
            // Its historical random cursor can stall on concurrently rewritten records; a bounded
            // full sample of a small collection remains cheap and preserves the pinned timestamp.
            var pipeline = List.of(new Document("$sample", new Document("size", rows)),
                    new Document("$project", projection), new Document("$group", new Document("_id", null)
                            .append("values", new Document("$push", "$value"))));
            // Grouping at most 4096 fixed-width hints produces at most one small document.
            // batchSize=2 exhausts that reply, avoiding remote cursor cleanup on cancellation and
            // driver 5.14's invalid snapshot readConcern on killCursors. No getMore is needed.
            var command = new Document("aggregate", table.getName()).append("pipeline", pipeline)
                    .append("cursor", new Document("batchSize", 2))
                    .append("readConcern", new Document("level", "snapshot").append("atClusterTime",
                            new BsonTimestamp((int) snapshot.getTimestampSeconds(),
                                    (int) snapshot.getTimestampIncrement())));
            BsonDocument reply = probeDatabase(table.getDatabase(), deadline, checkCancelled)
                    .runCommand(command, properties.readPreferenceForSnapshot(preference), BsonDocument.class);
            checkCancelled.run();
            long replyBytes = MongoSplitPlan.encode(reply).length;
            bytes += replyBytes;
            accountBytes.accept(replyBytes);
            checkProbe(deadline, checkCancelled);
            if (bytes > 1024 * 1024) {
                throw new MongoRangePlanner.PlanningLimit();
            }
            for (BsonValue batch : reply.getDocument("cursor").getArray("firstBatch")) {
                for (BsonValue value : batch.asDocument().getArray("values")) {
                    checkProbe(deadline, checkCancelled);
                    if (samples.size() >= rows) {
                        throw new MongoRangePlanner.PlanningLimit();
                    }
                    samples.add(value);
                }
            }
            checkProbe(deadline, checkCancelled);
            return new MongoSplitProbe(samples, logicalBytes, bytes, true);
        } catch (MongoException error) {
            checkCancelled.run();
            // Only the optional planning deadline can trigger auto fallback. Permission denial,
            // expired snapshot history, replacement, and transport errors must fail the statement.
            // A thread can resume after its deadline while handling an earlier BadValue/permission
            // failure. Elapsed time alone must not disguise that source error as optional timeout.
            if (error instanceof MongoExecutionTimeoutException || error instanceof MongoOperationTimeoutException) {
                throw new MongoRangePlanner.PlanningLimit();
            }
            throw failure(error);
        }
    }

    static boolean eligibleIndex(Document index, String field) {
        Document keys = index.get("key", Document.class);
        if (keys == null || keys.isEmpty() || !field.equals(keys.keySet().iterator().next())
                || !(keys.get(field) instanceof Number direction) || Math.abs(direction.intValue()) != 1
                || Boolean.TRUE.equals(index.getBoolean("sparse")) || index.containsKey("partialFilterExpression")
                || Boolean.TRUE.equals(index.getBoolean("hidden")) || index.containsKey("buildUUID")) {
            return false;
        }
        // A TEXT suffix makes prefix inequalities unusable; wildcard suffixes can also make
        // coverage incomplete without sparse:true. Only ordinary B-tree components qualify.
        for (var entry : keys.entrySet()) {
            if ("$**".equals(entry.getKey()) || entry.getKey().endsWith(".$**")
                    || !(entry.getValue() instanceof Number value) || Math.abs(value.doubleValue()) != 1) {
                return false;
            }
        }
        Document collation = index.get("collation", Document.class);
        return collation == null || "simple".equals(collation.getString("locale"));
    }

    private MongoDatabase probeDatabase(String database, long deadline, Runnable checkCancelled) {
        checkProbe(deadline, checkCancelled);
        return metadataDatabase(database).withTimeout(Math.max(1,
                TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())), TimeUnit.MILLISECONDS);
    }

    private static void checkProbe(long deadline, Runnable checkCancelled) {
        checkCancelled.run();
        if (Thread.currentThread().isInterrupted()) {
            throw new java.util.concurrent.CancellationException("MongoDB split planning interrupted");
        }
        if (System.nanoTime() >= deadline) {
            throw new MongoRangePlanner.PlanningLimit();
        }
    }

    private static void checkSnapshot(long deadline, Runnable checkCancelled) {
        checkCancelled.run();
        if (Thread.currentThread().isInterrupted()) {
            throw new java.util.concurrent.CancellationException("MongoDB snapshot planning interrupted");
        }
        if (System.nanoTime() >= deadline) {
            throw new DorisConnectorException("MongoDB snapshot planning deadline exceeded");
        }
    }

    private MongoDatabase snapshotDatabase(String database, long deadline, Runnable checkCancelled) {
        checkSnapshot(deadline, checkCancelled);
        // CSOT covers server selection/network/command time and supplies the wire maxTimeMS;
        // hand-adding maxTimeMS to a raw command would create a duplicate BSON field.
        return metadataDatabase(database).withTimeout(Math.max(1,
                TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())),
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
