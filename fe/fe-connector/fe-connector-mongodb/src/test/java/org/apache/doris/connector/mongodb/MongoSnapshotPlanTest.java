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
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMongoScanDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

class MongoSnapshotPlanTest {
    static class Scope implements ConnectorStatementScope {
        private final Map<String, Object> values = new ConcurrentHashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return (T) values.computeIfAbsent(key, ignored -> loader.get());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getIfPresent(String key) {
            return (T) values.get(key);
        }
    }

    static class Client implements MongoCatalogClient {
        MongoSnapshot.Source source = new MongoSnapshot.Source("replica_set", "000000000000000000000001", 1, 25);
        BsonTimestamp time = new BsonTimestamp(1700000000, 7);
        int identities;
        int selections;
        String preference;

        @Override
        public MongoSnapshot.Source snapshotSource(long timeoutMs) {
            identities++;
            return source;
        }

        @Override
        public BsonTimestamp selectSnapshot(String database, String collection, String readPreference, long timeoutMs) {
            selections++;
            preference = readPreference;
            return time;
        }

        @Override
        public List<String> databases() {
            return List.of();
        }

        @Override
        public List<Document> collections(String database) {
            return List.of();
        }

        @Override
        public void close() {
        }
    }

    private static MongoScanPlanTest.Session session(Scope scope, long catalog, String concern) {
        return new MongoScanPlanTest.Session(Map.of("mongodb_read_concern", concern,
                "mongodb_read_preference", "secondaryPreferred")) {
            @Override
            public long getCatalogId() {
                return catalog;
            }

            @Override
            public ConnectorStatementScope getStatementScope() {
                return scope;
            }
        };
    }

    private static MongoCatalogProperties properties() {
        return MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
    }

    private static MongoTableHandle collection(String name, MongoCatalogProperties properties) {
        return new MongoTableHandle("sample", new Document("name", name).append("type", "collection")
                .append("info", new Document("uuid", new Binary((byte) 4, new byte[16]))), properties);
    }

    private static TMongoScanDesc plan(MongoScanPlanProvider provider, MongoScanPlanTest.Session session,
            MongoTableHandle table) {
        var range = provider.planScan(session, ConnectorScanRequest.builder(table, List.of()).build()).get(0);
        var format = new TTableFormatFileDesc();
        range.populateRangeParams(format, new TFileRangeDesc());
        return format.getMongodbParams();
    }

    @Test
    void collectionUuidSurvivesExtendedJsonPinning() {
        byte[] bytes = new byte[16];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        var metadata = new Document("name", "records").append("type", "collection")
                .append("info", new Document("uuid", new Binary((byte) 4, bytes)));
        Assertions.assertEquals("000102030405060708090a0b0c0d0e0f",
                new MongoTableHandle("sample", metadata, properties()).getUuid());
    }

    @Test
    void explainAndScansAcrossCatalogsShareOneOwnedTimestamp() {
        var scope = new Scope();
        var properties = properties();
        var first = new Client();
        var second = new Client();
        second.time = new BsonTimestamp(1700000010, 99);
        var firstProvider = new MongoScanPlanProvider(properties, () -> first);
        var secondProvider = new MongoScanPlanProvider(properties, () -> second);
        var firstTable = collection("first", properties);
        var secondTable = collection("second", properties);
        var firstSession = session(scope, 1, "snapshot");
        var secondSession = session(scope, 2, "snapshot");
        var explanation = firstProvider.getScanNodeProperties(firstSession, firstTable, List.of(),
                java.util.Optional.empty());
        Assertions.assertEquals("1700000000:7", explanation.get("snapshot"));
        var left = plan(firstProvider, firstSession, firstTable);
        var right = plan(secondProvider, secondSession, secondTable);
        Assertions.assertEquals(left.getSnapshot(), right.getSnapshot());
        Assertions.assertEquals("snapshot", left.getReadConcern());
        Assertions.assertEquals("secondaryPreferred", first.preference);
        Assertions.assertEquals(1, first.identities);
        Assertions.assertEquals(1, second.identities);
        Assertions.assertEquals(1, first.selections);
        Assertions.assertEquals(0, second.selections);
        left.getSnapshot().setTimestampSeconds(1);
        Assertions.assertEquals(1700000000, plan(secondProvider, secondSession, secondTable)
                .getSnapshot().getTimestampSeconds());
    }

    @Test
    void crossCatalogIdentityChangesFailBeforeDispatch() {
        var scope = new Scope();
        var properties = properties();
        var first = new Client();
        var second = new Client();
        second.source = new MongoSnapshot.Source("replica_set", "000000000000000000000002", 1, 25);
        plan(new MongoScanPlanProvider(properties, () -> first), session(scope, 1, "snapshot"),
                collection("records", properties));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> plan(new MongoScanPlanProvider(properties, () -> second), session(scope, 2, "snapshot"),
                        collection("records", properties)));
        Assertions.assertEquals(0, second.selections);
    }

    @Test
    void sourcePinsAndTimesAreScopedToOneStatement() {
        var properties = properties();
        var client = new Client();
        var provider = new MongoScanPlanProvider(properties, () -> client);
        var table = collection("records", properties);
        var first = plan(provider, session(new Scope(), 1, "snapshot"), table);
        client.time = new BsonTimestamp(1700000010, 1);
        var second = plan(provider, session(new Scope(), 1, "snapshot"), table);
        Assertions.assertNotEquals(first.getSnapshot().getTimestampSeconds(),
                second.getSnapshot().getTimestampSeconds());
        Assertions.assertEquals(2, client.identities);
        Assertions.assertEquals(2, client.selections);
    }

    @Test
    void timeSeriesPinsItsPublicDefinitionWithoutReadingBuckets() {
        var properties = properties();
        var client = new Client();
        var table = new MongoTableHandle("sample", new Document("name", "measurements").append("type", "timeseries")
                .append("options", new Document("timeseries", new Document("timeField", "observed"))), properties);
        var descriptor = plan(new MongoScanPlanProvider(properties, () -> client),
                session(new Scope(), 1, "snapshot"), table);
        Assertions.assertEquals("measurements", descriptor.getCollection());
        Assertions.assertTrue(descriptor.getSnapshot().getTimeseriesOptionsJson().contains("observed"));
    }

    @Test
    void ordinaryCollectionSnapshotRequiresAnIdentity() {
        var properties = properties();
        var client = new Client();
        var table = new MongoTableHandle("sample", new Document("name", "records").append("type", "collection"),
                properties);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> plan(new MongoScanPlanProvider(properties, () -> client),
                        session(new Scope(), 1, "snapshot"), table));
        Assertions.assertEquals(0, client.identities);
        Assertions.assertEquals(0, client.selections);
    }

    @Test
    void timestampComponentsUseUnsignedBsonWidths() {
        var properties = properties();
        var client = new Client();
        client.time = new BsonTimestamp(-1, -1);
        var descriptor = plan(new MongoScanPlanProvider(properties, () -> client),
                session(new Scope(), 1, "snapshot"), collection("records", properties));
        Assertions.assertEquals(4294967295L, descriptor.getSnapshot().getTimestampSeconds());
        Assertions.assertEquals(4294967295L, descriptor.getSnapshot().getTimestampIncrement());
    }
}
