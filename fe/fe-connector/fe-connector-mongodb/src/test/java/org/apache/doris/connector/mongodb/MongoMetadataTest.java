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

import org.apache.doris.connector.spi.ConnectorTableSchema;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class MongoMetadataTest {
    private static final class RecordingClient implements MongoCatalogClient {
        private final List<Document> objects = new ArrayList<>(List.of(
                new Document("name", "records").append("type", "collection"),
                new Document("name", "projected").append("type", "view")
                        .append("options", new Document("viewOn", "records").append("pipeline", List.of())),
                new Document("name", "measurements").append("type", "timeseries")
                        .append("options", new Document("timeseries", new Document("timeField", "time")
                                .append("metaField", "meta"))),
                new Document("name", "system.buckets.measurements").append("type", "collection")));
        private int metadataRequests;

        @Override
        public List<String> databases() {
            return List.of("sample");
        }

        @Override
        public List<Document> collections(String database) {
            metadataRequests++;
            return objects;
        }

        @Override
        public void close() {
        }
    }

    private static MongoConnectorMetadata metadata(RecordingClient client) {
        return new MongoConnectorMetadata(client,
                MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017")));
    }

    @Test
    void exposesLogicalObjectsAndHidesPhysicalBuckets() {
        RecordingClient client = new RecordingClient();
        MongoConnectorMetadata metadata = metadata(client);
        Assertions.assertEquals(List.of("sample"), metadata.listDatabaseNames(null));
        Assertions.assertEquals(List.of("records", "projected", "measurements"),
                metadata.listTableNames(null, "sample"));
        Assertions.assertFalse(metadata.getTableHandle(null, "sample", "system.buckets.measurements").isPresent());
        for (String name : List.of("records", "projected", "measurements")) {
            MongoTableHandle handle = (MongoTableHandle) metadata.getTableHandle(null, "sample", name).orElseThrow();
            Assertions.assertTrue(handle.getUuid().isEmpty());
            ConnectorTableSchema schema = metadata.getTableSchema(null, handle);
            Assertions.assertFalse(schema.getColumns().isEmpty());
            Assertions.assertFalse(schema.getProperties().containsKey("mongodb.uri"));
        }
    }

    @Test
    void retainsViewAndTimeSeriesOptionsWithoutReadingBaseCollections() {
        RecordingClient client = new RecordingClient();
        MongoConnectorMetadata metadata = metadata(client);
        MongoTableHandle view = (MongoTableHandle) metadata.getTableHandle(null, "sample", "projected").orElseThrow();
        MongoTableHandle timeSeries = (MongoTableHandle) metadata.getTableHandle(null, "sample", "measurements")
                .orElseThrow();
        Assertions.assertEquals("view", view.getObjectKind());
        Assertions.assertEquals("records", view.getOptions().getString("viewOn"));
        Assertions.assertEquals("timeseries", timeSeries.getObjectKind());
        Assertions.assertEquals("time", timeSeries.getOptions().get("timeseries", Document.class)
                .getString("timeField"));
        Assertions.assertEquals(2, client.metadataRequests);
    }

    @Test
    void oldHandlesRemainPinnedWhenMetadataChanges() {
        RecordingClient client = new RecordingClient();
        MongoConnectorMetadata metadata = metadata(client);
        MongoTableHandle first = (MongoTableHandle) metadata.getTableHandle(null, "sample", "records").orElseThrow();
        client.objects.get(0).append("options", Document.parse("{\"validator\":{\"$jsonSchema\":{"
                + "\"properties\":{\"value\":{\"bsonType\":\"int\"}}}}}"));
        MongoTableHandle next = (MongoTableHandle) metadata.getTableHandle(null, "sample", "records").orElseThrow();
        Assertions.assertNotEquals(first.getSchema().getVersion(), next.getSchema().getVersion());
        Assertions.assertEquals("DOCUMENT", first.getSchema().getSource());
        Assertions.assertEquals("MONGODB_VALIDATOR", next.getSchema().getSource());
    }

    @Test
    void serializedHandleContainsStableValuesAndRestoresItsSchema() throws Exception {
        MongoTableHandle handle = (MongoTableHandle) metadata(new RecordingClient())
                .getTableHandle(null, "sample", "records").orElseThrow();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(handle);
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            MongoTableHandle restored = (MongoTableHandle) input.readObject();
            Assertions.assertEquals(handle.getSchema().getVersion(), restored.getSchema().getVersion());
            Assertions.assertEquals(handle.getSchema().getColumns(), restored.getSchema().getColumns());
        }
    }
}
