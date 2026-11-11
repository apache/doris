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

import org.apache.doris.connector.spi.ConnectorType;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class MongoSchemaTest {
    private static Document collection(String fields) {
        return Document.parse(("{name:'records',type:'collection',options:{validator:{$jsonSchema:"
                + "{bsonType:'object',properties:" + fields + "}}}}").replace('\'', '"'));
    }

    private static MongoSchema resolve(String mode, Document metadata) {
        return MongoSchema.resolve("sample.records", metadata,
                MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                        "mongodb.schema.mode", mode)));
    }

    @Test
    void mapsValidatorWithoutPromisingHistoricalNonNullValues() {
        MongoSchema schema = resolve("validator", collection("{_id:{bsonType:'objectId'},"
                + "sequence:{bsonType:['int','long','null']},label:{bsonType:'string'}}"));
        Assertions.assertEquals("MONGODB_VALIDATOR", schema.getSource());
        Assertions.assertEquals(ConnectorType.of("VARBINARY", 12, -1), schema.getColumns().get(0).getType());
        Assertions.assertEquals(ConnectorType.of("BIGINT"), schema.getColumns().get(1).getType());
        Assertions.assertTrue(schema.getColumns().stream().allMatch(c -> c.isNullable()));
    }

    @Test
    void mapsNestedObjectsAndArraysWithoutSampling() {
        MongoSchema schema = resolve("validator", collection("{profile:{bsonType:'object',properties:"
                + "{City:{bsonType:'string'}}},scores:{bsonType:'array',items:{bsonType:'long'}}}"));
        Assertions.assertEquals(List.of("city"), schema.getColumns().get(0).getType().getFieldNames());
        Assertions.assertEquals(ConnectorType.arrayOf(ConnectorType.of("BIGINT")),
                schema.getColumns().get(1).getType());
        Assertions.assertTrue(schema.toBindingsJson().contains("City"));
    }

    @Test
    void unsupportedValidatorFallsBackAsAWhole() {
        Document metadata = collection("{known:{bsonType:'int'},unknown:{anyOf:[{bsonType:'int'}]}}");
        MongoSchema schema = resolve("auto", metadata);
        Assertions.assertEquals("DOCUMENT", schema.getSource());
        Assertions.assertEquals(List.of("_mongo_ejson", "_mongo_bson"),
                schema.getColumns().stream().map(c -> c.getName()).collect(java.util.stream.Collectors.toList()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> resolve("validator", metadata));
    }

    @Test
    void tupleArrayValidatorFallsBackAsAWhole() {
        Document metadata = collection("{known:{bsonType:'int'},values:{bsonType:'array',"
                + "items:[{bsonType:'int'},{bsonType:'string'}]}}");
        Assertions.assertEquals("DOCUMENT", resolve("auto", metadata).getSource());
        Assertions.assertThrows(IllegalArgumentException.class, () -> resolve("validator", metadata));
    }

    @Test
    void schemaBearingKeywordsNeverPublishPartialFields() {
        for (String keyword : List.of(
                "{patternProperties:{'^extra$':{bsonType:'string'}}}",
                "{dependencies:{known:{properties:{extra:{bsonType:'string'}}}}}",
                "{additionalProperties:{bsonType:'string'}}",
                "{not:{properties:{extra:{bsonType:'string'}}}}")) {
            Document extension = Document.parse(keyword.replace('\'', '"'));
            Document root = collection("{known:{bsonType:'int'}}");
            root.get("options", Document.class).get("validator", Document.class)
                    .get("$jsonSchema", Document.class).putAll(extension);
            Assertions.assertEquals("DOCUMENT", resolve("auto", root).getSource(), keyword);
            Assertions.assertThrows(IllegalArgumentException.class, () -> resolve("validator", root), keyword);
            Document nested = collection("{profile:{bsonType:'object',properties:{known:{bsonType:'int'}}}}");
            nested.get("options", Document.class).get("validator", Document.class)
                    .get("$jsonSchema", Document.class).get("properties", Document.class)
                    .get("profile", Document.class).putAll(extension);
            Assertions.assertEquals("DOCUMENT", resolve("auto", nested).getSource(), keyword);
            Assertions.assertThrows(IllegalArgumentException.class, () -> resolve("validator", nested), keyword);
        }
    }

    @Test
    void ordinaryObjectConstraintsKeepTypedSchema() {
        Document root = collection("{known:{bsonType:'int'}}");
        root.get("options", Document.class).get("validator", Document.class)
                .get("$jsonSchema", Document.class).append("additionalProperties", false)
                .append("patternProperties", new Document()).append("dependencies", new Document("known", List.of("known")));
        Assertions.assertEquals("MONGODB_VALIDATOR", resolve("auto", root).getSource());
    }

    @Test
    void viewsAndTimeSeriesUseTheirOutputSchema() {
        Document metadata = collection("{underlying:{bsonType:'int'}}");
        for (String kind : List.of("view", "timeseries")) {
            metadata.put("type", kind);
            Assertions.assertEquals("DOCUMENT", resolve("auto", metadata).getSource());
            Assertions.assertThrows(IllegalArgumentException.class, () -> resolve("validator", metadata));
        }
    }

    @Test
    void explicitPathsPreserveLiteralDotsAndDeclaredLogicalTypes() {
        String manifest = "{\"sample.records\":{\"columns\":["
                + "{\"name\":\"identifier\",\"path\":[\"_id\"],\"bson_types\":[\"objectId\"],"
                + "\"doris_type\":\"VARBINARY(12)\"},"
                + "{\"name\":\"amount\",\"path\":[\"a.b\"],\"bson_types\":[\"decimal\"],"
                + "\"doris_type\":\"DECIMAL(20,2)\"}]}}";
        MongoSchema schema = MongoSchema.resolve("sample.records", collection("{}"),
                MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                        "mongodb.schema", manifest)));
        Assertions.assertEquals("DORIS_EXPLICIT", schema.getSource());
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 20, 2), schema.getColumns().get(1).getType());
        Assertions.assertEquals(List.of("a.b"), schema.getBindings().get(1).getPath());
    }

    @Test
    void rejectsAmbiguousColumnNamesAndUnsupportedExplicitTypes() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolve("validator", collection("{A:{bsonType:'int'},a:{bsonType:'int'}}")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MongoSchema.parseDorisType("DECIMAL(39,2)"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MongoSchema.parseDorisType("DATETIME(7)"));
    }

    @Test
    void definitionChangesProduceNewPinnedVersions() {
        Document metadata = collection("{n:{bsonType:'int'}}");
        MongoSchema first = resolve("auto", metadata);
        metadata.get("options", Document.class).put("collation", new Document("locale", "en"));
        Assertions.assertNotEquals(first.getVersion(), resolve("auto", metadata).getVersion());
        Assertions.assertEquals(first.getVersion(), resolve("auto", collection("{n:{bsonType:'int'}}")).getVersion());
    }

    @Test
    void schemasDoNotInventKeysForSourceDocuments() {
        MongoSchema schema = MongoSchema.resolve("sample.records",
                new Document("name", "records").append("type", "view"),
                MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017")));
        Assertions.assertTrue(schema.getColumns().stream().noneMatch(c -> c.isKey()));
    }

}
