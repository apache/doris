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

import com.mongodb.ConnectionString;
import com.mongodb.ReadPreference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class MongoCatalogPropertiesTest {
    @Test
    void snapshotReadConcernIsAnExplicitCatalogOrStatementPolicy() {
        var defaults = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
        Assertions.assertEquals("snapshot", Assertions.assertDoesNotThrow(() -> defaults.resolveReadConcern("SNAPSHOT")));
        var catalog = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.read_concern", "snapshot"));
        Assertions.assertEquals("snapshot", catalog.resolveReadConcern("inherit"));
    }

    @Test
    void appliesBoundedDefaultsAndRedactsDiagnostics() {
        MongoCatalogProperties properties = MongoCatalogProperties.of(Map.of(
                "mongodb.uri", "mongodb://localhost:27017",
                "mongodb.username", "fixture_reader", "mongodb.password", "fixture_secret"));
        Assertions.assertEquals("auto", properties.getSchemaMode());
        Assertions.assertEquals("primary", properties.getReadPreference());
        Assertions.assertEquals("majority", properties.getReadConcern());
        Assertions.assertFalse(properties.toString().contains("fixture_secret"));
        Assertions.assertFalse(properties.toString().contains("mongodb://"));
    }

    @Test
    void preservesUriReadPreferenceTagsAndMaxStaleness() {
        String uri = "mongodb://localhost:27017/?readPreference=secondary"
                + "&readPreferenceTags=region:analytics&readPreferenceTags=&maxStalenessSeconds=90";
        MongoCatalogProperties properties = MongoCatalogProperties.of(Map.of(
                "mongodb.uri", uri, "mongodb.read_preference", "nearest"));
        Assertions.assertEquals("secondary", properties.getReadPreference());
        Assertions.assertEquals(new ConnectionString(uri).getReadPreference(),
                properties.clientSettings().getReadPreference());
    }

    @Test
    void snapshotProbeRetainsRoutingCriteriaWhenReplacingOnlyTheMode() {
        String uri = "mongodb://localhost:27017/?readPreference=secondary"
                + "&readPreferenceTags=region:analytics&readPreferenceTags=&maxStalenessSeconds=90";
        var properties = MongoCatalogProperties.of(Map.of("mongodb.uri", uri));
        var inherited = new ConnectionString(uri).getReadPreference();
        Assertions.assertEquals(inherited, properties.readPreferenceForSnapshot("inherit"));
        var expected = inherited.toDocument();
        expected.put("mode", new org.bson.BsonString("nearest"));
        Assertions.assertEquals(expected, properties.readPreferenceForSnapshot("nearest").toDocument());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> properties.readPreferenceForSnapshot("primary"));
        var defaults = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017"));
        Assertions.assertEquals(ReadPreference.primary(), defaults.readPreferenceForSnapshot("inherit"));
        Assertions.assertEquals(ReadPreference.secondary(), defaults.readPreferenceForSnapshot("secondary"));
    }

    @Test
    void rejectsConflictingCredentialSources() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> MongoCatalogProperties.of(Map.of(
                "mongodb.uri", "mongodb://fixture_user:fixture_secret@localhost:27017",
                "mongodb.username", "another_fixture")));
    }

    @Test
    void malformedUriDoesNotLeakItsInput() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> MongoCatalogProperties.of(Map.of("mongodb.uri", "bad_fixture_secret")));
        Assertions.assertFalse(error.toString().contains("bad_fixture_secret"));
    }

    @Test
    void rejectsUnimplementedModesAndInvalidResourceBounds() {
        for (Map.Entry<String, String> invalid : Map.of(
                "mongodb.schema.mode", "inferred", "mongodb.type_mismatch", "skip",
                "mongodb.read_concern", "linearizable", "mongodb.pool_max_clients", "0",
                "mongodb.socket_timeout_ms", "-1", "mongodb.datetime_timezone", "Asia/Shanghai").entrySet()) {
            Map<String, String> properties = new HashMap<>();
            properties.put("mongodb.uri", "mongodb://localhost:27017");
            properties.put(invalid.getKey(), invalid.getValue());
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> MongoCatalogProperties.of(properties), invalid.getKey());
        }
    }

    @Test
    void boundsExplicitSchemaAndRejectsUnknownMongoProperties() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> MongoCatalogProperties.of(Map.of(
                "mongodb.uri", "mongodb://localhost:27017", "mongodb.unknown", "value")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> MongoCatalogProperties.of(Map.of(
                "mongodb.uri", "mongodb://localhost:27017", "mongodb.schema", "x".repeat(1_048_577))));
    }
}
