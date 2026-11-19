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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMongoScanDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class MongoScanPlanTest {
    static class Session implements ConnectorSession {
        private final Map<String, String> policies;

        Session() {
            this(Map.of("batch_size", "128", "query_timeout", "30"));
        }

        Session(Map<String, String> policies) {
            this.policies = policies;
        }

        @Override
        public String getQueryId() {
            return "fixture-query";
        }

        @Override
        public String getUser() {
            return "fixture-user";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en";
        }

        @Override
        public long getCatalogId() {
            return 42;
        }

        @Override
        public String getCatalogName() {
            return "fixture-catalog";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            throw new UnsupportedOperationException("Query policy must use session properties directly");
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Map.of();
        }

        @Override
        public Map<String, String> getSessionProperties() {
            return policies;
        }
    }

    private static MongoScanPlanProvider provider(MongoCatalogProperties properties) {
        return new MongoScanPlanProvider(properties, () -> {
            throw new AssertionError("Ordinary scans and admission failures must not open a snapshot probe");
        });
    }

    private static MongoCatalogProperties properties() {
        return MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.username", "fixture-user", "mongodb.password", "fixture-secret"));
    }

    private static TMongoScanDesc plan(MongoCatalogProperties properties, Map<String, String> policies) {
        MongoTableHandle handle = new MongoTableHandle("sample",
                new Document("name", "records").append("type", "collection"), properties);
        ConnectorScanRange range = provider(properties).planScan(new Session(policies),
                ConnectorScanRequest.builder(handle, List.of()).build()).get(0);
        TTableFormatFileDesc descriptor = new TTableFormatFileDesc();
        range.populateRangeParams(descriptor, new TFileRangeDesc());
        return descriptor.getMongodbParams();
    }

    @Test
    void snapshotScansRequireALiveStatementScope() {
        var failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> plan(properties(), Map.of("mongodb_read_concern", "snapshot")));
        Assertions.assertTrue(failure.getMessage().contains("statement scope"));
    }

    @Test
    void snapshotViewsAreRejectedBeforeOpeningAProbe() {
        var properties = properties();
        var view = new MongoTableHandle("sample", new Document("name", "view").append("type", "view"), properties);
        var failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider(properties).planScan(
                        new Session(Map.of("mongodb_read_concern", "snapshot")),
                        ConnectorScanRequest.builder(view, List.of()).build()));
        Assertions.assertTrue(failure.getMessage().contains("view"));
    }

    @Test
    void queryPoliciesOverrideCatalogAndRemainPinnedToTheirScan() {
        MongoCatalogProperties properties = properties();
        TMongoScanDesc overridden = plan(properties, Map.of("mongodb_read_preference", "SECONDARYPREFERRED",
                "mongodb_read_concern", "LOCAL"));
        Assertions.assertEquals("secondaryPreferred", overridden.getReadPreference());
        Assertions.assertEquals("local", overridden.getReadConcern());
        TMongoScanDesc inherited = plan(properties, Map.of("mongodb_read_preference", "inherit",
                "mongodb_read_concern", "inherit"));
        Assertions.assertEquals("primary", inherited.getReadPreference());
        Assertions.assertEquals("majority", inherited.getReadConcern());
        Assertions.assertEquals("secondaryPreferred", overridden.getReadPreference());
        Assertions.assertEquals(inherited.getConfigurationVersion(), overridden.getConfigurationVersion());
    }

    @Test
    void inheritsUriPoliciesAndRejectsPrimaryWithSecondaryCriteria() {
        MongoCatalogProperties properties = MongoCatalogProperties.of(Map.of("mongodb.uri",
                "mongodb://localhost:27017/?readPreference=secondary&readPreferenceTags=zone:east"
                        + "&maxStalenessSeconds=120&readConcernLevel=available"));
        TMongoScanDesc inherited = plan(properties, Map.of());
        Assertions.assertEquals("secondary", inherited.getReadPreference());
        Assertions.assertEquals("available", inherited.getReadConcern());
        TMongoScanDesc overridden = plan(properties, Map.of("mongodb_read_preference", "nearest",
                "mongodb_read_concern", "majority"));
        Assertions.assertEquals("nearest", overridden.getReadPreference());
        Assertions.assertEquals(properties.getUri(), overridden.getUri());
        Assertions.assertEquals("majority", overridden.getReadConcern());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> plan(properties, Map.of("mongodb_read_preference", "primary")));
    }

    @Test
    void validatesEachInheritedRoutingConstraintWithoutChangingCatalogSettings() {
        for (String suffix : List.of("&readPreferenceTags=zone:east", "&maxStalenessSeconds=120")) {
            MongoCatalogProperties properties = MongoCatalogProperties.of(Map.of("mongodb.uri",
                    "mongodb://localhost:27017/?readPreference=secondary" + suffix));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> plan(properties, Map.of("mongodb_read_preference", "primary")));
            Assertions.assertEquals("secondary", plan(properties, Map.of()).getReadPreference());
        }
        MongoCatalogProperties modeOnly = MongoCatalogProperties.of(Map.of("mongodb.uri",
                "mongodb://localhost:27017/?readPreference=secondary&maxStalenessSeconds=-1"));
        Assertions.assertEquals("primary",
                plan(modeOnly, Map.of("mongodb_read_preference", "primary")).getReadPreference());
        MongoCatalogProperties catalog = MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.read_preference", "nearest", "mongodb.read_concern", "local"));
        TMongoScanDesc inherited = plan(catalog, Map.of("mongodb_read_preference", "INHERIT",
                "mongodb_read_concern", "INHERIT"));
        Assertions.assertEquals("nearest", inherited.getReadPreference());
        Assertions.assertEquals("local", inherited.getReadConcern());
    }

    @Test
    void rejectsUnsupportedQueryPoliciesBeforeDispatch() {
        for (String preference : List.of("", "invalid", "secondary_preferred")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> plan(properties(), Map.of("mongodb_read_preference", preference)));
        }
        for (String concern : List.of("", "invalid", "linearizable")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> plan(properties(), Map.of("mongodb_read_concern", concern)));
        }
        for (String preference : List.of("primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest")) {
            Assertions.assertEquals(preference,
                    plan(properties(), Map.of("mongodb_read_preference", preference)).getReadPreference());
        }
        for (String concern : List.of("local", "majority", "available")) {
            Assertions.assertEquals(concern,
                    plan(properties(), Map.of("mongodb_read_concern", concern)).getReadConcern());
        }
    }

    @Test
    void emitsOneStableNativeRangeForEachLogicalObjectKind() {
        MongoCatalogProperties properties = properties();
        MongoScanPlanProvider provider = provider(properties);
        for (String kind : List.of("collection", "view", "timeseries")) {
            MongoTableHandle handle = new MongoTableHandle("sample",
                    new Document("name", "records").append("type", kind), properties);
            ConnectorScanRequest request = ConnectorScanRequest.builder(handle, List.of()).build();
            List<ConnectorScanRange> ranges = provider.planScan(new Session(), request);
            Assertions.assertEquals(1, ranges.size());
            ConnectorScanRange range = ranges.get(0);
            Assertions.assertEquals("mongodb", range.getTableFormatType());
            Assertions.assertEquals("mongodb", range.getFileFormat());
            Assertions.assertTrue(range.isNativeReadRange());
            Assertions.assertTrue(range.getProperties().isEmpty());
            TTableFormatFileDesc descriptor = new TTableFormatFileDesc();
            range.populateRangeParams(descriptor, new TFileRangeDesc());
            Assertions.assertFalse(descriptor.isSetJdbcParams());
            Assertions.assertEquals(1, descriptor.getMongodbParams().getVersion());
            Assertions.assertEquals(kind, descriptor.getMongodbParams().getObjectKind());
            Assertions.assertEquals(handle.getSchema().getVersion(), descriptor.getMongodbParams().getSchemaVersion());
            Assertions.assertEquals(128, descriptor.getMongodbParams().getBatchSize());
            Assertions.assertEquals(30000, descriptor.getMongodbParams().getTimeoutMs());
        }
    }

    @Test
    void explainContainsNoCredentialsAndDoesNotClaimPredicatePushdown() {
        MongoCatalogProperties properties = properties();
        MongoTableHandle handle = new MongoTableHandle("sample",
                new Document("name", "records").append("type", "collection"), properties);
        MongoScanPlanProvider provider = provider(properties);
        ScanNodePropertiesResult result = provider.getScanNodePropertiesResult(new Session(), handle,
                List.of(), Optional.empty());
        Assertions.assertFalse(result.hasConjunctTracking());
        Assertions.assertFalse(result.getProperties().toString().contains("fixture-secret"));
        Assertions.assertFalse(result.getProperties().toString().contains("mongodb://"));
    }

    @Test
    void columnHandlesRetainBsonIdentityAndRejectAnotherSchemaVersion() {
        MongoCatalogProperties properties = properties();
        MongoTableHandle handle = new MongoTableHandle("sample",
                new Document("name", "records").append("type", "collection"), properties);
        MongoSchema.Binding binding = handle.getSchema().getBindings().get(0);
        MongoColumnHandle current = new MongoColumnHandle(binding, handle.getSchema().getVersion());
        MongoColumnHandle stale = new MongoColumnHandle(binding, "another-schema");
        Assertions.assertNotEquals(current, stale);
        Assertions.assertEquals(current, new MongoColumnHandle(binding, handle.getSchema().getVersion()));
        Assertions.assertTrue(current.getBindingJson().contains("bson_types"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> provider(properties)
                .planScan(new Session(), ConnectorScanRequest.builder(handle, List.of(stale)).build()));
    }

    @Test
    void nullPolicyEnablesCandidatesWithoutConsumingResiduals() {
        MongoCatalogProperties properties = MongoCatalogProperties.of(Map.of(
                "mongodb.uri", "mongodb://localhost:27017", "mongodb.type_mismatch", "null",
                "mongodb.schema", "{\"sample.records\":{\"columns\":[{\"name\":\"seq\","
                        + "\"path\":[\"seq\"],\"doris_type\":\"BIGINT\",\"bson_types\":[\"long\"]}]}}"));
        MongoTableHandle handle = new MongoTableHandle("sample",
                new Document("name", "records").append("type", "collection"), properties);
        var expression = new ConnectorComparison(ConnectorComparison.Operator.GE,
                new ConnectorColumnRef("seq", ConnectorType.of("BIGINT")), ConnectorLiteral.ofLong(10));
        var result = provider(properties).getScanNodePropertiesResult(new Session(), handle,
                List.of(new MongoColumnHandle(handle.getSchema().getBindings().get(0),
                        handle.getSchema().getVersion())), Optional.of(expression));
        Assertions.assertEquals("SUPERSET", result.getProperties().get("predicate_pushdown"));
        Assertions.assertFalse(result.hasConjunctTracking());
    }

    private MongoCatalogProperties typedProperties(String policy, List<String> path) {
        Document column = new Document("name", "seq").append("path", path)
                .append("doris_type", "BIGINT").append("bson_types", List.of("long"));
        return MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.type_mismatch", policy, "mongodb.schema",
                new Document("sample.records", new Document("columns", List.of(column))).toJson()));
    }

    private TMongoScanDesc descriptor(MongoCatalogProperties properties, String kind, ConnectorSession session,
            boolean columns) {
        MongoTableHandle table = new MongoTableHandle("sample",
                new Document("name", "records").append("type", kind), properties);
        var binding = table.getSchema().getBindings().get(0);
        var filter = new ConnectorComparison(ConnectorComparison.Operator.GE,
                new ConnectorColumnRef("seq", binding.column().getType()), ConnectorLiteral.ofLong(10));
        var request = ConnectorScanRequest.builder(table,
                columns ? List.of(new MongoColumnHandle(binding, table.getSchema().getVersion())) : List.of())
                .filter(Optional.of(filter)).build();
        var range = provider(properties).planScan(session, request).get(0);
        var result = new TTableFormatFileDesc();
        range.populateRangeParams(result, new TFileRangeDesc());
        return result.getMongodbParams();
    }

    @Test
    void scanRangeCarriesBsonRatherThanRelaxedJsonOrUntypedNumbers() {
        var descriptor = descriptor(typedProperties("null", List.of("Source")), "collection", new Session(), true);
        var filter = new RawBsonDocument(descriptor.getFilterBson());
        Assertions.assertEquals(10, filter.getDocument("Source").getInt64("$gte").getValue());
        var projection = new RawBsonDocument(descriptor.getProjectionBson());
        Assertions.assertEquals(1, projection.getInt32("Source").getValue());
        Assertions.assertEquals(0, projection.getInt32("_id").getValue());
    }

    @Test
    void strictAndViewScansCannotHideHistoricalTypeFailures() {
        Assertions.assertFalse(descriptor(typedProperties("error", List.of("seq")),
                "collection", new Session(), true).isSetFilterBson());
        Assertions.assertFalse(descriptor(typedProperties("null", List.of("seq")),
                "view", new Session(), true).isSetFilterBson());
    }

    @Test
    void statementSwitchesDisableRemoteWorkIndependently() {
        var session = new Session() {
            @Override
            public Map<String, String> getSessionProperties() {
                return Map.of("enable_mongodb_predicate_pushdown", "false",
                        "enable_mongodb_projection_pushdown", "false");
            }
        };
        var descriptor = descriptor(typedProperties("null", List.of("seq")), "collection", session, true);
        Assertions.assertFalse(descriptor.isSetFilterBson());
        Assertions.assertFalse(descriptor.isSetProjectionBson());
    }

    @Test
    void projectionKeepsWholeRootsForPathErrorsAndPrunedStructChildren() {
        var descriptor = descriptor(typedProperties("error", List.of("Parent", "Child")),
                "collection", new Session(), true);
        var projection = new RawBsonDocument(descriptor.getProjectionBson());
        Assertions.assertEquals(1, projection.getInt32("Parent").getValue());
        Assertions.assertFalse(projection.containsKey("Parent.Child"));
        Assertions.assertFalse(descriptor.isSetFilterBson());
    }

    @Test
    void literalDotAndWholeDocumentBindingsDisableProjection() {
        Assertions.assertFalse(descriptor(typedProperties("null", List.of("literal.dot")),
                "collection", new Session(), true).isSetProjectionBson());
        var table = new MongoTableHandle("sample", new Document("name", "records").append("type", "collection"),
                properties());
        var request = ConnectorScanRequest.builder(table, List.of(new MongoColumnHandle(
                table.getSchema().getBindings().get(0), table.getSchema().getVersion()))).build();
        var result = new TTableFormatFileDesc();
        provider(properties()).planScan(new Session(), request).get(0)
                .populateRangeParams(result, new TFileRangeDesc());
        Assertions.assertFalse(result.getMongodbParams().isSetProjectionBson());
    }

    @Test
    void countOnlyProjectionPreservesOneDocumentPerSourceRow() {
        var d = descriptor(typedProperties("null", List.of("seq")), "collection", new Session(), false);
        Assertions.assertEquals(1, new RawBsonDocument(d.getProjectionBson()).getInt32("_id").getValue());
    }

    @Test
    void strictSqlCastErrorsAlsoKeepCandidatesLocal() {
        var session = new Session() {
            @Override
            public Map<String, String> getSessionProperties() {
                return Map.of("enable_strict_cast", "true");
            }
        };
        Assertions.assertFalse(descriptor(typedProperties("null", List.of("seq")),
                "collection", session, true).isSetFilterBson());
    }

}
