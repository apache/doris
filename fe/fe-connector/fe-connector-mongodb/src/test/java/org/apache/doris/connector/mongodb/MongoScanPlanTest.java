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
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class MongoScanPlanTest {
    private static final class Session implements ConnectorSession {
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
            return Map.of("batch_size", "128", "query_timeout", "30");
        }
    }

    private static MongoCatalogProperties properties() {
        return MongoCatalogProperties.of(Map.of("mongodb.uri", "mongodb://localhost:27017",
                "mongodb.username", "fixture-user", "mongodb.password", "fixture-secret"));
    }

    @Test
    void emitsOneStableNativeRangeForEachLogicalObjectKind() {
        MongoCatalogProperties properties = properties();
        MongoScanPlanProvider provider = new MongoScanPlanProvider(properties);
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
        MongoScanPlanProvider provider = new MongoScanPlanProvider(properties);
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
        Assertions.assertThrows(IllegalArgumentException.class, () -> new MongoScanPlanProvider(properties)
                .planScan(new Session(), ConnectorScanRequest.builder(handle, List.of(stale)).build()));
    }

}
