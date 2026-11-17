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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class HiveConnectorDeltaSiblingTest {
    private static final String PARQUET = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

    @Test
    void detectsDeltaBeforeGenericHiveFormatsAndPreservesOtherFormats() {
        for (String provider : List.of("delta", "DELTA", "DeLtA")) {
            Assertions.assertEquals(HiveTableType.DELTA, HiveTableFormatDetector.detect(
                    table(Map.of("spark.sql.sources.provider", provider))));
        }
        Assertions.assertEquals(HiveTableType.HIVE, HiveTableFormatDetector.detect(table(Map.of())));
        Assertions.assertEquals(HiveTableType.ICEBERG, HiveTableFormatDetector.detect(
                table(Map.of("table_type", "ICEBERG", "spark.sql.sources.provider", "delta"))));
        Assertions.assertEquals(HiveTableType.HUDI, HiveTableFormatDetector.detect(
                table(Map.of("flink.connector", "hudi", "spark.sql.sources.provider", "delta"))));
    }

    @Test
    void forcesHmsAndCopiesAllConnectionAndCacheProperties() {
        Map<String, String> input = new HashMap<>(Map.of(
                "deltalake.catalog.type", "filesystem", "uri", "thrift://host:9083",
                "s3.endpoint", "https://storage", "dfs.nameservices", "ns1",
                "hive.metastore.client.principal", "hive/_HOST@REALM",
                "meta.cache.deltalake.latest.ttl-second", "5"));
        Map<String, String> result = DeltaSiblingProperties.synthesize(input);
        Assertions.assertEquals("hms", result.remove("deltalake.catalog.type"));
        Assertions.assertEquals("filesystem", input.remove("deltalake.catalog.type"));
        Assertions.assertEquals(input, result);
        result.clear();
        Assertions.assertFalse(input.isEmpty());
    }

    @Test
    void routesTheRawHandleSchemaAndScanThroughOneStatementMetadata() throws Exception {
        DeltaContext context = new DeltaContext();
        try (HiveConnector gateway = gateway(context)) {
            HiveConnectorMetadata metadata = gateway.newMetadata(hms(table(Map.of("spark.sql.sources.provider", "delta"))));
            ConnectorSession session = new ScopeSession(0, "delta", new TestStatementScope());
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "t").orElseThrow();
            Assertions.assertSame(context.delta.handle, handle);
            Assertions.assertEquals(SiblingOwner.DELTA_LABEL, gateway.resolveSiblingOwnerLabeled(handle).label());
            Assertions.assertSame(context.delta.schema, metadata.getTableSchema(session, handle));
            Assertions.assertSame(context.delta.scans, gateway.getScanPlanProvider(handle));
            Assertions.assertEquals(1, context.delta.metadataCalls);
            Assertions.assertEquals(1, context.builds);
        }
    }

    @Test
    void plainHiveAndRefreshNeverBuildUnneededPlugins() throws Exception {
        DeltaContext context = new DeltaContext();
        try (HiveConnector gateway = gateway(context)) {
            HiveConnectorMetadata metadata = gateway.newMetadata(hms(table(Map.of())));
            Assertions.assertInstanceOf(HiveTableHandle.class, metadata.getTableHandle(
                    new ScopeSession(0, "hive", new TestStatementScope()), "db", "t").orElseThrow());
            gateway.invalidateTable("db", "t");
            gateway.invalidateDb("db");
            gateway.invalidateAll();
            Assertions.assertEquals(0, context.builds);
        }
        Assertions.assertEquals(0, context.delta.closes);
    }

    @Test
    void refreshAndCloseReachTheDeltaSiblingAndPreventRecreation() throws Exception {
        DeltaContext context = new DeltaContext();
        HiveConnector gateway = gateway(context);
        gateway.getOrCreateDeltaSibling();
        gateway.invalidateTable("db", "t");
        gateway.invalidateDb("db");
        gateway.invalidateAll();
        Assertions.assertEquals(List.of("db.t", "db", "all"), context.delta.invalidated);
        gateway.close();
        gateway.close();
        Assertions.assertEquals(1, context.delta.closes);
        Assertions.assertThrows(DorisConnectorException.class, gateway::getOrCreateDeltaSibling);
        Assertions.assertEquals(1, context.builds);
    }

    @Test
    void unavailablePluginFailsClearlyAndCanRecover() throws Exception {
        DeltaContext context = new DeltaContext();
        context.available = false;
        try (HiveConnector gateway = gateway(context)) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    gateway::getOrCreateDeltaSibling);
            Assertions.assertTrue(error.getMessage().contains("deltalake connector plugin is not available"));
            Assertions.assertTrue(error.getMessage().contains("test_catalog"));
            context.available = true;
            Assertions.assertSame(context.delta, gateway.getOrCreateDeltaSibling());
            Assertions.assertEquals(2, context.builds);
        }
    }

    private static HiveConnector gateway(DeltaContext context) {
        return new HiveConnector(Map.of("hive.metastore.uris", "thrift://host:9083"), context);
    }

    private static HmsTableInfo table(Map<String, String> parameters) {
        return HmsTableInfo.builder().dbName("db").tableName("t").tableType("EXTERNAL_TABLE")
                .inputFormat(PARQUET).parameters(parameters).build();
    }

    private static HmsClient hms(HmsTableInfo table) {
        return (HmsClient) Proxy.newProxyInstance(HmsClient.class.getClassLoader(), new Class<?>[] {HmsClient.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "tableExists":
                            return true;
                        case "getTable":
                            return table;
                        default:
                            throw new AssertionError("Unexpected HMS call: " + method.getName());
                    }
                });
    }

    private static final class DeltaContext extends FakeConnectorContext {
        private final DeltaSibling delta = new DeltaSibling();
        private boolean available = true;
        private int builds;

        @Override
        public Connector createSiblingConnector(String type, Map<String, String> properties) {
            Assertions.assertEquals("deltalake", type, "unrelated sibling must not be created");
            Assertions.assertEquals("hms", properties.get("deltalake.catalog.type"));
            builds++;
            return available ? delta : null;
        }
    }

    private static final class DeltaSibling implements Connector {
        private final ConnectorTableHandle handle = new ConnectorTableHandle() {};
        private final ConnectorTableSchema schema = new ConnectorTableSchema("t", List.of(), "deltalake", Map.of());
        private final List<String> invalidated = new ArrayList<>();
        private int metadataCalls;
        private int closes;
        private final ConnectorScanPlanProvider scans = new ConnectorScanPlanProvider() {
            @Override
            public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
                return List.of();
            }
        };

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            metadataCalls++;
            return new ConnectorMetadata() {
                @Override
                public Optional<ConnectorTableHandle> getTableHandle(ConnectorSession session, String db, String table) {
                    return Optional.of(handle);
                }

                @Override
                public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table) {
                    Assertions.assertSame(handle, table);
                    return schema;
                }
            };
        }

        @Override
        public boolean ownsHandle(ConnectorTableHandle table) {
            return table == handle;
        }

        @Override
        public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle table) {
            Assertions.assertSame(handle, table);
            return scans;
        }

        @Override
        public void invalidateTable(String db, String table) {
            invalidated.add(db + "." + table);
        }

        @Override
        public void invalidateDb(String db) {
            invalidated.add(db);
        }

        @Override
        public void invalidateAll() {
            invalidated.add("all");
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
