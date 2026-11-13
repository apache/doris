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
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TMongoScanDesc;

import java.util.List;
import java.util.Map;
import java.util.Optional;

final class MongoScanPlanProvider implements ConnectorScanPlanProvider {
    private final MongoCatalogProperties properties;

    MongoScanPlanProvider(MongoCatalogProperties properties) {
        this.properties = properties;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        MongoTableHandle handle = (MongoTableHandle) request.getTableHandle();
        for (ConnectorColumnHandle column : request.getColumns()) {
            if (!(column instanceof MongoColumnHandle)
                    || !handle.getSchema().getVersion().equals(((MongoColumnHandle) column).getSchemaVersion())) {
                throw new IllegalArgumentException("MongoDB column schema changed; refresh metadata and retry");
            }
        }
        TMongoScanDesc d = new TMongoScanDesc();
        d.setVersion(1);
        d.setCatalogId(session.getCatalogId());
        d.setConfigurationVersion(properties.configurationVersion());
        d.setDatabase(handle.getDatabase());
        d.setCollection(handle.getName());
        d.setObjectKind(handle.getObjectKind());
        d.setCollectionUuid(handle.getUuid());
        d.setSchemaVersion(handle.getSchema().getVersion());
        d.setBindingsJson(handle.getSchema().toBindingsJson());
        d.setUri(properties.getUri());
        d.setUsername(properties.getUsername());
        d.setPassword(properties.getPassword());
        d.setAuthDatabase(properties.getAuthDatabase());
        d.setReadPreference(properties.getReadPreference());
        d.setReadConcern(properties.getReadConcern());
        d.setTypeMismatchAsNull("null".equals(properties.getTypeMismatch()));
        Map<String, String> policies = session.getSessionProperties();
        d.setBatchSize(bounded(policies, "batch_size", 1024, 1, 65536));
        d.setTimeoutMs(1000L * bounded(policies, "query_timeout", 300, 1, Integer.MAX_VALUE / 1000));
        d.setConnectTimeoutMs(properties.getConnectTimeoutMs());
        d.setServerSelectionTimeoutMs(properties.getServerSelectionTimeoutMs());
        d.setSocketTimeoutMs(properties.getSocketTimeoutMs());
        d.setPoolMaxClients(properties.getPoolMaxClients());
        // One cursor owns the entire logical namespace. Filters and LIMIT remain Doris residuals.
        return List.of(new MongoScanRange(d));
    }

    private static int bounded(Map<String, String> properties, String name, int fallback, int min, int max) {
        try {
            int value = Integer.parseInt(properties.getOrDefault(name, Integer.toString(fallback)));
            if (value >= min && value <= max) {
                return value;
            }
        } catch (NumberFormatException ignored) {
            // Reject malformed query policy rather than silently changing execution limits.
        }
        throw new IllegalArgumentException("Invalid MongoDB query policy: " + name);
    }

    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
        MongoTableHandle table = (MongoTableHandle) handle;
        return Map.of("object_kind", table.getObjectKind(), "schema_source", table.getSchema().getSource(),
                "schema_version", table.getSchema().getVersion());
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix).append("MongoDB object: ").append(nodeProperties.get("object_kind"))
                .append("; schema: ").append(nodeProperties.get("schema_source"))
                .append("; version: ").append(nodeProperties.get("schema_version")).append("\n");
    }

}
