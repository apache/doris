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

import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

final class MongoScanPlanProvider implements ConnectorScanPlanProvider {
    private final MongoCatalogProperties properties;

    private final Supplier<MongoCatalogClient> client;

    MongoScanPlanProvider(MongoCatalogProperties properties, Supplier<MongoCatalogClient> client) {
        this.properties = properties;
        this.client = client;
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
        // FE-to-BE value contract; no driver object, cursor or active session crosses this boundary.
        TMongoScanDesc d = new TMongoScanDesc();
        // Layout version and pool isolation: configurationVersion fingerprints connection/auth settings.
        d.setVersion(1);
        d.setCatalogId(session.getCatalogId());
        d.setConfigurationVersion(properties.configurationVersion());
        // Preserve the public source namespace. objectKind selects view/time-series restrictions;
        // collectionUuid detects namespace replacement independently of the decoding schema version.
        d.setDatabase(handle.getDatabase());
        d.setCollection(handle.getName());
        d.setObjectKind(handle.getObjectKind());
        d.setCollectionUuid(handle.getUuid());
        // These owned values freeze exact field paths, accepted BSON tags and target column bindings.
        d.setSchemaVersion(handle.getSchema().getVersion());
        d.setBindingsJson(handle.getSchema().toBindingsJson());
        // Secrets travel only in the typed descriptor. authDatabase authenticates the separate user
        // and can differ from the data database; URI credentials are used when no separate user is set.
        d.setUri(properties.getUri());
        d.setUsername(properties.getUsername());
        d.setPassword(properties.getPassword());
        d.setAuthDatabase(properties.getAuthDatabase());
        // Conversion mismatch policy only; it never suppresses source, cancellation or allocation errors.
        d.setTypeMismatchAsNull("null".equals(properties.getTypeMismatch()));
        Map<String, String> policies = session.getSessionProperties();
        // Resolve inherit before RPC: readPreference chooses nodes, readConcern controls visibility.
        // Pin effective policies in the range; never mutate the catalog or its shared metadata client.
        d.setReadPreference(properties.resolveReadPreference(
                policies.getOrDefault("mongodb_read_preference", "inherit")));
        d.setReadConcern(properties.resolveReadConcern(policies.getOrDefault("mongodb_read_concern", "inherit")));
        // Source batch size is a document count. timeoutMs is a scan budget in milliseconds;
        // snapshot selection below subtracts elapsed FE time before the range is published.
        d.setBatchSize(bounded(policies, "batch_size", 1024, 1, 65536));
        d.setTimeoutMs(1000L * bounded(policies, "query_timeout", 300, 1, Integer.MAX_VALUE / 1000));
        var snapshot = MongoSnapshot.prepare(session, properties, handle, client,
                d.getReadConcern(), d.getReadPreference(), d.getTimeoutMs());
        if (snapshot != null) {
            // Each range owns a copy of the statement's T/source pins; BE opens an independent session.
            d.setSnapshot(snapshot);
            d.setTimeoutMs(MongoSnapshot.remainingMillis(session));
        }
        // Distinct limits for connection setup, node selection and socket I/O. BE caps these by
        // the scan budget; poolMaxClients limits leased clients per compatible BE-local pool.
        d.setConnectTimeoutMs(properties.getConnectTimeoutMs());
        d.setServerSelectionTimeoutMs(properties.getServerSelectionTimeoutMs());
        d.setSocketTimeoutMs(properties.getSocketTimeoutMs());
        d.setPoolMaxClients(properties.getPoolMaxClients());
        MongoPredicateTranslator.Result predicate = predicate(session, handle, request.getFilter());
        if (predicate.kind() != MongoPredicateTranslator.Kind.UNSUPPORTED) {
            // Binary BSON preserves Int64/Decimal128/Date tags. This candidate never replaces the residual.
            d.setFilterBson(encode(predicate.filter()));
        }
        Document projection = projection(session, handle, request.getColumns());
        if (!projection.isEmpty()) {
            // Include complete source roots so pruning cannot turn malformed nested paths into missing values.
            d.setProjectionBson(encode(projection));
        }
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
        Map<String, String> result = new LinkedHashMap<>();
        var policies = session.getSessionProperties();
        String concern = properties.resolveReadConcern(policies.getOrDefault("mongodb_read_concern", "inherit"));
        String preference = properties.resolveReadPreference(
                policies.getOrDefault("mongodb_read_preference", "inherit"));
        var snapshot = MongoSnapshot.prepare(session, properties, table, client, concern, preference,
                1000L * bounded(policies, "query_timeout", 300, 1, Integer.MAX_VALUE / 1000));
        result.put("read_concern", concern);
        result.put("snapshot", snapshot == null ? "none" : snapshot.getTimestampSeconds()
                + ":" + snapshot.getTimestampIncrement());
        result.put("object_kind", table.getObjectKind());
        result.put("schema_source", table.getSchema().getSource());
        result.put("schema_version", table.getSchema().getVersion());
        result.put("predicate_pushdown", predicate(session, table, filter).kind().name());
        result.put("projection_pushdown", projection(session, table, columns).isEmpty() ? "false" : "true");
        return result;
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix).append("MongoDB object: ").append(nodeProperties.get("object_kind"))
                .append("; schema: ").append(nodeProperties.get("schema_source"))
                .append("; version: ").append(nodeProperties.get("schema_version")).append("\n");
        output.append(prefix).append("MongoDB read concern: ").append(nodeProperties.get("read_concern"))
                .append("; snapshot: ").append(nodeProperties.get("snapshot")).append("\n");
        output.append(prefix).append("MongoDB predicate: ").append(nodeProperties.get("predicate_pushdown"))
                .append("; residual: retained; projection: ").append(nodeProperties.get("projection_pushdown"))
                .append("\n");
    }

    private MongoPredicateTranslator.Result predicate(ConnectorSession session, MongoTableHandle table,
            Optional<ConnectorExpression> filter) {
        // Even a predicate on a valid field can hide a strict decode failure in another projected field.
        // A validator does not validate historical rows, so strict scans and views keep every filter local.
        if (!"null".equals(properties.getTypeMismatch()) || "view".equals(table.getObjectKind())
                || Boolean.parseBoolean(session.getSessionProperties().getOrDefault("enable_strict_cast", "false"))
                || !enabled(session, "enable_mongodb_predicate_pushdown") || filter.isEmpty()) {
            return MongoPredicateTranslator.Result.unsupported();
        }
        var result = new MongoPredicateTranslator(table.getSchema(), enabled(session, "enable_mongodb_like_pushdown"))
                .translate(filter.get());
        return encode(result.filter()).length <= 1024 * 1024
                ? result : MongoPredicateTranslator.Result.unsupported();
    }

    private static Document projection(ConnectorSession session, MongoTableHandle table,
            List<ConnectorColumnHandle> columns) {
        if (!enabled(session, "enable_mongodb_projection_pushdown")) {
            return new Document();
        }
        Document result = new Document();
        for (ConnectorColumnHandle column : columns) {
            Document binding = Document.parse(((MongoColumnHandle) column).getBindingJson());
            List<String> path = binding.getList("path", String.class);
            if (!"".equals(binding.getString("special")) || path.isEmpty()
                    || !MongoPredicateTranslator.safePath(path.get(0))) {
                return new Document();
            }
            // Project the complete root: {a:1} at [a,b] must remain a path error, not become missing.
            result.put(path.get(0), 1);
        }
        // _id alone is an inclusion projection for COUNT(*) and preserves one result per document.
        result.put("_id", result.isEmpty() || result.containsKey("_id") ? 1 : 0);
        return result;
    }

    private static boolean enabled(ConnectorSession session, String name) {
        return Boolean.parseBoolean(session.getSessionProperties().getOrDefault(name, "true"));
    }

    private static byte[] encode(Document document) {
        org.bson.io.BasicOutputBuffer buffer = new org.bson.io.BasicOutputBuffer();
        new org.bson.codecs.DocumentCodec().encode(new org.bson.BsonBinaryWriter(buffer), document,
                org.bson.codecs.EncoderContext.builder().build());
        return buffer.toByteArray();
    }

}
