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
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.thrift.TMongoScanDesc;
import org.apache.doris.thrift.TMongoSplit;

import org.bson.Document;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

final class MongoScanPlanProvider implements ConnectorScanPlanProvider {
    private final MongoCatalogProperties properties;

    private final Supplier<MongoCatalogClient> client;
    private final MongoRangePlanner rangePlanner;

    MongoScanPlanProvider(MongoCatalogProperties properties, Supplier<MongoCatalogClient> client) {
        this.properties = properties;
        this.client = client;
        this.rangePlanner = new MongoRangePlanner(client);
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        MongoRangePlanner.checkCancelled(session, request.getScanContext());
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
        // Stable connection/selection caps identify the shared pool; per-client socket timeouts
        // and local scan deadlines are separate. poolMaxClients bounds compatible BE-local leases.
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
        long descriptorSize = descriptorBytes(session, handle, d);
        MongoSplitPlan plan = rangePlanner.plan(session, handle, snapshot, d.getReadPreference(),
                request.getScanContext(), descriptorSize);
        // Retain the shared descriptor once; streaming creates owned RPC values under engine backpressure.
        // A wide schema must not be copied 256 times merely to keep a frozen boundary table in FE.
        return new AbstractList<>() {
            @Override
            public ConnectorScanRange get(int index) {
                java.util.Objects.checkIndex(index, plan.size());
                MongoRangePlanner.checkCancelled(session, request.getScanContext());
                TMongoScanDesc range = d.deepCopy();
                TMongoSplit split = new TMongoSplit();
                split.setStrategy(plan.family().isEmpty() ? "single" : "range");
                split.setField(plan.field());
                split.setTypeFamily(plan.family());
                split.setIndex(index);
                split.setRequested(plan.requested());
                split.setAdmitted(plan.admitted());
                split.setActual(plan.size());
                split.setReason(plan.reason());
                if (!plan.family().isEmpty()) {
                    range.setVersion(2);
                    split.setFilterBson(plan.partitionBytes(index));
                }
                range.setSplit(split);
                if (snapshot != null) {
                    range.setTimeoutMs(MongoSnapshot.remainingMillis(session));
                }
                if (plan.descriptorBudget() > 0 && descriptorSize > plan.descriptorBudget()) {
                    try {
                        // EXPLAIN may freeze a plan before a later caller supplies a larger candidate
                        // or projection. Never let that RPC silently exceed the frozen reservation.
                        int actual = new org.apache.thrift.TSerializer(
                                new org.apache.thrift.protocol.TBinaryProtocol.Factory()).serialize(range).length;
                        if (actual > plan.descriptorBudget()) {
                            throw new DorisConnectorException("MongoDB descriptor grew after split plan freeze");
                        }
                    } catch (org.apache.thrift.TException failure) {
                        throw new DorisConnectorException("Cannot size MongoDB split descriptor");
                    }
                }
                return new MongoScanRange(range);
            }

            @Override
            public int size() {
                return plan.size();
            }
        };
    }

    @Override
    public long streamingSplitEstimate(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter, boolean countPushdown) {
        return "single".equals(session.getSessionProperties().getOrDefault("mongodb_split_strategy", "single"))
                ? -1 : Math.min(8, MongoRangePlanner.splitLimit(session));
    }

    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, ConnectorScanRequest request) {
        return new ConnectorSplitSource() {
            private Iterator<ConnectorScanRange> ranges;
            private boolean closed;

            @Override
            public boolean hasNext() {
                if (closed) {
                    return false;
                }
                MongoRangePlanner.checkCancelled(session, request.getScanContext());
                if (ranges == null) {
                    // All probes, admission and boundary validation finish before the first publication.
                    ranges = planScan(session, request).iterator();
                }
                return ranges.hasNext();
            }

            @Override
            public ConnectorScanRange next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return ranges.next();
            }

            @Override
            public void close() {
                closed = true;
                ranges = Collections.emptyIterator();
            }
        };
    }

    private long descriptorBytes(ConnectorSession session, MongoTableHandle table,
            TMongoScanDesc descriptor) {
        if ("single".equals(session.getSessionProperties().getOrDefault("mongodb_split_strategy", "single"))) {
            return 0;
        }
        // A count-only cap is insufficient: 100 copies of a large schema/credential/filter can exceed
        // a split RPC's body limit. Include UTF-8 payloads plus room for Thrift fields and range BSON.
        long bytes = 8192L + (descriptor.isSetFilterBson() ? descriptor.getFilterBson().length : 0)
                + (descriptor.isSetProjectionBson() ? descriptor.getProjectionBson().length : 0);
        if (descriptor.isSetSnapshot() && descriptor.getSnapshot().isSetTimeseriesOptionsJson()) {
            // A legal timeField can exceed the fixed header allowance. Every auto fallback
            // repeats these options, so omit none of their UTF-8 bytes from source admission.
            bytes += descriptor.getSnapshot().getTimeseriesOptionsJson()
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
        for (String value : List.of(properties.getUri(), properties.getUsername(), properties.getPassword(),
                properties.getAuthDatabase(), table.getDatabase(), table.getName(), table.getUuid(),
                table.getSchema().getVersion(), table.getSchema().toBindingsJson())) {
            bytes += value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
        return bytes;
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
        var pushed = predicate(session, table, filter);
        var pruned = projection(session, table, columns);
        result.put("read_concern", concern);
        result.put("object_kind", table.getObjectKind());
        result.put("schema_source", table.getSchema().getSource());
        result.put("schema_version", table.getSchema().getVersion());
        result.put("predicate_pushdown", pushed.kind().name());
        result.put("projection_pushdown", pruned.isEmpty() ? "false" : "true");
        // initSchemaParams reads properties BEFORE WHERE/projection are attached. Source probes
        // and byte admission here would reject a normal large predicate and ignore KILL QUERY.
        // Resolve diagnostics lazily from the final cancellable plan, without doing any source I/O.
        return new AbstractMap<>() {
            @Override
            public Set<Entry<String, String>> entrySet() {
                Map<String, String> current = new LinkedHashMap<>(result);
                var plan = rangePlanner.planned(session, table);
                if (plan.isEmpty() && "single".equals(policies.getOrDefault("mongodb_split_strategy", "single"))) {
                    plan = Optional.of(MongoSplitPlan.single(1, 1, "single_strategy"));
                }
                if (plan.isPresent()) {
                    var frozen = plan.get();
                    current.put("split_strategy", frozen.family().isEmpty() ? "single" : "range");
                    current.put("split_field", frozen.field());
                    current.put("split_type", frozen.family());
                    current.put("splits_requested", Integer.toString(frozen.requested()));
                    current.put("splits_admitted", Integer.toString(frozen.admitted()));
                    current.put("splits_actual", Integer.toString(frozen.size()));
                    current.put("split_reason", frozen.reason());
                } else {
                    current.put("split_strategy", policies.getOrDefault("mongodb_split_strategy", "single"));
                    current.put("split_field", policies.getOrDefault("mongodb_split_field", ""));
                    current.put("split_type", "");
                    current.put("splits_requested", "0");
                    current.put("splits_admitted", "0");
                    current.put("splits_actual", "0");
                    current.put("split_reason", "planning_pending");
                }
                current.put("snapshot", MongoSnapshot.pinnedTimestamp(session));
                return Collections.unmodifiableMap(current).entrySet();
            }
        };
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix).append("MongoDB object: ").append(nodeProperties.get("object_kind"))
                .append("; schema: ").append(nodeProperties.get("schema_source"))
                .append("; version: ").append(nodeProperties.get("schema_version")).append("\n");
        output.append(prefix).append("MongoDB read concern: ").append(nodeProperties.get("read_concern"))
                .append("; snapshot: ").append(nodeProperties.get("snapshot")).append("\n");
        output.append(prefix).append("MongoDB splits: ").append(nodeProperties.get("split_strategy"))
                .append("; field: ").append(nodeProperties.get("split_field"))
                .append("; type: ").append(nodeProperties.get("split_type"))
                .append("; requested/admitted/actual: ").append(nodeProperties.get("splits_requested"))
                .append("/").append(nodeProperties.get("splits_admitted"))
                .append("/").append(nodeProperties.get("splits_actual"))
                .append("; reason: ").append(nodeProperties.get("split_reason")).append("\n");
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
