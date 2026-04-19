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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.NamedColumnHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ES scan plan provider — generates shard-level scan ranges and node-level properties.
 *
 * <p>This replaces the scan planning logic from fe-core's {@code EsScanNode}.
 * It fetches shard routing from the ES cluster and builds query DSL from
 * the connector expression filter.</p>
 *
 * <p>The provider produces:</p>
 * <ul>
 *   <li><b>Per-range:</b> one {@link EsScanRange} per shard with host routing</li>
 *   <li><b>Per-node properties:</b> query DSL, auth info, doc_values_mode,
 *       docvalue_context and fields_context (JSON-serialized)</li>
 * </ul>
 */
public class EsScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(EsScanPlanProvider.class);

    // Cache TTL: metadata is shared within a single query planning cycle
    // (planScan and getScanNodeProperties are called in rapid succession).
    // 10 seconds avoids redundant REST calls without serving stale data
    // across different queries.
    static final long METADATA_CACHE_TTL_MS = 10_000;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static final String PROP_QUERY_DSL = "query_dsl";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String PROP_DOC_VALUES_MODE = "doc_values_mode";
    public static final String PROP_NOT_PUSHED_INDICES = "_not_pushed_conjunct_indices";

    public static final String PROP_DOCVALUE_CONTEXT_JSON = "docvalue_context_json";
    public static final String PROP_FIELDS_CONTEXT_JSON = "fields_context_json";

    private final EsConnectorRestClient restClient;
    private final Map<String, String> properties;
    private final ConcurrentHashMap<String, CachedMetadata> metadataCache = new ConcurrentHashMap<>();

    /** Timestamped wrapper for cached metadata state. */
    private static final class CachedMetadata {
        final EsMetadataState state;
        final long timestampMs;

        CachedMetadata(EsMetadataState state) {
            this.state = state;
            this.timestampMs = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestampMs > METADATA_CACHE_TTL_MS;
        }
    }

    public EsScanPlanProvider(EsConnectorRestClient restClient,
            Map<String, String> properties) {
        this.restClient = restClient;
        this.properties = properties;
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.ES_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        EsTableHandle esHandle = (EsTableHandle) handle;
        String indexName = esHandle.getIndexName();

        EsMetadataState state = fetchMetadataState(esHandle, columns);
        EsShardPartitions shardPartitions = state.getShardPartitions();
        if (shardPartitions == null) {
            LOG.warn("No shard partitions found for index {}", indexName);
            return Collections.emptyList();
        }

        String mappingType = properties.getOrDefault(
                EsConnectorProperties.MAPPING_TYPE, null);

        boolean enableParallelScroll = Boolean.parseBoolean(
                session.getSessionProperties()
                        .getOrDefault("enable_es_parallel_scroll", "true"));

        List<ConnectorScanRange> ranges = new ArrayList<>();
        Map<Integer, List<EsShardRouting>> routingsMap = shardPartitions.getShardRoutings();

        if (!enableParallelScroll) {
            // Single query mode: one scan range for the whole index
            List<String> hosts = collectAllHosts(routingsMap);
            ranges.add(new EsScanRange(
                    shardPartitions.getIndexName(), mappingType, -1, hosts));
        } else {
            // Parallel scroll mode: one scan range per shard
            for (Map.Entry<Integer, List<EsShardRouting>> entry : routingsMap.entrySet()) {
                List<EsShardRouting> shardRouting = entry.getValue();
                if (shardRouting.isEmpty()) {
                    continue;
                }
                List<String> hosts = new ArrayList<>();
                for (EsShardRouting routing : shardRouting) {
                    hosts.add(routing.getHttpHost() + ":" + routing.getHttpPort());
                }
                ranges.add(new EsScanRange(
                        shardRouting.get(0).getIndexName(),
                        mappingType,
                        shardRouting.get(0).getShardId(),
                        hosts));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("ES scan plan for index {}: {} ranges", indexName, ranges.size());
        }
        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        EsTableHandle esHandle = (EsTableHandle) handle;
        EsMetadataState state = fetchMetadataState(esHandle, columns);

        Map<String, String> nodeProps = new HashMap<>();

        // Table/index metadata for EXPLAIN
        nodeProps.put("_table_name", esHandle.getIndexName());
        nodeProps.put("_es_index", esHandle.getIndexName());

        // Auth properties
        String user = properties.getOrDefault(EsConnectorProperties.USER, null);
        if (user != null && !user.isEmpty()) {
            nodeProps.put(PROP_USER, user);
        }
        String password = properties.getOrDefault(EsConnectorProperties.PASSWORD, null);
        if (password != null && !password.isEmpty()) {
            nodeProps.put(PROP_PASSWORD, password);
        }
        nodeProps.put(PROP_HTTP_SSL_ENABLED, properties.getOrDefault(
                EsConnectorProperties.HTTP_SSL_ENABLED,
                EsConnectorProperties.HTTP_SSL_ENABLED_DEFAULT));

        // Query DSL (with not-pushed conjunct tracking)
        EsQueryDslResult dslResult = buildQueryDsl(filter, state);
        nodeProps.put(PROP_QUERY_DSL, dslResult.getQueryDsl());

        // Serialize not-pushed conjunct indices so the scan node can prune pushed conjuncts
        if (!dslResult.getNotPushedIndices().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dslResult.getNotPushedIndices().size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(dslResult.getNotPushedIndices().get(i));
            }
            nodeProps.put(PROP_NOT_PUSHED_INDICES, sb.toString());
        }

        // Doc values mode — two-gate check matching old EsScanNode.useDocValueScan():
        // Gate 1: selected field count must not exceed maxDocValueFields
        // Gate 2: every selected field must exist in the docValueFieldsContext map
        nodeProps.put(PROP_DOC_VALUES_MODE,
                String.valueOf(useDocValueScan(columns, state)));

        // Serialize docvalue_context and fields_context as JSON into flat properties
        // so we don't need the ES-specific getScanNodeMapProperties() on the generic SPI.
        serializeFieldContexts(state, nodeProps);

        return nodeProps;
    }

    private void serializeFieldContexts(EsMetadataState state, Map<String, String> nodeProps) {
        if (state.getFieldContext() == null) {
            return;
        }
        boolean enableDocValueScan = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.DOC_VALUE_SCAN,
                EsConnectorProperties.DOC_VALUE_SCAN_DEFAULT));
        boolean enableKeywordSniff = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.KEYWORD_SNIFF,
                EsConnectorProperties.KEYWORD_SNIFF_DEFAULT));
        try {
            if (enableDocValueScan) {
                Map<String, String> docCtx = state.getFieldContext().getDocValueFieldsContext();
                if (docCtx != null && !docCtx.isEmpty()) {
                    nodeProps.put(PROP_DOCVALUE_CONTEXT_JSON,
                            JSON_MAPPER.writeValueAsString(docCtx));
                }
            }
            if (enableKeywordSniff) {
                Map<String, String> fieldsCtx = state.getFieldContext().getFetchFieldsContext();
                if (fieldsCtx != null && !fieldsCtx.isEmpty()) {
                    nodeProps.put(PROP_FIELDS_CONTEXT_JSON,
                            JSON_MAPPER.writeValueAsString(fieldsCtx));
                }
            }
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to serialize ES field contexts to JSON", e);
        }
    }

    private EsQueryDslResult buildQueryDsl(Optional<ConnectorExpression> filter,
            EsMetadataState state) {
        if (!filter.isPresent()) {
            return new EsQueryDslResult("{\"match_all\":{}}", Collections.emptyList());
        }

        Map<String, String> fieldsContext = Collections.emptyMap();
        List<String> needCompatDateFields = Collections.emptyList();
        Map<String, String> column2typeMap = Collections.emptyMap();

        if (state.getFieldContext() != null) {
            boolean enableKeywordSniff = Boolean.parseBoolean(properties.getOrDefault(
                    EsConnectorProperties.KEYWORD_SNIFF,
                    EsConnectorProperties.KEYWORD_SNIFF_DEFAULT));
            if (enableKeywordSniff) {
                fieldsContext = state.getFieldContext().getFetchFieldsContext();
            }
            needCompatDateFields = state.getFieldContext().getNeedCompatDateFields();
            column2typeMap = state.getFieldContext().getColumn2typeMap();
        }

        boolean likePushDown = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.LIKE_PUSH_DOWN,
                EsConnectorProperties.LIKE_PUSH_DOWN_DEFAULT));

        return EsQueryDslBuilder.buildQueryDslWithResult(
                filter.get(), fieldsContext, column2typeMap,
                likePushDown, needCompatDateFields);
    }

    private EsMetadataState fetchMetadataState(EsTableHandle handle,
            List<ConnectorColumnHandle> columns) {
        String indexName = handle.getIndexName();
        CachedMetadata cached = metadataCache.get(indexName);
        if (cached != null && !cached.isExpired()) {
            return cached.state;
        }

        List<String> columnNames = new ArrayList<>();
        // Column names not strictly needed for scan planning but useful for field context
        String mappingType = properties.getOrDefault(
                EsConnectorProperties.MAPPING_TYPE, null);
        boolean nodesDiscovery = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.NODES_DISCOVERY,
                EsConnectorProperties.NODES_DISCOVERY_DEFAULT));
        String hostsStr = properties.getOrDefault(EsConnectorProperties.HOSTS, "");
        String[] seeds = hostsStr.split(",");

        EsMetadataState state = new EsMetadataState(
                indexName, mappingType, columnNames, nodesDiscovery, seeds);
        EsMetadataFetcher fetcher = new EsMetadataFetcher(restClient, state);
        EsMetadataState result = fetcher.fetch();
        metadataCache.put(indexName, new CachedMetadata(result));
        return result;
    }

    /** Visible for testing: returns the number of cached metadata entries. */
    int metadataCacheSize() {
        return metadataCache.size();
    }

    /** Visible for testing: clears the metadata cache. */
    void clearMetadataCache() {
        metadataCache.clear();
    }

    /**
     * Determine whether doc_value scan should be used for this query.
     *
     * <p>Matches the two-gate logic from old {@code EsScanNode.useDocValueScan()}:
     * <ul>
     *   <li>Gate 0: catalog-level enable_docvalue_scan must be true</li>
     *   <li>Gate 1: selected field count must not exceed max_docvalue_fields</li>
     *   <li>Gate 2: every selected field must exist in the docvalue context map</li>
     * </ul>
     *
     * @return 1 if doc_value scan should be used, 0 otherwise
     */
    private int useDocValueScan(List<ConnectorColumnHandle> columns,
            EsMetadataState state) {
        boolean enableDocValueScan = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.DOC_VALUE_SCAN,
                EsConnectorProperties.DOC_VALUE_SCAN_DEFAULT));
        if (!enableDocValueScan || state.getFieldContext() == null) {
            return 0;
        }

        // Extract selected field names from column handles
        List<String> selectedFields = new ArrayList<>();
        for (ConnectorColumnHandle col : columns) {
            if (col instanceof NamedColumnHandle) {
                selectedFields.add(((NamedColumnHandle) col).getName());
            }
        }

        // Gate 1: field count limit
        int maxDocValueFields = Integer.parseInt(properties.getOrDefault(
                EsConnectorProperties.MAX_DOCVALUE_FIELDS,
                String.valueOf(EsConnectorProperties.MAX_DOCVALUE_FIELDS_DEFAULT)));
        if (selectedFields.size() > maxDocValueFields) {
            return 0;
        }

        // Gate 2: every selected field must have a docvalue mapping
        Set<String> docValueFields = state.getFieldContext().getDocValueFieldsContext().keySet();
        for (String field : selectedFields) {
            if (!docValueFields.contains(field)) {
                return 0;
            }
        }
        return 1;
    }

    private List<String> collectAllHosts(
            Map<Integer, List<EsShardRouting>> routingsMap) {
        List<String> hosts = new ArrayList<>();
        for (List<EsShardRouting> routings : routingsMap.values()) {
            for (EsShardRouting routing : routings) {
                String addr = routing.getHttpHost() + ":" + routing.getHttpPort();
                if (!hosts.contains(addr)) {
                    hosts.add(addr);
                }
            }
        }
        return hosts;
    }
}
