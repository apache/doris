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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static final String PROP_QUERY_DSL = "query_dsl";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String PROP_DOC_VALUES_MODE = "doc_values_mode";
    public static final String PROP_ES_INDEX = "_es_index";

    public static final String PROP_DOCVALUE_CONTEXT_JSON = "docvalue_context_json";
    public static final String PROP_FIELDS_CONTEXT_JSON = "fields_context_json";

    /**
     * BE contract: ES reads this out of {@code es_properties} and stops the search after that many hits
     * instead of scrolling the whole result ({@code ESScanReader::KEY_TERMINATE_AFTER}). The literal is the
     * wire value and must not change.
     */
    private static final String PROP_LIMIT = "limit";

    /**
     * Connector-private: how many rows BE reads per batch, taken from the session while the properties are
     * built (populateScanLevelParams gets no session) and read back there. Namespaced so it can never collide
     * with an engine-read key.
     */
    private static final String PROP_BATCH_SIZE = "es.batch_size";

    /** Session variable carrying BE's per-batch row count; the engine exports every visible variable. */
    private static final String SESSION_BATCH_SIZE = "batch_size";

    private final EsConnectorRestClient restClient;
    private final Map<String, String> properties;

    // ES-F1 per-scan hoist. planScan and buildScanNodeProperties of one scan node run on the same
    // per-scan-node provider instance on the synchronous FE planning thread, and each used to fetch
    // the full metadata state (mapping + shard routing + node topology) independently. Memoizing the
    // last resolved state lets the second call reuse the first. Guarded on (index, columns) so a
    // provider reused for a different request refetches; shard routing stays per-scan (fresh) because
    // the provider is discarded at scan end. Plain field: safe ONLY because ES never enters batch mode
    // (no off-thread scan pool) -- make it volatile if this provider ever declares batch scan.
    private EsMetadataState memoizedState;

    public EsScanPlanProvider(EsConnectorRestClient restClient,
            Map<String, String> properties) {
        this.restClient = restClient;
        this.properties = properties;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        List<ConnectorColumnHandle> columns = request.getColumns();
        EsTableHandle esHandle = (EsTableHandle) request.getTableHandle();
        String indexName = esHandle.getIndexName();

        EsMetadataState state = fetchMetadataState(session, esHandle, columns);
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
                    hosts.add(EsHostAddress.formatHostPort(routing.getHttpHost(), routing.getHttpPort()));
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
    public ScanNodePropertiesResult getScanNodePropertiesResult(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return buildScanNodeProperties(session, handle, columns, filter);
    }

    private ScanNodePropertiesResult buildScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        EsTableHandle esHandle = (EsTableHandle) handle;
        EsMetadataState state = fetchMetadataState(session, esHandle, columns);

        Map<String, String> nodeProps = new HashMap<>();

        // File format type for PluginDrivenScanNode.getFileFormatType()
        nodeProps.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "es_http");

        // Carry BE's per-batch row count forward: populateScanLevelParams decides there whether the pushed
        // limit is small enough to ask ES to stop early, and it receives no session.
        String batchSize = session.getSessionProperties().get(SESSION_BATCH_SIZE);
        if (batchSize != null) {
            nodeProps.put(PROP_BATCH_SIZE, batchSize);
        }

        // Table/index metadata for EXPLAIN
        nodeProps.put("_table_name", esHandle.getIndexName());
        nodeProps.put(PROP_ES_INDEX, esHandle.getIndexName());

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

        // Doc values mode — two-gate check matching old EsScanNode.useDocValueScan():
        // Gate 1: selected field count must not exceed maxDocValueFields
        // Gate 2: every selected field must exist in the docValueFieldsContext map
        nodeProps.put(PROP_DOC_VALUES_MODE,
                String.valueOf(useDocValueScan(columns, state)));

        // Serialize docvalue_context and fields_context as JSON into flat properties
        // so we don't need the ES-specific getScanNodeMapProperties() on the generic SPI.
        serializeFieldContexts(state, nodeProps);

        // Build not-pushed conjunct indices set for structured reporting
        Set<Integer> notPushedSet = new HashSet<>(dslResult.getNotPushedIndices());

        return ScanNodePropertiesResult.withPushdownTracking(nodeProps, notPushedSet);
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

    private EsMetadataState fetchMetadataState(ConnectorSession session, EsTableHandle handle,
            List<ConnectorColumnHandle> columns) {
        String indexName = handle.getIndexName();
        List<String> columnNames = new ArrayList<>();
        for (ConnectorColumnHandle col : columns) {
            if (col instanceof NamedColumnHandle) {
                columnNames.add(((NamedColumnHandle) col).getName());
            }
        }
        EsMetadataState cached = memoizedState;
        if (cached != null && cached.getSourceIndex().equals(indexName)
                && cached.getColumnNames().equals(columnNames)) {
            return cached;
        }

        String mappingType = properties.getOrDefault(
                EsConnectorProperties.MAPPING_TYPE, null);
        boolean nodesDiscovery = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.NODES_DISCOVERY,
                EsConnectorProperties.NODES_DISCOVERY_DEFAULT));
        String hostsStr = properties.getOrDefault(EsConnectorProperties.HOSTS, "");
        String[] seeds = hostsStr.split(",");

        EsMetadataState state = new EsMetadataState(
                indexName, mappingType, columnNames, nodesDiscovery, seeds);
        EsMetadataFetcher fetcher = new EsMetadataFetcher(restClient, state, session);
        state = fetcher.fetch();
        memoizedState = state;
        return state;
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

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
            Map<String, String> properties) {
        // Build es_properties map from scan node properties
        Map<String, String> esProperties = new HashMap<>();
        copyIfPresent(properties, PROP_QUERY_DSL, esProperties);
        copyIfPresent(properties, PROP_USER, esProperties);
        copyIfPresent(properties, PROP_PASSWORD, esProperties);
        copyIfPresent(properties, PROP_HTTP_SSL_ENABLED, esProperties);
        copyIfPresent(properties, PROP_DOC_VALUES_MODE, esProperties);
        // Ask ES to stop after N hits instead of scrolling everything. Only correct when the engine has NO
        // filtering left to do after the scan (otherwise rows ES returns could still be filtered out, and
        // stopping early would lose rows), and only worth it when the limit fits in one BE batch. The engine
        // supplies both facts; used to live in the generic scan node, which had to recognize this connector
        // by its format string to do it.
        long pushdownLimit = parseLongOrDefault(properties.get(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT), -1L);
        long batchSize = parseLongOrDefault(properties.get(PROP_BATCH_SIZE), -1L);
        if (pushdownLimit > 0 && batchSize > 0 && pushdownLimit <= batchSize
                && allConjunctsPushed(properties)) {
            esProperties.put(PROP_LIMIT, String.valueOf(pushdownLimit));
        }
        params.setEsProperties(esProperties);

        // Deserialize docvalue_context and fields_context from JSON
        String docvalueJson = properties.get(PROP_DOCVALUE_CONTEXT_JSON);
        if (docvalueJson != null && !docvalueJson.isEmpty()) {
            try {
                TypeReference<Map<String, String>> mapTypeRef =
                        new TypeReference<Map<String, String>>() {};
                Map<String, String> docCtx =
                        JSON_MAPPER.readValue(docvalueJson, mapTypeRef);
                params.setEsDocvalueContext(docCtx);
            } catch (Exception e) {
                LOG.warn("Failed to parse docvalue_context_json", e);
            }
        }

        String fieldsJson = properties.get(PROP_FIELDS_CONTEXT_JSON);
        if (fieldsJson != null && !fieldsJson.isEmpty()) {
            try {
                TypeReference<Map<String, String>> mapTypeRef =
                        new TypeReference<Map<String, String>>() {};
                Map<String, String> fieldsCtx =
                        JSON_MAPPER.readValue(fieldsJson, mapTypeRef);
                params.setEsFieldsContext(fieldsCtx);
            } catch (Exception e) {
                LOG.warn("Failed to parse fields_context_json", e);
            }
        }
    }

    private static void copyIfPresent(Map<String, String> src,
            String key, Map<String, String> dst) {
        String value = src.get(key);
        if (value != null) {
            dst.put(key, value);
        }
    }

    private static boolean allConjunctsPushed(Map<String, String> properties) {
        return "true".equals(properties.get(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED));
    }

    private static long parseLongOrDefault(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            Map<String, String> properties) {
        String indexName = properties.get(PROP_ES_INDEX);
        if (indexName != null) {
            output.append(prefix).append("ES index: ").append(indexName)
                    .append("\n");
        }
        String docvalueJson = properties.get(PROP_DOCVALUE_CONTEXT_JSON);
        if (docvalueJson != null && !docvalueJson.isEmpty()) {
            try {
                TypeReference<Map<String, String>> mapTypeRef =
                        new TypeReference<Map<String, String>>() {};
                Map<String, String> dvMap =
                        JSON_MAPPER.readValue(docvalueJson, mapTypeRef);
                output.append(prefix).append("ES doc-value fields: ")
                        .append(dvMap.keySet()).append("\n");
            } catch (Exception e) {
                output.append(prefix).append("ES doc-value fields: ")
                        .append("(parse error)").append("\n");
            }
        }
        String fieldsJson = properties.get(PROP_FIELDS_CONTEXT_JSON);
        if (fieldsJson != null && !fieldsJson.isEmpty()) {
            try {
                TypeReference<Map<String, String>> mapTypeRef =
                        new TypeReference<Map<String, String>>() {};
                Map<String, String> fMap =
                        JSON_MAPPER.readValue(fieldsJson, mapTypeRef);
                output.append(prefix).append("ES source fields: ")
                        .append(fMap.keySet()).append("\n");
            } catch (Exception e) {
                output.append(prefix).append("ES source fields: ")
                        .append("(parse error)").append("\n");
            }
        }
        // ATTN this deliberately does NOT repeat populateScanLevelParams' "limit fits in one batch" test, so
        // with a batch size below the limit EXPLAIN claims an early stop that is not actually requested. That
        // mismatch predates the move (the two halves used to live in different files, one with the test and
        // one without) and is preserved byte for byte here: the acceptance baseline for this relocation is
        // that EXPLAIN text does not change. Fixing it changes user-visible EXPLAIN and needs a live ES
        // cluster to verify, so it is tracked separately.
        long pushdownLimit = parseLongOrDefault(properties.get(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT), -1L);
        if (pushdownLimit > 0 && allConjunctsPushed(properties)) {
            output.append(prefix).append("ES terminate_after: ").append(pushdownLimit).append("\n");
        }
    }

    private List<String> collectAllHosts(
            Map<Integer, List<EsShardRouting>> routingsMap) {
        List<String> hosts = new ArrayList<>();
        for (List<EsShardRouting> routings : routingsMap.values()) {
            for (EsShardRouting routing : routings) {
                String addr = EsHostAddress.formatHostPort(routing.getHttpHost(), routing.getHttpPort());
                if (!hosts.contains(addr)) {
                    hosts.add(addr);
                }
            }
        }
        return hosts;
    }
}
