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

package org.apache.doris.datasource;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.NamedColumnHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.api.pushdown.ProjectionApplicationResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TEsScanNode;
import org.apache.doris.thrift.TEsScanRange;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

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
 * Scan node for plugin-driven ES catalogs.
 *
 * <p>Extends {@link ExternalScanNode} (like the legacy EsScanNode) to produce
 * {@code ES_HTTP_SCAN_NODE} Thrift types. All ES-specific logic (shard routing,
 * query DSL building, field context resolution) is delegated to the connector
 * plugin via {@link ConnectorScanPlanProvider}.</p>
 *
 * <p>This node is used when a {@link PluginDrivenExternalTable} belongs to an
 * ES-type connector (detected via {@code getScanRangeType() == ES_SCAN}).</p>
 */
public class PluginDrivenEsScanNode extends ExternalScanNode {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenEsScanNode.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE_REF =
            new TypeReference<Map<String, String>>() {};

    private final Connector connector;
    private final ConnectorSession connectorSession;
    private ConnectorTableHandle currentHandle;

    // Populated during finalize
    private Map<String, String> nodeProperties = Collections.emptyMap();

    public PluginDrivenEsScanNode(PlanNodeId id, TupleDescriptor desc,
            ScanContext scanContext, Connector connector,
            ConnectorSession connectorSession, ConnectorTableHandle tableHandle) {
        super(id, desc, "PluginDrivenEsScanNode", scanContext, false);
        this.connector = connector;
        this.connectorSession = connectorSession;
        this.currentHandle = tableHandle;
    }

    /**
     * Creates from catalog and table references.
     */
    public static PluginDrivenEsScanNode create(PlanNodeId id, TupleDescriptor desc,
            ScanContext scanContext, PluginDrivenExternalCatalog catalog,
            PluginDrivenExternalTable table) {
        Connector conn = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = conn.getMetadata(session);
        String dbName = table.getDb() != null ? table.getDb().getRemoteName() : "";
        String tableName = table.getRemoteName();
        ConnectorTableHandle handle = metadata.getTableHandle(session, dbName, tableName)
                .orElseThrow(() -> new RuntimeException(
                        "Table handle not found for ES table: " + dbName + "." + tableName));
        return new PluginDrivenEsScanNode(id, desc, scanContext, conn, session, handle);
    }

    @Override
    public void finalizeForNereids() throws UserException {
        // Attempt filter pushdown
        pushdownFilter();
        // Build scan ranges and node properties
        createScanRangeLocations();
    }

    private void pushdownFilter() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        ConnectorExpression combined = ExprToConnectorExpressionConverter.convertConjuncts(conjuncts);
        ConnectorFilterConstraint constraint = new ConnectorFilterConstraint(
                combined, Collections.emptyMap());
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(connectorSession, currentHandle, constraint);
        if (result.isPresent()) {
            FilterApplicationResult<ConnectorTableHandle> filterResult = result.get();
            currentHandle = filterResult.getHandle();
            ConnectorExpression remaining = filterResult.getRemainingFilter();
            if (remaining == null) {
                conjuncts.clear();
                LOG.debug("ES filter fully pushed down via applyFilter, cleared conjuncts");
            } else {
                LOG.debug("ES filter pushdown accepted with remaining, keeping conjuncts");
            }
        }
    }

    /**
     * Attempts to push the projection down via the SPI applyProjection() protocol.
     */
    private void tryPushDownProjection(List<ConnectorColumnHandle> columns) {
        if (columns.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(connectorSession, currentHandle, columns);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("ES projection pushed down via applyProjection");
        }
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        List<ConnectorColumnHandle> columns = buildColumnHandles();

        // Attempt projection pushdown via SPI protocol
        tryPushDownProjection(columns);

        Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();

        // Get scan ranges (shard routing)
        List<ConnectorScanRange> ranges = scanProvider.planScan(
                connectorSession, currentHandle, columns, remainingFilter);

        // Get node-level properties (query DSL, auth, doc values mode)
        nodeProperties = scanProvider.getScanNodeProperties(
                connectorSession, currentHandle, columns, remainingFilter);

        // Remove pushed-down conjuncts to avoid redundant evaluation by BE.
        // The connector returns indices of conjuncts that could NOT be pushed.
        pruneConjuncts(nodeProperties);

        // Convert to Thrift scan range locations
        for (ConnectorScanRange range : ranges) {
            TScanRangeLocations locations = convertToThrift(range);
            scanRangeLocations.add(locations);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("ES plugin scan: {} ranges, query_dsl={}",
                    scanRangeLocations.size(),
                    nodeProperties.getOrDefault("query_dsl", "N/A"));
        }
    }

    private TScanRangeLocations convertToThrift(ConnectorScanRange range) throws UserException {
        Map<String, String> props = range.getProperties();
        TScanRangeLocations locations = new TScanRangeLocations();

        // Assign BE backends
        List<String> hosts = range.getHosts();
        FederationBackendPolicy bePolicy = new FederationBackendPolicy();
        bePolicy.init(hosts);
        int numBackends = bePolicy.numBackends();
        for (int i = 0; i < numBackends; i++) {
            TScanRangeLocation location = new TScanRangeLocation();
            Backend be = bePolicy.getNextBe();
            location.setBackendId(be.getId());
            location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
            locations.addToLocations(location);
        }

        // Build TEsScanRange
        TEsScanRange esScanRange = new TEsScanRange();

        // ES hosts
        String hostsStr = props.getOrDefault("es.hosts", "");
        List<TNetworkAddress> esHosts = new ArrayList<>();
        for (String hostPort : hostsStr.split(",")) {
            String trimmed = hostPort.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // Strip scheme prefix (http:// or https://) if present
            String hostAndPort = trimmed;
            int schemeEnd = hostAndPort.indexOf("://");
            if (schemeEnd >= 0) {
                hostAndPort = hostAndPort.substring(schemeEnd + 3);
            }
            int colonIdx = hostAndPort.lastIndexOf(':');
            String host;
            int port;
            if (colonIdx > 0) {
                host = hostAndPort.substring(0, colonIdx);
                port = Integer.parseInt(hostAndPort.substring(colonIdx + 1));
            } else {
                host = hostAndPort;
                port = 9200;
            }
            esHosts.add(new TNetworkAddress(host, port));
        }
        esScanRange.setEsHosts(esHosts);

        // Index
        String index = props.getOrDefault("es.index", "");
        esScanRange.setIndex(index);

        // Type
        String type = props.getOrDefault("es.type", null);
        if (type != null) {
            esScanRange.setType(type);
        }

        // Shard ID
        int shardId = Integer.parseInt(props.getOrDefault("es.shard_id", "-1"));
        esScanRange.setShardId(shardId);

        TScanRange scanRange = new TScanRange();
        scanRange.setEsScanRange(esScanRange);
        locations.setScanRange(scanRange);

        return locations;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ES_HTTP_SCAN_NODE;

        Map<String, String> esProperties = new HashMap<>(nodeProperties);

        TEsScanNode esScanNode = new TEsScanNode(desc.getId().asInt());

        // Deserialize docvalue_context and fields_context from JSON flat properties.
        // The ES connector serializes these Map<String,String> as JSON strings
        // into the generic flat properties to avoid ES-specific API on the SPI.
        String docvalueJson = esProperties.remove("docvalue_context_json");
        if (docvalueJson != null && !docvalueJson.isEmpty()) {
            try {
                Map<String, String> docvalueContext = OBJECT_MAPPER.readValue(
                        docvalueJson, MAP_TYPE_REF);
                esScanNode.setDocvalueContext(docvalueContext);
            } catch (Exception e) {
                LOG.warn("Failed to deserialize docvalue_context_json", e);
            }
        }

        String fieldsJson = esProperties.remove("fields_context_json");
        if (fieldsJson != null && !fieldsJson.isEmpty()) {
            try {
                Map<String, String> fieldsContext = OBJECT_MAPPER.readValue(
                        fieldsJson, MAP_TYPE_REF);
                esScanNode.setFieldsContext(fieldsContext);
            } catch (Exception e) {
                LOG.warn("Failed to deserialize fields_context_json", e);
            }
        }

        esScanNode.setProperties(esProperties);
        msg.es_scan_node = esScanNode;
        super.toThrift(msg);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        String tableName = nodeProperties.getOrDefault("_table_name",
                desc != null && desc.getTable() != null ? desc.getTable().getName() : "PluginDrivenES");
        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        String sortColumn = nodeProperties.get("sort_column");
        if (sortColumn != null) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("LOCAL_PREDICATES: ")
                    .append(getExplainString(conjuncts)).append("\n");
        }
        String queryDsl = nodeProperties.getOrDefault("query_dsl", "N/A");
        output.append(prefix).append("REMOTE_PREDICATES: ")
                .append(queryDsl).append("\n");

        String indexName = nodeProperties.get("_es_index");
        String typeName = nodeProperties.get("_es_type");
        if (indexName != null) {
            output.append(prefix).append(String.format("ES index/type: %s/%s",
                    indexName, typeName != null ? typeName : "_doc")).append("\n");
        }

        String docValueScan = nodeProperties.get("doc_values_mode");
        if (docValueScan != null) {
            output.append(prefix).append("DOC_VALUE_SCAN: ").append(docValueScan).append("\n");
        }

        return output.toString();
    }

    private List<ConnectorColumnHandle> buildColumnHandles() {
        ArrayList<SlotDescriptor> slots = desc.getSlots();
        if (slots == null || slots.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumnHandle> handles = new ArrayList<>(slots.size());
        for (SlotDescriptor slot : slots) {
            handles.add(new NamedColumnHandle(slot.getColumn().getName()));
        }
        return handles;
    }

    private Optional<ConnectorExpression> buildRemainingFilter() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ExprToConnectorExpressionConverter.convertConjuncts(conjuncts));
    }

    /**
     * Prune conjuncts that were successfully pushed down to ES.
     *
     * <p>The connector returns a comma-separated list of indices for conjuncts
     * that could NOT be pushed. We keep only those and remove the rest,
     * matching the old EsScanNode behavior.</p>
     */
    private void pruneConjuncts(Map<String, String> props) {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        String notPushedStr = props.get("_not_pushed_conjunct_indices");
        if (notPushedStr == null) {
            // No info → all conjuncts were pushed
            conjuncts.clear();
            return;
        }
        Set<Integer> notPushedSet = new HashSet<>();
        for (String idx : notPushedStr.split(",")) {
            notPushedSet.add(Integer.parseInt(idx.trim()));
        }
        List<Expr> remaining = new ArrayList<>();
        for (int i = 0; i < conjuncts.size(); i++) {
            if (notPushedSet.contains(i)) {
                remaining.add(conjuncts.get(i));
            }
        }
        conjuncts.clear();
        conjuncts.addAll(remaining);
    }
}
