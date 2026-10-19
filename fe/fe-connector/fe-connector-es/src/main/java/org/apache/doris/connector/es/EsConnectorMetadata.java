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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata operations for Elasticsearch connector.
 * Provides database/table listing and schema retrieval via the connector SPI.
 */
public class EsConnectorMetadata implements ConnectorMetadata {

    public static final String DEFAULT_DB = "default_db";

    private final EsConnectorRestClient restClient;
    private final EsCatalogProperties props;

    // ES-F3 per-statement schema memo. This metadata instance is created fresh per statement
    // (funnel-memoized one-per-statement), so an index's mapping is resolved into columns once and
    // reused, collapsing the repeated getColumnHandles->getTableSchema remote mapping fetches to one
    // per index per statement. Read-only metadata -> no in-statement invalidation. ConcurrentHashMap
    // to match the maxcompute handle-memo precedent (cheap defensiveness against any concurrent
    // metadata access within a statement).
    private final Map<String, ConnectorTableSchema> schemaMemo = new ConcurrentHashMap<>();

    public EsConnectorMetadata(EsConnectorRestClient restClient,
            EsCatalogProperties props) {
        this.restClient = restClient;
        this.props = props;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return Collections.singletonList(DEFAULT_DB);
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return DEFAULT_DB.equals(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return restClient.listTable(props.isIncludeHiddenIndex());
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String db, String table) {
        if (restClient.existIndex(table)) {
            return Optional.of(new EsTableHandle(table));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        EsTableHandle esHandle = (EsTableHandle) handle;
        String indexName = esHandle.getIndexName();
        return schemaMemo.computeIfAbsent(indexName, idx -> {
            // Share the raw mapping with the scan path via the per-statement scope (ES-F2): one
            // getMapping per index per statement across both paths. The schema memo above still
            // collapses repeat getTableSchema calls within this metadata instance.
            String mapping = EsStatementScope.sharedIndexMapping(
                    session, idx, () -> restClient.getMapping(idx));
            List<ConnectorColumn> columns = EsTypeMapping.parseMapping(
                    idx, mapping, props.isMappingEsId());
            return new ConnectorTableSchema(idx, columns, "ELASTICSEARCH",
                    Collections.emptyMap());
        });
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        ConnectorTableSchema schema = getTableSchema(session, handle);
        List<ConnectorColumn> columns = schema.getColumns();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(columns.size());
        for (ConnectorColumn col : columns) {
            handles.put(col.getName(), new NamedColumnHandle(col.getName()));
        }
        return handles;
    }

    /**
     * Elasticsearch accepts CAST-bearing predicates ({@code true}, the SPI default, stated here rather than
     * inherited).
     *
     * <p>This is a conscious acceptance of the risk the SPI documents, not a claim of safety: the residual
     * predicate is compiled into the ES query DSL ({@code EsScanPlanProvider.buildQueryDsl}) and evaluated by
     * Elasticsearch, so a comparison whose literal ES matches differently than Doris coerced it drops rows AT
     * THE SOURCE. It stays {@code true} for parity with the legacy {@code EsScanNode}, which built the same
     * DSL; unconvertible conjuncts are already reported back as not-pushed and re-evaluated by BE.</p>
     */
    @Override
    public boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return true;
    }

    @Override
    public org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        org.apache.doris.thrift.TEsTable tEsTable = new org.apache.doris.thrift.TEsTable();
        org.apache.doris.thrift.TTableDescriptor desc = new org.apache.doris.thrift.TTableDescriptor(
                tableId, org.apache.doris.thrift.TTableType.ES_TABLE,
                numCols, 0, tableName, "");
        desc.setEsTable(tEsTable);
        return desc;
    }

    /**
     * Fetch full metadata state for an index, including field contexts and shard routing.
     * This is the plugin-side equivalent of fe-core's EsMetaStateTracker.run().
     *
     * @param indexName the ES index name
     * @param columnNames column names to resolve field contexts for
     * @return fully populated EsMetadataState
     */
    public EsMetadataState fetchMetadataState(ConnectorSession session, String indexName,
            List<String> columnNames) {
        EsMetadataState state = new EsMetadataState(
                indexName, props.getMappingType(), columnNames, props.isNodesDiscovery(),
                props.getSeeds());
        EsMetadataFetcher fetcher = new EsMetadataFetcher(restClient, state, session);
        return fetcher.fetch();
    }

    /**
     * Convenience method to fetch metadata state using column names from schema.
     */
    public EsMetadataState fetchMetadataState(ConnectorSession session,
            ConnectorTableHandle handle) {
        ConnectorTableSchema schema = getTableSchema(session, handle);
        List<String> columnNames = new ArrayList<>();
        for (ConnectorColumn col : schema.getColumns()) {
            columnNames.add(col.getName());
        }
        EsTableHandle esHandle = (EsTableHandle) handle;
        return fetchMetadataState(session, esHandle.getIndexName(), columnNames);
    }
}
