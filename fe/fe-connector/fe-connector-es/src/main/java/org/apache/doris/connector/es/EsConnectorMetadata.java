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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.NamedColumnHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Metadata operations for Elasticsearch connector.
 * Provides database/table listing and schema retrieval via the connector SPI.
 */
public class EsConnectorMetadata implements ConnectorMetadata {

    public static final String DEFAULT_DB = "default_db";

    private final EsConnectorRestClient restClient;
    private final Map<String, String> properties;

    public EsConnectorMetadata(EsConnectorRestClient restClient,
            Map<String, String> properties) {
        this.restClient = restClient;
        this.properties = properties;
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
        boolean includeHidden = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.INCLUDE_HIDDEN_INDEX,
                EsConnectorProperties.INCLUDE_HIDDEN_INDEX_DEFAULT));
        return restClient.listTable(includeHidden);
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
        String mapping = restClient.getMapping(indexName);
        boolean mappingEsId = Boolean.parseBoolean(properties.getOrDefault(
                EsConnectorProperties.MAPPING_ES_ID,
                EsConnectorProperties.MAPPING_ES_ID_DEFAULT));

        List<ConnectorColumn> columns = EsTypeMapping.parseMapping(
                indexName, mapping, mappingEsId);
        return new ConnectorTableSchema(indexName, columns, "ELASTICSEARCH",
                Collections.emptyMap());
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
     * Validates that required properties are present.
     * Called during connector creation.
     */
    public static void validateProperties(Map<String, String> properties) {
        if (!properties.containsKey(EsConnectorProperties.HOSTS)
                || properties.get(EsConnectorProperties.HOSTS).trim().isEmpty()) {
            throw new DorisConnectorException(
                    "Required property '" + EsConnectorProperties.HOSTS + "' is missing");
        }
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
    public EsMetadataState fetchMetadataState(String indexName, List<String> columnNames) {
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
        return fetchMetadataState(esHandle.getIndexName(), columnNames);
    }
}
