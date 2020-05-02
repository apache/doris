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

package org.apache.doris.external;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * it is used to call es api to get shard allocation state
 */
public class EsStateStore extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(EsStateStore.class);

    private Map<Long, EsTable> esTables;

    public EsStateStore() {
        super("es state store", Config.es_state_sync_interval_second * 1000);
        esTables = Maps.newConcurrentMap();
    }

    public void registerTable(EsTable esTable) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        esTables.put(esTable.getId(), esTable);
        LOG.info("register a new table [{}] to sync list", esTable.toString());
    }

    public void deRegisterTable(long tableId) {
        esTables.remove(tableId);
        LOG.info("deregister table [{}] from sync list", tableId);
    }

    @Override
    protected void runAfterCatalogReady() {
        for (EsTable esTable : esTables.values()) {
            try {
                EsRestClient client = new EsRestClient(esTable.getSeeds(),
                    esTable.getUserName(), esTable.getPasswd());

                String indexMapping = client.getIndexMapping(esTable.getIndexName());
                if (indexMapping == null) {
                    continue;
                }
                loadEsIndexMapping(indexMapping, esTable);

                String shardLocation = client.getSearchShards(esTable.getIndexName());
                EsTableState esTableState = loadEsSearchShards(shardLocation, esTable);
                if (esTableState == null) {
                    continue;
                }

                if (EsTable.TRANSPORT_HTTP.equals(esTable.getTransport())) {
                    Map<String, EsNodeInfo> nodesInfo = client.getHttpNodes();
                    esTableState.addHttpAddress(nodesInfo);
                }
                esTable.setEsTableState(esTableState);
            } catch (Throwable e) {
                LOG.warn("Exception happens when fetch index [{}] meta data from remote es cluster",
                    esTable.getName(), e);
            }
        }
    }

    // should call this method to init the state store after loading image
    // the rest of tables will be added or removed by replaying edit log
    // when fe is start to load image, should call this method to init the state store

    public void loadTableFromCatalog() {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database database = Catalog.getCurrentCatalog().getDb(dbId);

            List<Table> tables = database.getTables();
            for (Table table : tables) {
                if (table.getType() == TableType.ELASTICSEARCH) {
                    esTables.put(table.getId(), (EsTable) table);
                }
            }
        }
    }

    // Configure keyword and doc_values by mapping
    public void loadEsIndexMapping(String indexMapping, EsTable esTable) {
        JSONObject jsonObject = new JSONObject(indexMapping);
        // the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keys();
        if ((esTable.isKeywordSniffEnable() || esTable.isDocValueScanEnable()) && keys.hasNext()) {
            String docKey = keys.next();
            JSONObject docData = jsonObject.optJSONObject(docKey);
            JSONObject mappings = docData.optJSONObject("mappings");
            JSONObject rootSchema = mappings.optJSONObject(esTable.getMappingType());
            JSONObject schema = rootSchema.optJSONObject("properties");
            // we build the doc value context for fields maybe used for scanning
            // "properties": {
            //      "city": {
            //        "type": "text", // text field does not have docvalue
            //        "fields": {
            //          "raw": {
            //            "type":  "keyword"
            //          }
            //        }
            //      }
            //    }
            // then the docvalue context provided the mapping between the select field and real request field :
            // {"city": "city.raw"}
            List<Column> colList = esTable.getFullSchema();
            for (Column col : colList) {
                String colName = col.getName();
                if (!schema.has(colName)) {
                    continue;
                }
                JSONObject fieldObject = schema.optJSONObject(colName);
                String fieldType = fieldObject.optString("type");
                // string-type field used keyword type to generate predicate
                if (esTable.isKeywordSniffEnable()) {
                    // if text field type seen, we should use the `field` keyword type?
                    if ("text".equals(fieldType)) {
                        JSONObject fieldsObject = fieldObject.optJSONObject("fields");
                        if (fieldsObject != null) {
                            for (String key : fieldsObject.keySet()) {
                                JSONObject innerTypeObject = fieldsObject.optJSONObject(key);
                                // just for text type
                                if ("keyword".equals(innerTypeObject.optString("type"))) {
                                    esTable.addFetchField(colName, colName + "." + key);
                                }
                            }
                        }
                    }
                }

                if (esTable.isDocValueScanEnable()) {
                    if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(fieldType)) {
                        JSONObject fieldsObject = fieldObject.optJSONObject("fields");
                        if (fieldsObject != null) {
                            for (String key : fieldsObject.keySet()) {
                                JSONObject innerTypeObject = fieldsObject.optJSONObject(key);
                                if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS
                                    .contains(innerTypeObject.optString("type"))) {
                                    continue;
                                }
                                if (innerTypeObject.has("doc_values")) {
                                    boolean docValue = innerTypeObject.getBoolean("doc_values");
                                    if (docValue) {
                                        esTable.addDocValueField(colName, colName);
                                    }
                                } else {
                                    // a : {c : {}} -> a -> a.c
                                    esTable.addDocValueField(colName, colName + "." + key);
                                }
                            }
                        }
                        // skip this field
                        continue;
                    }
                    // set doc_value = false manually
                    if (fieldObject.has("doc_values")) {
                        boolean docValue = fieldObject.optBoolean("doc_values");
                        if (!docValue) {
                            continue;
                        }
                    }
                    esTable.addDocValueField(colName, colName);
                }
            }
        }
    }

    public EsTableState loadEsSearchShards(String shardLocation, EsTable esTable)
        throws ExternalDataSourceException, DdlException {
        JSONObject jsonObject = new JSONObject(shardLocation);
        JSONObject indicesRoutingMap = jsonObject.getJSONObject("nodes");
        JSONArray shards = jsonObject.getJSONArray("shards");
        EsTableState esTableState = new EsTableState();
        RangePartitionInfo partitionInfo = null;
        if (esTable.getPartitionInfo() != null) {
            if (esTable.getPartitionInfo() instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) esTable
                    .getPartitionInfo();
                partitionInfo = new RangePartitionInfo(rangePartitionInfo.getPartitionColumns());
                esTableState.setPartitionInfo(partitionInfo);
                if (LOG.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    int idx = 0;
                    for (Column column : rangePartitionInfo.getPartitionColumns()) {
                        if (idx != 0) {
                            sb.append(", ");
                        }
                        sb.append("`").append(column.getName()).append("`");
                        idx++;
                    }
                    sb.append(")");
                    LOG.debug("begin to parse es table [{}] state from search shards,"
                        + " with partition info [{}]", esTable.getName(), sb.toString());
                }
            } else if (esTable.getPartitionInfo() instanceof SinglePartitionInfo) {
                LOG.debug("begin to parse es table [{}] state from search shards, "
                    + "with no partition info", esTable.getName());
            } else {
                throw new ExternalDataSourceException("es table only support range partition, "
                    + "but current partition type is "
                    + esTable.getPartitionInfo().getType());
            }
        }

        EsIndexState indexState = EsIndexState.parseIndexState(esTable.getIndexName(),
            indicesRoutingMap, shards);
        esTableState.addIndexState(esTable.getIndexName(), indexState);
        LOG.debug("add index {} to es table {}", indexState, esTable.getName());
        if (partitionInfo != null) {
            // sort the index state according to partition key and then add to range map
            List<EsIndexState> esIndexStates = new ArrayList<>(
                esTableState.getPartitionedIndexStates().values());
            esIndexStates.sort(Comparator.comparing(EsIndexState::getPartitionKey));
            long partitionId = 0;
            for (EsIndexState esIndexState : esIndexStates) {
                Range<PartitionKey> range = partitionInfo.handleNewSinglePartitionDesc(
                    esIndexState.getPartitionDesc(), partitionId, false);
                esTableState.addPartition(esIndexState.getIndexName(), partitionId);
                esIndexState.setPartitionId(partitionId);
                ++partitionId;
                LOG.debug("add parition to es table [{}] with range [{}]", esTable.getName(),
                    range);
            }
        }
        return esTableState;
    }

}
