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

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.Daemon;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import okhttp3.Authenticator;
import okhttp3.Call;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

/**
 * it is used to call es api to get shard allocation state
 * @author yiguolei
 *
 */
public class EsStateStore extends Daemon {
    
    private static final Logger LOG = LogManager.getLogger(EsStateStore.class);

    private Map<Long, EsTable> esTables;
    
    public EsStateStore() {
        super(Config.es_state_sync_interval_second * 1000);
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
    
    protected void runOneCycle() {
        for (EsTable esTable : esTables.values()) {
            try {
                EsRestClient client = new EsRestClient(esTable.getSeeds(),
                        esTable.getUserName(), esTable.getPasswd());
//                EsTableState esTableState = loadEsIndexMetadataV55(esTable);
                String indexMetaData = client.getIndexMetaData(esTable.getIndexName());
                if (indexMetaData == null) {
                    continue;
                }
                EsTableState esTableState = parseClusterState55(indexMetaData, esTable);
                if (esTableState == null) {
                    continue;
                }
                if (EsTable.TRANSPORT_HTTP.equals(esTable.getTransport())) {
                    Map<String, EsNodeInfo> nodesInfo = client.getHttpNodes();
                    esTableState.addHttpAddress(nodesInfo);
                }
                esTable.setEsTableState(esTableState);
            } catch (Throwable e) {
                LOG.error("errors while load table {} state from es", esTable.getName());
            }
        }
    }
    
    // when fe is start to load image, should call this method to init the state store
    public void loadTableFromCatalog() {
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for(Long dbId : dbIds) {
            Database database = Catalog.getInstance().getDb(dbId);
            List<Table> tables = database.getTables();
            for (Table table : tables) {
                if (table.getType() == TableType.ELASTICSEARCH) {
                    esTables.put(table.getId(), (EsTable) table);
                }
            }
        }
    }
    
    private EsTableState loadEsIndexMetadataV55(final EsTable esTable) {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
        clientBuilder.authenticator(new Authenticator() {
            @Override
            public Request authenticate(Route route, Response response) throws IOException {
                String credential = Credentials.basic(esTable.getUserName(), esTable.getPasswd());
                return response.request().newBuilder().header("Authorization", credential).build();
            }
        });
        String[] seeds = esTable.getSeeds();
        for (String seed : seeds) {
            String url = seed + "/_cluster/state?indices=" 
                    + esTable.getIndexName()
                    + "&metric=routing_table,nodes,metadata&expand_wildcards=open";
            String basicAuth = "";
            try {
                Request request = new Request.Builder()
                        .get()
                        .url(url)
                        .addHeader("Authorization", basicAuth)
                        .build();
                Call call = clientBuilder.build().newCall(request);
                Response response = call.execute();
                String responseStr = response.body().string();
                if (response.isSuccessful()) {
                    try {
                        EsTableState esTableState = parseClusterState55(responseStr, esTable);
                        if (esTableState != null) {
                            return esTableState;
                        }
                    }
                    catch (Exception e) {
                        LOG.warn("errors while parse response msg {}", responseStr, e);
                    }
                } else {
                    LOG.info("errors while call es [{}] to get state info {}", url, responseStr);
                }
            } catch (Exception e) {
                LOG.warn("errors while call es [{}]", url, e);
            }
        }
        return null;
    }
    
    @VisibleForTesting
    public EsTableState parseClusterState55(String responseStr, EsTable esTable) 
            throws DdlException, AnalysisException, ExternalDataSourceException {
        JSONObject jsonObject = new JSONObject(responseStr);
        String clusterName = jsonObject.getString("cluster_name");
        JSONObject nodesMap = jsonObject.getJSONObject("nodes");

        JSONObject indicesMetaMap = jsonObject.getJSONObject("metadata").getJSONObject("indices");
        JSONObject indicesRoutingMap = jsonObject.getJSONObject("routing_table").getJSONObject("indices");
        EsTableState esTableState = new EsTableState(); 
        PartitionInfo partitionInfo = null;
        if (esTable.getPartitionInfo() != null) {
            if (esTable.getPartitionInfo() instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) esTable.getPartitionInfo();
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
                    LOG.debug("begin to parse es table [{}] state from cluster state," 
                            + " with partition info [{}]", esTable.getName(), sb.toString());
                }
            } else if (esTable.getPartitionInfo() instanceof SinglePartitionInfo) {
                LOG.debug("begin to parse es table [{}] state from cluster state, " 
                        + "with no partition info", esTable.getName());
            } else {
                throw new ExternalDataSourceException("es table only support range partition, " 
                            + "but current partition type is " 
                            + esTable.getPartitionInfo().getType());
            }
        }
        for (String indexName : indicesRoutingMap.keySet()) {
            EsIndexState indexState = EsIndexState.parseIndexStateV55(indexName, 
                    indicesRoutingMap, nodesMap, 
                    indicesMetaMap, partitionInfo);
            esTableState.addIndexState(indexName, indexState);
            LOG.debug("add index {} to es table {}", indexState, esTable.getName());
        }
        
        if (partitionInfo instanceof RangePartitionInfo) {
            // sort the index state according to partition key and then add to range map
            List<EsIndexState> esIndexStates = esTableState.getPartitionedIndexStates().values()
                                                        .stream().collect(Collectors.toList());
            Collections.sort(esIndexStates, new Comparator<EsIndexState>() {
                @Override
                public int compare(EsIndexState o1, EsIndexState o2) {
                    return o1.getPartitionKey().compareTo(o2.getPartitionKey());
                }
            });
            long partitionId = 0;
            for (EsIndexState esIndexState : esIndexStates) {
                Range<PartitionKey> range = ((RangePartitionInfo) partitionInfo).handleNewSinglePartitionDesc(
                        esIndexState.getPartitionDesc(), 
                        partitionId);
                esTableState.addPartition(esIndexState.getIndexName(), partitionId);
                esIndexState.setPartitionId(partitionId);
                ++ partitionId;
                LOG.debug("add parition to es table [{}] with range [{}]", esTable.getName(), range);
            }
        }
        return esTableState;
    }
}
