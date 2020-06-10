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

package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.persist.Storage;

import com.google.gson.Gson;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

public class ShowMetaInfoAction extends RestBaseAction {
    private enum Action {
        SHOW_DB_SIZE,
        SHOW_HA,
        INVALID;
        
        public static Action getAction(String str) {
            try {
                return valueOf(str);
            } catch (Exception ex) {
                return INVALID;
            }
        }
    }

    private static final Logger LOG = LogManager.getLogger(ShowMetaInfoAction.class);

    public ShowMetaInfoAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_meta_info",
                                   new ShowMetaInfoAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String action = request.getSingleParameter("action");
        Gson gson = new Gson();
        response.setContentType("application/json");

        switch (Action.getAction(action.toUpperCase())) {
            case SHOW_DB_SIZE:
                response.getContent().append(gson.toJson(getDataSize()));
                break;
            case SHOW_HA:
                response.getContent().append(gson.toJson(getHaInfo()));
                break;
            default:
                break;
        }
        sendResult(request, response);
    }
    
    public Map<String, String> getHaInfo() {
        HashMap<String, String> feInfo = new HashMap<String, String>();
        feInfo.put("role", Catalog.getCurrentCatalog().getFeType().toString());
        if (Catalog.getCurrentCatalog().isMaster()) {
            feInfo.put("current_journal_id",
                       String.valueOf(Catalog.getCurrentCatalog().getEditLog().getMaxJournalId()));
        } else {
            feInfo.put("current_journal_id",
                       String.valueOf(Catalog.getCurrentCatalog().getReplayedJournalId()));
        }

        HAProtocol haProtocol = Catalog.getCurrentCatalog().getHaProtocol();
        if (haProtocol != null) {

            InetSocketAddress master = null;
            try {
                master = haProtocol.getLeader();
            } catch (Exception e) {
                // this may happen when majority of FOLLOWERS are down and no MASTER right now.
                LOG.warn("failed to get leader: {}", e.getMessage());
            }
            if (master != null) {
                feInfo.put("master", master.getHostString());
            } else {
                feInfo.put("master", "unknown");
            }

            List<InetSocketAddress> electableNodes = haProtocol.getElectableNodes(false);
            ArrayList<String> electableNodeNames = new ArrayList<String>();
            if (!electableNodes.isEmpty()) {
                for (InetSocketAddress node : electableNodes) {
                    electableNodeNames.add(node.getHostString());
                }
                feInfo.put("electable_nodes", StringUtils.join(electableNodeNames.toArray(), ","));
            }

            List<InetSocketAddress> observerNodes = haProtocol.getObserverNodes();
            ArrayList<String> observerNodeNames = new ArrayList<String>();
            if (observerNodes != null) {
                for (InetSocketAddress node : observerNodes) {
                    observerNodeNames.add(node.getHostString());
                }
                feInfo.put("observer_nodes", StringUtils.join(observerNodeNames.toArray(), ","));
            }
        }

        feInfo.put("can_read", String.valueOf(Catalog.getCurrentCatalog().canRead()));
        feInfo.put("is_ready", String.valueOf(Catalog.getCurrentCatalog().isReady()));
        try {
            Storage storage = new Storage(Config.meta_dir + "/image");
            feInfo.put("last_checkpoint_version", String.valueOf(storage.getImageSeq()));
            long lastCheckpointTime = storage.getCurrentImageFile().lastModified();
            feInfo.put("last_checkpoint_time", String.valueOf(lastCheckpointTime));
        } catch (IOException e) {
            LOG.warn(e.getMessage());
        }
        return feInfo;
    }

    public Map<String, Long> getDataSize() {
        Map<String, Long> result = new HashMap<String, Long>();
        List<String> dbNames = Catalog.getCurrentCatalog().getDbNames();

        for (int i = 0; i < dbNames.size(); i++) {
            String dbName = dbNames.get(i);
            Database db = Catalog.getCurrentCatalog().getDb(dbName);

            long totalSize = 0;
            List<Table> tables = db.getTables();
            for (int j = 0; j < tables.size(); j++) {
                Table table = tables.get(j);
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableSize = 0;
                for (Partition partition : olapTable.getAllPartitions()) {
                    long partitionSize = 0;
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        long indexSize = 0;
                        for (Tablet tablet : mIndex.getTablets()) {
                            long maxReplicaSize = 0;
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getState() == ReplicaState.NORMAL
                                        || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                                    if (replica.getDataSize() > maxReplicaSize) {
                                        maxReplicaSize = replica.getDataSize();
                                    }
                                }
                            } // end for replicas
                            indexSize += maxReplicaSize;
                        } // end for tablets
                        partitionSize += indexSize;
                    } // end for tables
                    tableSize += partitionSize;
                } // end for partitions
                totalSize += tableSize;
            } // end for tables
            result.put(dbName, Long.valueOf(totalSize));
        } // end for dbs
        return result;
    }
}
