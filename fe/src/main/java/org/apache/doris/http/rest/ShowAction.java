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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.doris.analysis.RedirectStatus;
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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.Storage;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class ShowAction extends RestBaseController{

    private static final Logger LOG = LogManager.getLogger(ShowAction.class);

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


    @RequestMapping(path = "/api/show_meta_info",method = RequestMethod.GET)
    public Object show_meta_info(HttpServletRequest request, HttpServletResponse response) {
        String action = request.getParameter("action");
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        switch (Action.getAction(action.toUpperCase())) {
            case SHOW_DB_SIZE:
                entity.setData(getDataSize());
                break;
            case SHOW_HA:
                entity.setData(getHaInfo());
                break;
            default:
                break;
        }
        return entity;
    }


    // Format:
    //http://username:password@192.168.1.1:8030/api/show_proc?path=/
    @RequestMapping(path = "/api/show_proc",method = RequestMethod.GET)
    public Object show_proc(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        // check authority
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        String path = request.getParameter("path");
        String forward = request.getParameter("forward");
        boolean isForward = false;
        if (!Strings.isNullOrEmpty(forward) && forward.equals("true")) {
            isForward = true;
        }

        // forward to master if necessary
        if (!Catalog.getCurrentCatalog().isMaster() && isForward) {
            String showProcStmt = "SHOW PROC \"" + path + "\"";
            ConnectContext context = new ConnectContext(null);
            context.setCatalog(Catalog.getCurrentCatalog());
            context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
            context.setQualifiedUser(ConnectContext.get().getQualifiedUser());
            context.setRemoteIP(ConnectContext.get().getRemoteIP());
            MasterOpExecutor masterOpExecutor = new MasterOpExecutor(new OriginStatement(showProcStmt, 0), context,
                    RedirectStatus.FORWARD_NO_SYNC);
            LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());

            try {
                masterOpExecutor.execute();
            } catch (Exception e) {
                entity.setMsg("Failed to forward stmt: " + e.getMessage());
                entity.setCode(HttpStatus.BAD_REQUEST.value());
                return entity;
            }

            ShowResultSet resultSet = masterOpExecutor.getProxyResultSet();
            if (resultSet == null) {
                entity.setMsg("Failed to get result set");
                entity.setCode(HttpStatus.BAD_REQUEST.value());
                return entity;
            }
            entity.setData(resultSet.getResultRows());
        } else {
            ProcNodeInterface procNode = null;
            ProcService instance = ProcService.getInstance();
            try {
                if (Strings.isNullOrEmpty(path)) {
                    procNode = instance.open("/");
                } else {
                    procNode = instance.open(path);
                }
            } catch (AnalysisException e) {
                LOG.warn(e.getMessage());
                //response.getContent().append("[]");
            }

            if (procNode != null) {
                ProcResult result;
                try {
                    result = procNode.fetchResult();
                    List<List<String>> rows = result.getRows();
                    entity.setData(rows);
                } catch (AnalysisException e) {
                    LOG.warn(e.getMessage());
                    //response.getContent().append("[]");
                }
            }
        }
        return entity;
    }


    @RequestMapping(path = "/api/show_runtime_info",method = RequestMethod.GET)
    public Object show_runtime_info(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        HashMap<String, String> feInfo = new HashMap<String, String>();
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        // Get memory info
        Runtime r = Runtime.getRuntime();
        feInfo.put("free_mem", String.valueOf(r.freeMemory()));
        feInfo.put("total_mem", String.valueOf(r.totalMemory()));
        feInfo.put("max_mem", String.valueOf(r.maxMemory()));

        // Get thread count
        ThreadGroup parentThread;
        for (parentThread = Thread.currentThread().getThreadGroup();
             parentThread.getParent() != null;
             parentThread = parentThread.getParent()) {
        };
        feInfo.put("thread_cnt", String.valueOf(parentThread.activeCount()));

        entity.setData(feInfo);
        return feInfo;
    }

    private Map<String, String> getHaInfo() {
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

    private Map<String, Long> getDataSize() {
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
