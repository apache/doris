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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.Storage;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class ShowAction extends RestBaseController {

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

    @RequestMapping(path = "/api/show_meta_info", method = RequestMethod.GET)
    public Object show_meta_info(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        }

        String action = request.getParameter("action");
        if (Strings.isNullOrEmpty(action)) {
            return ResponseEntityBuilder.badRequest("Missing action parameter");
        }
        switch (Action.getAction(action.toUpperCase())) {
            case SHOW_DB_SIZE:
                return ResponseEntityBuilder.ok(getDataSize());
            case SHOW_HA:
                try {
                    return ResponseEntityBuilder.ok(getHaInfo());
                } catch (IOException e) {
                    return ResponseEntityBuilder.internalError(e.getMessage());
                }
            default:
                return ResponseEntityBuilder.badRequest("Unknown action: " + action);
        }
    }

    // Format:
    //http://username:password@192.168.1.1:8030/api/show_proc?path=/
    @RequestMapping(path = "/api/show_proc", method = RequestMethod.GET)
    public Object show_proc(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        // check authority
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String path = request.getParameter("path");
        String forward = request.getParameter("forward");
        boolean isForward = false;
        if (!Strings.isNullOrEmpty(forward) && forward.equals("true")) {
            isForward = true;
        }

        // forward to master if necessary
        if (!Env.getCurrentEnv().isMaster() && isForward) {
            try {
                RedirectView redirectView = redirectToMasterOrException(request, response);
                Preconditions.checkNotNull(redirectView);
                return redirectView;
            } catch (Exception e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
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
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }

            if (procNode != null) {
                ProcResult result;
                try {
                    result = procNode.fetchResult();
                    List<List<String>> rows = result.getRows();
                    return ResponseEntityBuilder.ok(rows);
                } catch (AnalysisException e) {
                    return ResponseEntityBuilder.okWithCommonError(e.getMessage());
                }
            } else {
                return ResponseEntityBuilder.badRequest("Invalid proc path: " + path);
            }
        }
    }

    @RequestMapping(path = "/api/show_runtime_info", method = RequestMethod.GET)
    public Object show_runtime_info(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        }

        HashMap<String, String> feInfo = new HashMap<String, String>();

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
        }
        feInfo.put("thread_cnt", String.valueOf(parentThread.activeCount()));

        return ResponseEntityBuilder.ok(feInfo);
    }

    @RequestMapping(path = "/api/show_data", method = RequestMethod.GET)
    public Object show_data(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        }

        Map<String, Long> oneEntry = Maps.newHashMap();

        String dbName = request.getParameter(DB_KEY);
        long totalSize = 0;
        if (dbName != null) {
            String fullDbName = getFullDbName(dbName);
            DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(fullDbName);
            if (db == null) {
                return ResponseEntityBuilder.okWithCommonError("database " + fullDbName + " not found.");
            }
            totalSize = getDataSizeOfDatabase(db);
            oneEntry.put(fullDbName, totalSize);
        } else {
            for (long dbId : Env.getCurrentInternalCatalog().getDbIds()) {
                DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
                if (db == null || !(db instanceof Database) || db instanceof MysqlCompatibleDatabase) {
                    continue;
                }
                totalSize += getDataSizeOfDatabase(db);
            }
            oneEntry.put("__total_size", totalSize);
        }
        return ResponseEntityBuilder.ok(oneEntry);
    }

    @RequestMapping(path = "/api/show_table_data", method = RequestMethod.GET)
    public Object show_table_data(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }
        String dbName = request.getParameter(DB_KEY);
        String tableName = request.getParameter(TABLE_KEY);

        if (StringUtils.isEmpty(dbName) && StringUtils.isEmpty(tableName)) {
            return ResponseEntityBuilder.okWithCommonError("db and table cannot be empty at the same time");
        }

        String singleReplica = request.getParameter(SINGLE_REPLICA_KEY);
        boolean singleReplicaBool = Boolean.parseBoolean(singleReplica);
        Map<String, Map<String, Long>> oneEntry = Maps.newHashMap();
        if (dbName != null) {
            String fullDbName = getFullDbName(dbName);
            if (!StringUtils.isEmpty(tableName) && Config.enable_all_http_auth) {
                checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.SHOW);
            }

            DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(fullDbName);
            if (db == null) {
                return ResponseEntityBuilder.okWithCommonError("database " + fullDbName + " not found.");
            }
            Map<String, Long> tablesEntry = getDataSizeOfTables(db, tableName, singleReplicaBool);
            oneEntry.put(ClusterNamespace.getNameFromFullName(fullDbName), tablesEntry);
        } else {
            for (long dbId : Env.getCurrentInternalCatalog().getDbIds()) {
                DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
                if (db == null || !(db instanceof Database) || ((Database) db) instanceof MysqlCompatibleDatabase) {
                    continue;
                }
                if (Config.enable_all_http_auth && !Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get().getCurrentUserIdentity(),
                                InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(), tableName,
                                PrivPredicate.SHOW)) {
                    continue;
                }
                Map<String, Long> tablesEntry = getDataSizeOfTables(db, tableName, singleReplicaBool);
                oneEntry.put(ClusterNamespace.getNameFromFullName(db.getFullName()), tablesEntry);
            }
        }
        return ResponseEntityBuilder.ok(oneEntry);
    }

    private Map<String, String> getHaInfo() throws IOException {
        HashMap<String, String> feInfo = new HashMap<String, String>();
        feInfo.put("role", Env.getCurrentEnv().getFeType().toString());
        if (Env.getCurrentEnv().isMaster()) {
            feInfo.put("current_journal_id",
                    String.valueOf(Env.getCurrentEnv().getEditLog().getMaxJournalId()));
        } else {
            feInfo.put("current_journal_id",
                    String.valueOf(Env.getCurrentEnv().getReplayedJournalId()));
        }

        HAProtocol haProtocol = Env.getCurrentEnv().getHaProtocol();
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
            if (electableNodes != null) {
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

        feInfo.put("can_read", String.valueOf(Env.getCurrentEnv().canRead()));
        feInfo.put("is_ready", String.valueOf(Env.getCurrentEnv().isReady()));

        Storage storage = new Storage(Config.meta_dir + "/image");
        feInfo.put("last_checkpoint_version", String.valueOf(storage.getLatestImageSeq()));
        long lastCheckpointTime = storage.getCurrentImageFile().lastModified();
        feInfo.put("last_checkpoint_time", String.valueOf(lastCheckpointTime));

        return feInfo;
    }

    public long getDataSizeOfDatabase(DatabaseIf db) {
        long totalSize = 0;
        db.readLock();
        try {
            // sort by table name
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                if (!table.isManagedTable()) {
                    continue;
                }
                table.readLock();
                try {
                    long tableSize = ((OlapTable) table).getDataSize();
                    totalSize += tableSize;
                } finally {
                    table.readUnlock();
                }
            } // end for tables
        } finally {
            db.readUnlock();
        }
        return totalSize;
    }

    private Map<String, Long> getDataSizeOfTables(DatabaseIf db, String tableName, boolean singleReplica) {
        Map<String, Long> oneEntry = Maps.newHashMap();
        db.readLock();
        try {
            if (Strings.isNullOrEmpty(tableName)) {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    if (Config.enable_all_http_auth && !Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                                    table.getName(),
                                    PrivPredicate.SHOW)) {
                        continue;
                    }
                    Map<String, Long> tableEntry = getDataSizeOfTable(table, singleReplica);
                    oneEntry.putAll(tableEntry);
                }
            } else {
                Table table = ((Database) db).getTableNullable(tableName);
                if (table == null) {
                    return oneEntry;
                }
                Map<String, Long> tableEntry = getDataSizeOfTable(table, singleReplica);
                oneEntry.putAll(tableEntry);
            }
        } finally {
            db.readUnlock();
        }
        return oneEntry;
    }

    private Map<String, Long> getDataSizeOfTable(Table table, boolean singleReplica) {
        Map<String, Long> oneEntry = Maps.newHashMap();
        if (table.getType() == TableType.VIEW || table.getType() == TableType.ODBC) {
            oneEntry.put(table.getName(), 0L);
        } else if (table.isManagedTable()) {
            table.readLock();
            try {
                long tableSize = ((OlapTable) table).getDataSize(singleReplica);
                oneEntry.put(table.getName(), tableSize);
            } finally {
                table.readUnlock();
            }
        }
        return oneEntry;
    }

    private Map<String, Long> getDataSize() {
        Map<String, Long> result = new HashMap<String, Long>();
        List<String> dbNames = Env.getCurrentInternalCatalog().getDbNames();

        for (String dbName : dbNames) {
            Env.getCurrentInternalCatalog().getDb(dbName).ifPresent(db -> {
                long totalSize = getDataSizeOfDatabase(db);
                result.put(dbName, totalSize);
            });
        } // end for dbs
        return result;
    }
}
