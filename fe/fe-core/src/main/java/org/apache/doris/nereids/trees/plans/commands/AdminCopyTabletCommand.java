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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * ADMIN COPY TABLET 10110 PROPERTIES('version' = '1000', backend_id = '10001');
 */
public class AdminCopyTabletCommand extends ShowCommand {

    public static final String PROP_VERSION = "version";
    public static final String PROP_BACKEND_ID = "backend_id";
    public static final String PROP_EXPIRATION = "expiration_minutes";
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("TabletId")
            .add("BackendId").add("Ip").add("Path").add("ExpirationMinutes").add("CreateTableStmt").build();
    private static final long DEFAULT_EXPIRATION_MINUTES = 60;
    private static final Logger LOG = LogManager.getLogger(AdminCopyTabletCommand.class);

    private long tabletId;
    private Map<String, String> properties;
    private long version = -1;
    private long backendId = -1;
    private long expirationMinutes = DEFAULT_EXPIRATION_MINUTES;    // default 60min

    /**
     * AdminCopyTabletCommand
     */
    public AdminCopyTabletCommand(long tabletId, Map<String, String> properties) {
        super(PlanType.ADMIN_COPY_TABLET_COMMAND);
        Objects.requireNonNull(tabletId, "tabletId is null");
        Objects.requireNonNull(properties, "properties is null");
        this.tabletId = tabletId;
        this.properties = properties;

    }

    public long getTabletId() {
        return tabletId;
    }

    public long getVersion() {
        return version;
    }

    public long getBackendId() {
        return backendId;
    }

    public long getExpirationMinutes() {
        return expirationMinutes;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        return handleCopyTablet();
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (properties == null) {
            return;
        }
        try {
            Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                if (entry.getKey().equalsIgnoreCase(PROP_VERSION)) {
                    version = Long.valueOf(entry.getValue());
                    iter.remove();
                } else if (entry.getKey().equalsIgnoreCase(PROP_BACKEND_ID)) {
                    backendId = Long.valueOf(entry.getValue());
                    iter.remove();
                } else if (entry.getKey().equalsIgnoreCase(PROP_EXPIRATION)) {
                    expirationMinutes = Long.valueOf(entry.getValue());
                    expirationMinutes = Math.min(DEFAULT_EXPIRATION_MINUTES, expirationMinutes);
                    iter.remove();
                }
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid property: " + e.getMessage());
        }

        if (!properties.isEmpty()) {
            throw new AnalysisException("Unknown property: " + properties);
        }
    }

    private ShowResultSet handleCopyTablet() throws AnalysisException {
        ShowResultSet resultSet;
        long tabletId = getTabletId();
        long version = getVersion();
        long backendId = getBackendId();

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            throw new AnalysisException("Unknown tablet: " + tabletId);
        }

        // 1. find replica
        Replica replica = null;
        if (backendId != -1) {
            replica = invertedIndex.getReplica(tabletId, backendId);
        } else {
            List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
            if (!replicas.isEmpty()) {
                replica = replicas.get(0);
            }
        }
        if (replica == null) {
            throw new AnalysisException("Replica not found on backend: " + backendId);
        }
        backendId = replica.getBackendIdWithoutException();
        Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
        if (be == null || !be.isAlive()) {
            throw new AnalysisException("Unavailable backend: " + backendId);
        }

        // 2. find version
        if (version != -1 && replica.getVersion() < version) {
            throw new AnalysisException("Version is larger than replica max version: " + replica.getVersion());
        }
        version = version == -1 ? replica.getVersion() : version;

        // 3. get create table stmt
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(tabletMeta.getDbId());
        OlapTable tbl = (OlapTable) db.getTableNullable(tabletMeta.getTableId());
        if (tbl == null) {
            throw new AnalysisException("Failed to find table: " + tabletMeta.getTableId());
        }

        List<String> createTableStmt = Lists.newArrayList();
        tbl.readLock();
        try {
            Env.getDdlStmt(tbl, createTableStmt, null, null, false, true /* hide password */, version);
        } finally {
            tbl.readUnlock();
        }

        // 4. create snapshot task
        SnapshotTask task = new SnapshotTask(null, backendId, tabletId, -1, tabletMeta.getDbId(),
                tabletMeta.getTableId(), tabletMeta.getPartitionId(), tabletMeta.getIndexId(), tabletId, version, 0,
                getExpirationMinutes() * 60 * 1000, false);
        task.setIsCopyTabletTask(true);
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(1);
        countDownLatch.addMark(backendId, tabletId);
        task.setCountDownLatch(countDownLatch);

        // 5. send task and wait
        AgentBatchTask batchTask = new AgentBatchTask();
        batchTask.addTask(task);
        try {
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);

            boolean ok = false;
            try {
                ok = countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok) {
                throw new AnalysisException(
                    "Failed to make snapshot for tablet " + tabletId + " on backend: " + backendId);
            }

            // send result
            List<List<String>> resultRowSet = Lists.newArrayList();
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(tabletId));
            row.add(String.valueOf(backendId));
            row.add(be.getHost());
            row.add(task.getResultSnapshotPath());
            row.add(String.valueOf(getExpirationMinutes()));
            row.add(createTableStmt.get(0));
            resultRowSet.add(row);

            ShowResultSetMetaData showMetaData = getMetaData();
            resultSet = new ShowResultSet(showMetaData, resultRowSet);
        } finally {
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.MAKE_SNAPSHOT);
        }
        return resultSet;
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createStringType()));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCopyTabletCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
