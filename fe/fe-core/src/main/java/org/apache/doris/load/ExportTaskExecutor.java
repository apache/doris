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

package org.apache.doris.load;

import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Status;
import org.apache.doris.load.ExportFailMsg.CancelType;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ExportTaskExecutor implements TransientTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(ExportTaskExecutor.class);

    Optional<StatementBase> selectStmt;

    ExportJob exportJob;

    Long taskId;

    private StmtExecutor stmtExecutor;

    private AtomicBoolean isCanceled;

    private AtomicBoolean isFinished;

    ExportTaskExecutor(Optional<StatementBase> selectStmt, ExportJob exportJob) {
        this.taskId = UUID.randomUUID().getMostSignificantBits();
        this.selectStmt = selectStmt;
        this.exportJob = exportJob;
        this.isCanceled = new AtomicBoolean(false);
        this.isFinished = new AtomicBoolean(false);
    }

    @Override
    public Long getId() {
        return taskId;
    }

    @Override
    public void execute() throws JobException {
        LOG.debug("[Export Task] taskId: {} starting execution", taskId);
        if (isCanceled.get()) {
            LOG.debug("[Export Task] taskId: {} was already canceled before execution", taskId);
            throw new JobException("Export executor has been canceled, task id: {}", taskId);
        }
        LOG.debug("[Export Task] taskId: {} updating state to EXPORTING", taskId);
        exportJob.updateExportJobState(ExportJobState.EXPORTING, taskId, null, null, null);
        List<OutfileInfo> outfileInfoList = Lists.newArrayList();
        if (selectStmt.isPresent()) {
            if (isCanceled.get()) {
                LOG.debug("[Export Task] taskId: {} canceled during execution", taskId);
                throw new JobException("Export executor has been canceled, task id: {}", taskId);
            }
            // check the version of tablets, skip if the consistency is in partition level.
            if (exportJob.getExportTable().isManagedTable() && !exportJob.isPartitionConsistency()) {
                LOG.debug("[Export Task] taskId: {} checking tablet versions", taskId);
                try {
                    Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException(
                            exportJob.getTableName().getDb());
                    OlapTable table = db.getOlapTableOrAnalysisException(exportJob.getTableName().getTbl());
                    LOG.debug("[Export Lock] taskId: {}, table: {} about to acquire readLock",
                            taskId, table.getName());
                    table.readLock();
                    LOG.debug("[Export Lock] taskId: {}, table: {} acquired readLock", taskId, table.getName());
                    try {
                        List<Long> tabletIds;
                        LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) selectStmt.get();
                        Optional<UnboundRelation> unboundRelation = findUnboundRelation(
                                logicalPlanAdapter.getLogicalPlan());
                        tabletIds = unboundRelation.get().getTabletIds();

                        for (Long tabletId : tabletIds) {
                            TabletMeta tabletMeta = Env.getCurrentEnv().getTabletInvertedIndex().getTabletMeta(
                                    tabletId);
                            Partition partition = table.getPartition(tabletMeta.getPartitionId());
                            long nowVersion = partition.getVisibleVersion();
                            long oldVersion = exportJob.getPartitionToVersion().get(partition.getName());
                            if (nowVersion != oldVersion) {
                                LOG.debug("[Export Lock] taskId: {}, table: {} about to release readLock"
                                        + "due to version mismatch", taskId, table.getName());
                                exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                                        CancelType.RUN_FAIL, "The version of tablet {" + tabletId + "} has changed");
                                throw new JobException("Export Job[{}]: Tablet {} has changed version, old version = {}"
                                        + ", now version = {}", exportJob.getId(), tabletId, oldVersion, nowVersion);
                            }
                        }
                    } catch (Exception e) {
                        LOG.debug("[Export Lock] taskId: {}, table: {} about to release readLock"
                                + "due to exception: {}", taskId, table.getName(), e.getMessage());
                        exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                                ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                        throw new JobException(e);
                    } finally {
                        LOG.debug("[Export Lock] taskId: {}, table: {} releasing readLock in finally block",
                                taskId, table.getName());
                        table.readUnlock();
                        LOG.debug("[Export Lock] taskId: {}, table: {} released readLock successfully",
                                taskId, table.getName());
                    }
                } catch (AnalysisException e) {
                    exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                            ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                    throw new JobException(e);
                }
            }

            try (AutoCloseConnectContext r = buildConnectContext()) {
                LOG.debug("[Export Task] taskId: {} executing", taskId);
                stmtExecutor = new StmtExecutor(r.connectContext, selectStmt.get());
                stmtExecutor.execute();
                if (r.connectContext.getState().getStateType() == MysqlStateType.ERR) {
                    LOG.debug("[Export Task] taskId: {} failed with MySQL error: {}", taskId,
                            r.connectContext.getState().getErrorMessage());
                    exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                            ExportFailMsg.CancelType.RUN_FAIL, r.connectContext.getState().getErrorMessage());
                    return;
                }
                LOG.debug("[Export Task] taskId: {} executed successfully", taskId);
                outfileInfoList = getOutFileInfo(r.connectContext.getResultAttachedInfo());
            } catch (Exception e) {
                LOG.debug("[Export Task] taskId: {} failed with exception: {}",
                        taskId, e.getMessage(), e);
                exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                        ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                throw new JobException(e);
            }
        }
        if (isCanceled.get()) {
            LOG.debug("[Export Task] taskId: {} canceled after processing all statements", taskId);
            throw new JobException("Export executor has been canceled, task id: {}", taskId);
        }
        LOG.debug("[Export Task] taskId: {} completed successfully, updating state to FINISHED", taskId);
        exportJob.updateExportJobState(ExportJobState.FINISHED, taskId, outfileInfoList, null, null);
        isFinished.getAndSet(true);
        LOG.debug("[Export Task] taskId: {} execution completed", taskId);
    }

    @Override
    public void cancel() throws JobException {
        if (isFinished.get()) {
            throw new JobException("Export executor has finished, task id: {}", taskId);
        }
        isCanceled.getAndSet(true);
        if (stmtExecutor != null) {
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "export task cancelled"));
        }
    }

    private AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        exportJob.getSessionVariables().setQueryTimeoutS(exportJob.getTimeoutSecond());
        connectContext.setSessionVariable(VariableMgr.cloneSessionVariable(exportJob.getSessionVariables()));
        // The rollback to the old optimizer is prohibited
        // Since originStmt is empty, reverting to the old optimizer when the new optimizer is enabled is meaningless.
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(exportJob.getTableName().getDb());
        connectContext.setCurrentUserIdentity(exportJob.getUserIdentity());
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        return new AutoCloseConnectContext(connectContext);
    }

    private List<OutfileInfo> getOutFileInfo(List<Map<String, String>> resultAttachedInfo) {
        List<OutfileInfo> outfileInfo = Lists.newArrayList();
        for (Map<String, String> row : resultAttachedInfo) {
            OutfileInfo outfileInfoOneRow = new OutfileInfo();
            outfileInfoOneRow.setFileNumber(row.get(OutFileClause.FILE_NUMBER));
            outfileInfoOneRow.setTotalRows(row.get(OutFileClause.TOTAL_ROWS));
            outfileInfoOneRow.setFileSize(row.get(OutFileClause.FILE_SIZE));
            outfileInfoOneRow.setUrl(row.get(OutFileClause.URL));
            outfileInfoOneRow.setWriteTime(row.get(OutFileClause.WRITE_TIME_SEC));
            outfileInfoOneRow.setWriteSpeed(row.get(OutFileClause.WRITE_SPEED_KB));
            outfileInfo.add(outfileInfoOneRow);
        }
        return outfileInfo;
    }

    private Optional<UnboundRelation> findUnboundRelation(LogicalPlan plan) {
        if (plan instanceof UnboundRelation) {
            return Optional.of((UnboundRelation) plan);
        }
        for (int i = 0; i < plan.children().size(); ++i) {
            Optional<UnboundRelation> optional = findUnboundRelation((LogicalPlan) plan.children().get(i));
            if (optional.isPresent()) {
                return optional;
            }
        }
        return Optional.empty();
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }
}
