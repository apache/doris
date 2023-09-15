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
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.ExportFailMsg.CancelType;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ExportTaskExecutor implements TransientTaskExecutor {

    List<StatementBase> selectStmtLists;

    ExportJob exportJob;

    @Setter
    Long taskId;

    private StmtExecutor stmtExecutor;

    private AtomicBoolean isCanceled;

    private AtomicBoolean isFinished;

    ExportTaskExecutor(List<StatementBase> selectStmtLists, ExportJob exportJob) {
        this.selectStmtLists = selectStmtLists;
        this.exportJob = exportJob;
        this.isCanceled = new AtomicBoolean(false);
        this.isFinished = new AtomicBoolean(false);
    }

    @Override
    public void execute() throws JobException {
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", taskId);
        }
        exportJob.updateExportJobState(ExportJobState.EXPORTING, taskId, null, null, null);
        List<OutfileInfo> outfileInfoList = Lists.newArrayList();
        for (int idx = 0; idx < selectStmtLists.size(); ++idx) {
            if (isCanceled.get()) {
                throw new JobException("Export executor has been canceled, task id: {}", taskId);
            }
            // check the version of tablets
            if (exportJob.getExportTable().getType() == TableType.OLAP) {
                try {
                    Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException(
                            exportJob.getTableName().getDb());
                    OlapTable table = db.getOlapTableOrAnalysisException(exportJob.getTableName().getTbl());
                    table.readLock();
                    try {
                        List<Long> tabletIds;
                        if (exportJob.getSessionVariables().isEnableNereidsPlanner()) {
                            LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) selectStmtLists.get(idx);
                            Optional<UnboundRelation> unboundRelation = findUnboundRelation(
                                    logicalPlanAdapter.getLogicalPlan());
                            tabletIds = unboundRelation.get().getTabletIds();
                        } else {
                            SelectStmt selectStmt = (SelectStmt) selectStmtLists.get(idx);
                            tabletIds = selectStmt.getTableRefs().get(0).getSampleTabletIds();
                        }

                        for (Long tabletId : tabletIds) {
                            TabletMeta tabletMeta = Env.getCurrentEnv().getTabletInvertedIndex().getTabletMeta(
                                    tabletId);
                            Partition partition = table.getPartition(tabletMeta.getPartitionId());
                            long nowVersion = partition.getVisibleVersion();
                            long oldVersion = exportJob.getPartitionToVersion().get(partition.getName());
                            if (nowVersion != oldVersion) {
                                exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                                        CancelType.RUN_FAIL, "The version of tablet {" + tabletId + "} has changed");
                                throw new JobException("Export Job[{}]: Tablet {} has changed version, old version = {}"
                                        + ", now version = {}", exportJob.getId(), tabletId, oldVersion, nowVersion);
                            }
                        }
                    } catch (Exception e) {
                        exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                                ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                        throw new JobException(e);
                    } finally {
                        table.readUnlock();
                    }
                } catch (AnalysisException e) {
                    exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                            ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                    throw new JobException(e);
                }
            }

            try (AutoCloseConnectContext r = buildConnectContext()) {
                stmtExecutor = new StmtExecutor(r.connectContext, selectStmtLists.get(idx));
                stmtExecutor.execute();
                if (r.connectContext.getState().getStateType() == MysqlStateType.ERR) {
                    exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                            ExportFailMsg.CancelType.RUN_FAIL, r.connectContext.getState().getErrorMessage());
                    return;
                }
                OutfileInfo outfileInfo = getOutFileInfo(r.connectContext.getResultAttachedInfo());
                outfileInfoList.add(outfileInfo);
            } catch (Exception e) {
                exportJob.updateExportJobState(ExportJobState.CANCELLED, taskId, null,
                        ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                throw new JobException(e);
            } finally {
                stmtExecutor.addProfileToSpan();
            }
        }
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", taskId);
        }
        exportJob.updateExportJobState(ExportJobState.FINISHED, taskId, outfileInfoList, null, null);
        isFinished.getAndSet(true);
    }

    @Override
    public void cancel() throws JobException {
        if (isFinished.get()) {
            throw new JobException("Export executor has finished, task id: {}", taskId);
        }
        isCanceled.getAndSet(true);
        if (stmtExecutor != null) {
            stmtExecutor.cancel();
        }
    }

    private AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setSessionVariable(exportJob.getSessionVariables());
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(exportJob.getTableName().getDb());
        connectContext.setQualifiedUser(exportJob.getQualifiedUser());
        connectContext.setCurrentUserIdentity(exportJob.getUserIdentity());
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        return new AutoCloseConnectContext(connectContext);
    }

    private OutfileInfo getOutFileInfo(Map<String, String> resultAttachedInfo) {
        OutfileInfo outfileInfo = new OutfileInfo();
        outfileInfo.setFileNumber(resultAttachedInfo.get(OutFileClause.FILE_NUMBER));
        outfileInfo.setTotalRows(resultAttachedInfo.get(OutFileClause.TOTAL_ROWS));
        outfileInfo.setFileSize(resultAttachedInfo.get(OutFileClause.FILE_SIZE) + "bytes");
        outfileInfo.setUrl(resultAttachedInfo.get(OutFileClause.URL));
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
}
