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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.ExportFailMsg.CancelType;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class ExportTaskExecutor implements MemoryTaskExecutor {

    List<SelectStmt> selectStmtLists;

    ExportJob exportJob;

    @Setter
    Long taskId;

    ExportTaskExecutor(List<SelectStmt> selectStmtLists, ExportJob exportJob) {
        this.selectStmtLists = selectStmtLists;
        this.exportJob = exportJob;
    }

    @Override
    public void execute() throws JobException {
        List<OutfileInfo> outfileInfoList = Lists.newArrayList();

        for (int idx = 0; idx < selectStmtLists.size(); ++idx) {
            // check the version of tablets
            try {
                Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException(
                        exportJob.getTableName().getDb());
                OlapTable table = db.getOlapTableOrAnalysisException(exportJob.getTableName().getTbl());
                table.readLock();
                try {
                    SelectStmt selectStmt = selectStmtLists.get(idx);
                    List<Long> tabletIds = selectStmt.getTableRefs().get(0).getSampleTabletIds();
                    for (Long tabletId : tabletIds) {
                        TabletMeta tabletMeta = Env.getCurrentEnv().getTabletInvertedIndex().getTabletMeta(
                                tabletId);
                        Partition partition = table.getPartition(tabletMeta.getPartitionId());
                        long nowVersion = partition.getVisibleVersion();
                        long oldVersion = exportJob.getPartitionToVersion().get(partition.getName());
                        if (nowVersion != oldVersion) {
                            log.warn("Export Job[{}]: Tablet {} has changed version, old version = {}, "
                                            + "now version = {}", exportJob.getId(), tabletId, oldVersion, nowVersion);
                            exportJob.cancelExportTask(taskId, CancelType.RUN_FAIL,
                                    "The version of tablet {" + tabletId + "} has changed");
                            // TODO(ftw): return or throw exception?
                            return;
                        }
                    }
                } finally {
                    table.readUnlock();
                }
            } catch (AnalysisException e) {
                exportJob.cancelExportTask(taskId, ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                // TODO(ftw): return or throw exception?
                return;
            }

            try (AutoCloseConnectContext r = buildConnectContext()) {
                StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, selectStmtLists.get(idx));
                exportJob.setStmtExecutor(idx, stmtExecutor);
                stmtExecutor.execute();
                if (r.connectContext.getState().getStateType() == MysqlStateType.ERR) {
                    exportJob.cancelExportTask(taskId, ExportFailMsg.CancelType.RUN_FAIL,
                            r.connectContext.getState().getErrorMessage());
                    return;
                }
                OutfileInfo outfileInfo = getOutFileInfo(r.connectContext.getResultAttachedInfo());
                outfileInfoList.add(outfileInfo);
            } catch (Exception e) {
                exportJob.cancelExportTask(taskId, ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                // TODO(ftw): return or throw exception?
                return;
            } finally {
                exportJob.getStmtExecutor(idx).addProfileToSpan();
            }
        }

        exportJob.finishExportTask(taskId, outfileInfoList);
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
}
