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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TQueryType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Abstract insert executor.
 * The derived class should implement the abstract method for certain type of target table
 */
public abstract class AbstractInsertExecutor {
    private static final Logger LOG = LogManager.getLogger(AbstractInsertExecutor.class);
    protected long jobId;
    protected final ConnectContext ctx;
    protected final Coordinator coordinator;
    protected String labelName;
    protected final DatabaseIf database;
    protected final TableIf table;
    protected final long createTime = System.currentTimeMillis();
    protected long loadedRows = 0;
    protected int filteredRows = 0;

    protected String errMsg = "";
    protected Optional<InsertCommandContext> insertCtx;
    protected final boolean emptyInsert;

    /**
     * Constructor
     */
    public AbstractInsertExecutor(ConnectContext ctx, TableIf table, String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx, boolean emptyInsert) {
        this.ctx = ctx;
        this.coordinator = EnvFactory.getInstance().createCoordinator(ctx, null, planner, ctx.getStatsErrorEstimator());
        this.labelName = labelName;
        this.table = table;
        this.database = table.getDatabase();
        this.insertCtx = insertCtx;
        this.emptyInsert = emptyInsert;
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public DatabaseIf getDatabase() {
        return database;
    }

    public TableIf getTable() {
        return table;
    }

    public String getLabelName() {
        return labelName;
    }

    public abstract long getTxnId();

    /**
     * begin transaction if necessary
     */
    public abstract void beginTransaction();

    /**
     * finalize sink to complete enough info for sink execution
     */
    protected abstract void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink);

    /**
     * Do something before exec
     */
    protected abstract void beforeExec();

    /**
     * Do something after exec finished
     */
    protected abstract void onComplete() throws UserException;

    /**
     * Do something when exec throw exception
     */
    protected abstract void onFail(Throwable t);

    /**
     * Do something after exec
     */
    protected abstract void afterExec(StmtExecutor executor);

    protected final void execImpl(StmtExecutor executor, long jobId) throws Exception {
        String queryId = DebugUtil.printId(ctx.queryId());
        this.jobId = jobId;
        coordinator.setLoadZeroTolerance(ctx.getSessionVariable().getEnableInsertStrict());
        coordinator.setQueryType(TQueryType.LOAD);
        executor.getProfile().addExecutionProfile(coordinator.getExecutionProfile());
        QueryInfo queryInfo = new QueryInfo(ConnectContext.get(), executor.getOriginStmtInString(), coordinator);
        QeProcessorImpl.INSTANCE.registerQuery(ctx.queryId(), queryInfo);
        executor.updateProfile(false);
        coordinator.exec();
        int execTimeout = ctx.getExecTimeout();
        if (LOG.isDebugEnabled()) {
            LOG.debug("insert [{}] with query id {} execution timeout is {}", labelName, queryId, execTimeout);
        }
        boolean notTimeout = coordinator.join(execTimeout);
        if (!coordinator.isDone()) {
            coordinator.cancel();
            if (notTimeout) {
                errMsg = coordinator.getExecStatus().getErrorMsg();
                ErrorReport.reportDdlException("there exists unhealthy backend. "
                        + errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_EXECUTE_TIMEOUT);
            }
        }
        if (!coordinator.getExecStatus().ok()) {
            errMsg = coordinator.getExecStatus().getErrorMsg();
            LOG.warn("insert [{}] with query id {} failed, {}", labelName, queryId, errMsg);
            ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("insert [{}] with query id {} delta files is {}",
                    labelName, queryId, coordinator.getDeltaUrls());
        }
        if (coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
            loadedRows = Long.parseLong(coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
        }
        if (coordinator.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null) {
            filteredRows = Integer.parseInt(coordinator.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
        }
    }

    private void checkStrictModeAndFilterRatio() throws Exception {
        // if in strict mode, insert will fail if there are filtered rows
        if (ctx.getSessionVariable().getEnableInsertStrict()) {
            if (filteredRows > 0) {
                ErrorReport.reportDdlException("Insert has filtered data in strict mode",
                        ErrorCode.ERR_FAILED_WHEN_INSERT);
            }
        } else {
            if (filteredRows > ctx.getSessionVariable().getInsertMaxFilterRatio() * (filteredRows + loadedRows)) {
                ErrorReport.reportDdlException("Insert has too many filtered data %d/%d insert_max_filter_ratio is %f",
                        ErrorCode.ERR_FAILED_WHEN_INSERT, filteredRows, filteredRows + loadedRows,
                        ctx.getSessionVariable().getInsertMaxFilterRatio());
            }
        }
    }

    /**
     * execute insert txn for insert into select command.
     */
    public void executeSingleInsert(StmtExecutor executor, long jobId) throws Exception {
        beforeExec();
        try {
            executor.updateProfile(false);
            execImpl(executor, jobId);
            checkStrictModeAndFilterRatio();
            int retryTimes = 0;
            while (true) {
                try {
                    onComplete();
                    break;
                } catch (UserException e) {
                    LOG.warn("failed to commit txn, txnId={}, jobId={}, retryTimes={}",
                            getTxnId(), jobId, retryTimes, e);
                    if (e.getErrorCode() == InternalErrorCode.DELETE_BITMAP_LOCK_ERR) {
                        retryTimes++;
                        if (retryTimes >= Config.mow_insert_into_commit_retry_times) {
                            // should throw exception after running out of retry times
                            throw e;
                        }
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Throwable t) {
            onFail(t);
            // retry insert into from select when meet E-230 in cloud
            if (Config.isCloudMode() && t.getMessage().contains(FeConstants.CLOUD_RETRY_E230)) {
                throw t;
            }
            return;
        } finally {
            coordinator.close();
            executor.updateProfile(true);
            QeProcessorImpl.INSTANCE.unregisterQuery(ctx.queryId());
        }
        afterExec(executor);
    }

    public boolean isEmptyInsert() {
        return emptyInsert;
    }
}
