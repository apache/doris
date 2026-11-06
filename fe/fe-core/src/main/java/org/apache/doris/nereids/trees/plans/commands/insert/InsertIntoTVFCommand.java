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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystemUtil;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTVFTableSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Command for INSERT INTO tvf_name(properties) SELECT ...
 * This command is independent from InsertIntoTableCommand since TVF sink
 * has no real table, no transaction, and no table lock.
 */
public class InsertIntoTVFCommand extends Command implements ForwardWithSync, Explainable,
        NeedAuditEncryption {

    private static final Logger LOG = LogManager.getLogger(InsertIntoTVFCommand.class);

    private final LogicalPlan logicalQuery;
    private final Optional<String> labelName;
    private final Optional<LogicalPlan> cte;

    // Planning does not hold this lock. Cancellation may interrupt planning, but must
    // synchronize with the side effects and fragment dispatch before acknowledging it.
    private final Object executionLock = new Object();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private volatile Coordinator activeCoordinator;

    public InsertIntoTVFCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<LogicalPlan> cte) {
        super(PlanType.INSERT_INTO_TVF_COMMAND);
        this.logicalQuery = logicalQuery;
        this.labelName = labelName;
        this.cte = cte;
    }

    public void validateJobTarget() throws AnalysisException {
        if (!(logicalQuery instanceof UnboundTVFTableSink)
                || !"s3".equalsIgnoreCase(((UnboundTVFTableSink<?>) logicalQuery).getTvfName())) {
            throw new AnalysisException("INSERT JOB only supports S3 TVF targets; LOCAL and HDFS are not supported");
        }
    }

    /** Validate a persisted job without deleting files, registering a query, or executing it. */
    public void validateForJob(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validateJobTarget();
        preparePlan(ctx, executor);
    }

    private NereidsPlanner preparePlan(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 1. Check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)
                && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "INSERT INTO TVF requires ADMIN or LOAD privilege");
        }

        // 2. Prepare the plan
        LogicalPlan plan = logicalQuery;
        if (cte.isPresent()) {
            plan = (LogicalPlan) cte.get().withChildren(plan);
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(plan, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        executor.setPlanner(planner);
        executor.checkBlockRules();

        return planner;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        executor.registerQueryCancellationCallback(this::cancel);
        checkCanceled();
        NereidsPlanner planner = preparePlan(ctx, executor);
        checkCanceled();

        Coordinator coordinator = EnvFactory.getInstance().createCoordinator(
                ctx, planner, ctx.getStatsErrorEstimator());
        TUniqueId queryId = ctx.queryId();
        boolean registered = false;
        try {
            activeCoordinator = coordinator;
            executor.setCoord(coordinator);
            QeProcessorImpl.INSTANCE.registerQuery(queryId,
                    new QueryInfo(ctx, "INSERT INTO TVF", coordinator));
            registered = true;

            synchronized (executionLock) {
                checkCanceled();
                deleteExistingFiles(planner);
                checkCanceled();
                if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
                    ctx.getMysqlChannel().reset();
                }
                // Preserve external DML audit ownership from the resolved sink.
                executor.setExternalDmlAuditCoordinator(coordinator);
                coordinator.exec();
            }
            checkCanceled();

            // join(false) is a timeout. join(true) may also indicate a failed BE.
            boolean completed = coordinator.join(ctx.getExecTimeoutS());
            checkCanceled();
            Status executionStatus = coordinator.getExecStatus();
            if (!completed || !coordinator.isDone()) {
                String error = executionStatus.ok() ? "Insert into TVF timeout" : executionStatus.getErrorMsg();
                coordinator.cancel(new Status(TStatusCode.INTERNAL_ERROR, error));
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, error);
                return;
            }

            if (executionStatus.ok()) {
                String label = labelName.orElse(
                        String.format("tvf_insert_%x_%x", ctx.queryId().hi, ctx.queryId().lo));
                long loadedRows = 0;
                String loadedRowsStr = coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL);
                if (loadedRowsStr != null) {
                    loadedRows = Long.parseLong(loadedRowsStr);
                }
                ctx.getState().setOk(loadedRows, 0, "Insert into TVF succeeded. label: " + label);
            } else {
                String error = executionStatus.getErrorMsg();
                coordinator.cancel(executionStatus);
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, error);
            }
        } catch (Exception e) {
            String error = e.getMessage() == null ? "unknown error" : e.getMessage();
            coordinator.cancel(new Status(canceled.get() ? TStatusCode.CANCELLED : TStatusCode.INTERNAL_ERROR, error));
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, error);
        } finally {
            try {
                if (registered) {
                    QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
                }
            } finally {
                synchronized (executionLock) {
                    activeCoordinator = null;
                    executor.setCoord(null);
                }
                coordinator.close();
            }
        }
    }

    /** Called by StmtExecutor even when cancellation arrives before a Coordinator exists. */
    public void cancel() {
        canceled.set(true);
        Coordinator coordinator = activeCoordinator;
        if (coordinator != null) {
            // Wake a Coordinator that is waiting for workload admission during exec().
            coordinator.cancel(new Status(TStatusCode.CANCELLED, "Insert into TVF canceled"));
        }
        synchronized (executionLock) {
            // Dispatch may have installed new fragments after the first cancel. Do not
            // acknowledge cancellation until startup has exited and those fragments are canceled.
            coordinator = activeCoordinator;
            if (coordinator != null) {
                coordinator.cancel(new Status(TStatusCode.CANCELLED, "Insert into TVF canceled"));
            }
        }
    }

    private void checkCanceled() throws UserException {
        if (canceled.get()) {
            throw new UserException("Insert into TVF canceled");
        }
    }

    private void deleteExistingFiles(NereidsPlanner planner) throws Exception {
        // FE-side deletion of existing files (before BE execution)
        PhysicalPlan physicalPlan = planner.getPhysicalPlan();
        if (physicalPlan instanceof PhysicalTVFTableSink) {
            PhysicalTVFTableSink<?> tvfSink = (PhysicalTVFTableSink<?>) physicalPlan;
            String sinkTvfName = tvfSink.getTvfName();
            Map<String, String> sinkProps = tvfSink.getProperties();
            boolean deleteExisting = Boolean.parseBoolean(
                    sinkProps.getOrDefault("delete_existing_files", "false"));

            if (deleteExisting && !"local".equals(sinkTvfName)) {
                deleteExistingFilesInFE(sinkTvfName, sinkProps);
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this.logicalQuery;
    }

    private void deleteExistingFilesInFE(String tvfName, Map<String, String> props)
            throws Exception {
        // Copy props for building the storage adapter (exclude write-specific params)
        Map<String, String> fsCopyProps = new HashMap<>(props);
        fsCopyProps.remove("file_path");
        fsCopyProps.remove("format");
        fsCopyProps.remove("delete_existing_files");
        fsCopyProps.remove("max_file_size");
        fsCopyProps.remove("column_separator");
        fsCopyProps.remove("line_delimiter");
        fsCopyProps.remove("compression_type");
        fsCopyProps.remove("compress_type");

        StorageAdapter storageAdapter = StorageAdapter.of(fsCopyProps);
        // Concrete filesystems only accept their native schemes; normalize legacy compatibility
        // schemes (e.g. cos:// with s3.* properties) before crossing the plugin boundary.
        String filePath = storageAdapter.validateAndNormalizeUri(props.get("file_path"));
        // Extract parent directory from prefix path: s3://bucket/path/to/prefix_ -> s3://bucket/path/to/
        String parentDir = FileSystemUtil.extractParentDirectory(filePath);
        LOG.info("TVF sink: deleting existing files in directory: {}", parentDir);
        try (org.apache.doris.filesystem.FileSystem fs = FileSystemFactory.getFileSystem(storageAdapter)) {
            fs.delete(Location.of(parentDir), true);
        } catch (java.io.IOException e) {
            throw new UserException("Failed to delete existing files in " + parentDir + ": " + e.getMessage());
        }
    }
}
