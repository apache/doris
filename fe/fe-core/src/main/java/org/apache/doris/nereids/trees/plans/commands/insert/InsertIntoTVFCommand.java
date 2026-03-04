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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
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

/**
 * Command for INSERT INTO tvf_name(properties) SELECT ...
 * This command is independent from InsertIntoTableCommand since TVF sink
 * has no real table, no transaction, and no table lock.
 */
public class InsertIntoTVFCommand extends Command implements ForwardWithSync, Explainable {

    private static final Logger LOG = LogManager.getLogger(InsertIntoTVFCommand.class);

    private final LogicalPlan logicalQuery;
    private final Optional<String> labelName;
    private final Optional<LogicalPlan> cte;

    public InsertIntoTVFCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<LogicalPlan> cte) {
        super(PlanType.INSERT_INTO_TVF_COMMAND);
        this.logicalQuery = logicalQuery;
        this.labelName = labelName;
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
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

        if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }

        // 3. Create coordinator
        Coordinator coordinator = EnvFactory.getInstance().createCoordinator(
                ctx, planner, ctx.getStatsErrorEstimator());

        TUniqueId queryId = ctx.queryId();
        QeProcessorImpl.INSTANCE.registerQuery(queryId,
                new QueryInfo(ctx, "INSERT INTO TVF", coordinator));

        try {
            coordinator.exec();

            // Wait for completion
            int timeoutS = ctx.getExecTimeoutS();
            if (coordinator.join(timeoutS)) {
                if (!coordinator.isDone()) {
                    coordinator.cancel(new Status(TStatusCode.INTERNAL_ERROR, "Insert into TVF timeout"));
                    ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Insert into TVF timeout");
                    return;
                }
            }

            if (coordinator.getExecStatus().ok()) {
                String label = labelName.orElse(
                        String.format("tvf_insert_%x_%x", ctx.queryId().hi, ctx.queryId().lo));
                long loadedRows = 0;
                String loadedRowsStr = coordinator.getLoadCounters()
                        .get(LoadEtlTask.DPP_NORMAL_ALL);
                if (loadedRowsStr != null) {
                    loadedRows = Long.parseLong(loadedRowsStr);
                }
                ctx.getState().setOk(loadedRows, 0, "Insert into TVF succeeded. label: " + label);
            } else {
                String errMsg = coordinator.getExecStatus().getErrorMsg();
                LOG.warn("insert into TVF failed, error: {}", errMsg);
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, errMsg);
            }
        } catch (Exception e) {
            LOG.warn("insert into TVF failed", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getMessage() == null ? "unknown error" : e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            coordinator.close();
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this.logicalQuery;
    }

    private void deleteExistingFilesInFE(String tvfName, Map<String, String> props)
            throws Exception {
        String filePath = props.get("file_path");
        // Extract parent directory from prefix path: s3://bucket/path/to/prefix_ -> s3://bucket/path/to/
        String parentDir = extractParentDirectory(filePath);
        LOG.info("TVF sink: deleting existing files in directory: {}", parentDir);

        // Copy props for building StorageProperties (exclude write-specific params)
        Map<String, String> fsCopyProps = new HashMap<>(props);
        fsCopyProps.remove("file_path");
        fsCopyProps.remove("format");
        fsCopyProps.remove("delete_existing_files");
        fsCopyProps.remove("max_file_size");
        fsCopyProps.remove("column_separator");
        fsCopyProps.remove("line_delimiter");
        fsCopyProps.remove("compression_type");
        fsCopyProps.remove("compress_type");

        StorageProperties storageProps = StorageProperties.createPrimary(fsCopyProps);
        RemoteFileSystem fs = FileSystemFactory.get(storageProps);
        org.apache.doris.backup.Status deleteStatus = fs.deleteDirectory(parentDir);
        if (!deleteStatus.ok()) {
            throw new UserException("Failed to delete existing files in "
                    + parentDir + ": " + deleteStatus.getErrMsg());
        }
    }

    private static String extractParentDirectory(String prefixPath) {
        int lastSlash = prefixPath.lastIndexOf('/');
        if (lastSlash >= 0) {
            return prefixPath.substring(0, lastSlash + 1);
        }
        return prefixPath;
    }
}
