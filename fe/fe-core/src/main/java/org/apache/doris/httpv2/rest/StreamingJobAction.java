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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.BadRequestException;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.cdc.request.TaskFailureRequest;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class StreamingJobAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(StreamingJobAction.class);

    private final TableSchemaAction tableSchemaAction;

    public StreamingJobAction(TableSchemaAction tableSchemaAction) {
        this.tableSchemaAction = tableSchemaAction;
    }

    @RequestMapping(path = "/api/streaming/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema",
            method = RequestMethod.GET)
    public Object getTableSchema(@PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName, HttpServletRequest request) {
        checkAuth(request);
        if (!Env.getCurrentEnv().isMaster()) {
            return ResponseEntityBuilder.okWithCommonError("Table schema must be queried on the master FE");
        }
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(createJobContext(request))) {
            return tableSchemaAction.getSchema(InternalCatalog.INTERNAL_CATALOG_NAME, dbName, tblName);
        }
    }

    @RequestMapping(path = "/api/streaming/commit_offset", method = RequestMethod.PUT)
    public Object commitOffset(@RequestBody CommitOffsetRequest offsetRequest, HttpServletRequest request) {
        checkAuth(request);
        return updateOffset(offsetRequest);
    }

    @RequestMapping(path = "/api/streaming/report_task_failure", method = RequestMethod.PUT)
    public Object reportTaskFailure(@RequestBody TaskFailureRequest failureRequest, HttpServletRequest request) {
        checkAuth(request);
        return failTask(failureRequest);
    }

    @RequestMapping(path = "/api/streaming/schema_change", method = RequestMethod.POST)
    public Object executeSchemaChange(@RequestBody Map<String, String> body, HttpServletRequest request) {
        checkAuth(request);
        if (!Env.getCurrentEnv().isMaster()) {
            return ResponseEntityBuilder.okWithCommonError("Schema change must be executed on the master FE");
        }
        String stmt = body.get("stmt");
        if (Strings.isNullOrEmpty(stmt)) {
            return ResponseEntityBuilder.badRequest("Missing statement request body");
        }

        ConnectContext ctx = createJobContext(request);
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(ctx)) {
            StmtExecutor executor = new StmtExecutor(ctx, stmt);
            executor.execute();
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                return ResponseEntityBuilder.okWithCommonError(ctx.getState().getErrorMessage());
            }
            return ResponseEntityBuilder.ok();
        } catch (Exception e) {
            LOG.warn("Failed to execute schema change", e);
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    private void checkAuth(HttpServletRequest request) {
        String authToken = request.getHeader("token");
        if (Strings.isNullOrEmpty(authToken)) {
            throw new UnauthorizedException("Miss token");
        }
        if (!checkClusterToken(authToken)) {
            throw new UnauthorizedException("Invalid token");
        }
    }

    // Call only after validating the internal token. The caller owns the context's scope.
    private static ConnectContext createJobContext(HttpServletRequest request) {
        String jobIdHeader = request.getHeader("jobId");
        if (Strings.isNullOrEmpty(jobIdHeader)) {
            throw new BadRequestException("Missing jobId header; CDC client must send the streaming job ID");
        }
        long jobId;
        try {
            jobId = Long.parseLong(jobIdHeader);
        } catch (NumberFormatException e) {
            throw new BadRequestException("Invalid jobId header: " + jobIdHeader);
        }
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(jobId);
        if (!(job instanceof StreamingInsertJob)) {
            throw new BadRequestException("Job " + jobId + " is not a streaming job or does not exist");
        }
        if (job.getCreateUser() == null) {
            throw new BadRequestException("Streaming job " + jobId + " has no creator identity");
        }
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setRemoteIP(request.getRemoteAddr());
        ctx.setCurrentUserIdentity(job.getCreateUser());
        if (!Strings.isNullOrEmpty(job.getCurrentDbName())) {
            ctx.setDatabase(job.getCurrentDbName());
        }
        ctx.getState().setInternal(true);
        return ctx;
    }

    private Object failTask(TaskFailureRequest failureRequest) {
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(failureRequest.getJobId());
        if (!(job instanceof StreamingInsertJob)) {
            return ResponseEntityBuilder
                    .okWithCommonError("Job " + failureRequest.getJobId() + " is not a streaming job");
        }
        try {
            LOG.info("Reporting task failure with {}", failureRequest.toString());
            ((StreamingInsertJob) job).reportTaskFailure(failureRequest);
            return ResponseEntityBuilder.ok("Task failure reported successfully");
        } catch (Exception e) {
            LOG.warn("Failed to report task failure for job {}: {}", failureRequest.getJobId(), e.getMessage());
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    private Object updateOffset(CommitOffsetRequest offsetRequest) {
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(offsetRequest.getJobId());
        if (job == null) {
            String errMsg = "Job " + offsetRequest.getJobId() + " not found";
            return ResponseEntityBuilder.okWithCommonError(errMsg);
        }
        if (!(job instanceof StreamingInsertJob)) {
            return ResponseEntityBuilder
                    .okWithCommonError("Job " + offsetRequest.getJobId() + " is not a streaming job");
        }

        StreamingInsertJob streamingJob = (StreamingInsertJob) job;
        try {
            LOG.info("Committing offset with {}", offsetRequest.toString());
            streamingJob.commitOffset(offsetRequest);
            return ResponseEntityBuilder.ok("Offset committed successfully");
        } catch (Exception e) {
            LOG.warn("Failed to commit offset for job {}, offset {}: {}", offsetRequest.getJobId(),
                    offsetRequest.getOffset(), e.getMessage());
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }
}
