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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MTMVTaskProcessor {
    private static final Logger LOG = LogManager.getLogger(MTMVTaskProcessor.class);

    boolean process(MTMVTaskContext context) throws Exception {
        String taskId = context.getTask().getTaskId();
        long jobId = context.getJob().getId();
        LOG.info("Start to run a MTMV task, taskId={}, jobId={}.", taskId, jobId);

        String mvName = context.getTask().getMVName();
        Database db = context.getCtx().getEnv().getInternalCatalog()
                .getDbOrMetaException(context.getTask().getDBName());
        MaterializedView mv = (MaterializedView) db.getTableOrAnalysisException(mvName);

        if (!mv.tryLockMVTask()) {
            LOG.warn("Failed to run the MTMV task, taskId={}, jobId={}, msg={}.", taskId, jobId,
                    "Failed to get the lock");
            context.getTask().setMessage("Failed to get the lock.");
            return false;
        }
        try {
            String insertOverwriteSelectStatement = generateInsertOverwriteSelectStmt(context, mvName);
            if (!executeSQL(context, insertOverwriteSelectStatement)) {
                throw new RuntimeException(
                        "Failed to insert overwrite, sql=" + insertOverwriteSelectStatement
                                + ", cause=" + context.getCtx().getState().getErrorMessage() + ".");
            }
            String insertInfoMessage = context.getCtx().getState().getInfoMessage();

            context.getTask().setMessage(insertInfoMessage);
            LOG.info("Run MTMV task successfully, taskId={}, jobId={}.", taskId, jobId);
            return true;
        } catch (Throwable e) {
            context.getTask().setMessage(e.getMessage());
            throw e;
        } finally {
            mv.unLockMVTask();
        }
    }

    private boolean executeSQL(MTMVTaskContext context, String sql) {
        ConnectContext ctx = context.getCtx();
        ctx.setThreadLocalInfo();
        ctx.getState().reset();
        try {
            ctx.getSessionVariable().disableNereidsPlannerOnce();
            StmtExecutor executor = new StmtExecutor(ctx, sql);
            ctx.setExecutor(executor);
            executor.execute();
        } catch (Throwable e) {
            QueryState queryState = new QueryState();
            queryState.setError(ErrorCode.ERR_INTERNAL_ERROR, e.getMessage());
            ctx.setState(queryState);
        } finally {
            ConnectContext.remove();
        }

        if (ctx.getState().getStateType() == MysqlStateType.OK) {
            LOG.info("Execute SQL successfully, taskId={}, sql={}.", context.getTask().getTaskId(), sql);
        } else {
            LOG.warn("Failed to execute SQL, taskId={}, sql={}, errorCode={}, message={}.",
                    context.getTask().getTaskId(),
                    sql, ctx.getState().getErrorCode(), ctx.getState().getErrorMessage());
        }
        return ctx.getState().getStateType() == MysqlStateType.OK;
    }

    private String generateInsertOverwriteSelectStmt(MTMVTaskContext context, String mVName) {
        return "INSERT OVERWRITE " + mVName + " " + context.getQuery();
    }
}
