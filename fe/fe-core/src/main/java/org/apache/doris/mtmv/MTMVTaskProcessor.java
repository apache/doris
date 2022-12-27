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

import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class MTMVTaskProcessor {
    private static final Logger LOG = LogManager.getLogger(MTMVTaskProcessor.class);
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    private ConnectContext context;

    void process(MTMVTaskContext context) throws Exception {
        String taskId = context.getTask().getTaskId();
        long jobId = context.getJob().getId();
        LOG.info("run mtmv logic start, task_id:{}, jobid:{}", taskId, jobId);
        String tableName = context.getTask().getMvName();
        String tmpTableName = genTmpTableName(tableName);
        DatabaseIf db = Env.getCurrentEnv().getCatalogMgr().getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)
                .getDbOrAnalysisException(context.getTask().getDbName());
        MaterializedView table = (MaterializedView) db.getTableOrAnalysisException(tableName);
        if (!table.tryMvTaskLock()) {
            LOG.warn("run mtmv task  failed, taskid:{}, jobid:{}, msg:{}", taskId, jobId, "get lock fail");
            return;
        }
        try {
            //step1 create tmp table
            String tmpCreateTableStmt = genCreateTempMaterializedViewStmt(context, tableName, tmpTableName);
            //check whther tmp table exists, if exists means run mtmv task failed before, so need to drop it first
            if (db.isTableExist(tmpTableName)) {
                String dropStml = genDropStml(context, tmpTableName);
                ConnectContext dropResult = execSQL(context, dropStml);
                LOG.info("exec drop table stmt, taskid:{}, stmt:{}, ret:{}, msg:{}", taskId, dropStml,
                        dropResult.getState(), dropResult.getState().getInfoMessage());
            }
            ConnectContext createTempTableResult = execSQL(context, tmpCreateTableStmt);
            LOG.info("exec tmp table stmt, taskid:{}, stmt:{}, ret:{}, msg:{}", taskId, tmpCreateTableStmt,
                    createTempTableResult.getState(), createTempTableResult.getState().getInfoMessage());
            if (createTempTableResult.getState().getStateType() != QueryState.MysqlStateType.OK) {
                throw new Throwable("create tmp table failed, sql:" + tmpCreateTableStmt);
            }

            //step2 insert data to tmp table
            String insertStmt = genInsertIntoStmt(context, tmpTableName);
            ConnectContext insertDataResult = execSQL(context, insertStmt);
            LOG.info("exec insert into stmt, taskid:{}, stmt:{}, ret:{}, msg:{}, effected_row:{}", taskId, insertStmt,
                    insertDataResult.getState(), insertDataResult.getState().getInfoMessage(),
                    insertDataResult.getState().getAffectedRows());
            if (insertDataResult.getState().getStateType() != QueryState.MysqlStateType.OK) {
                throw new Throwable("insert data failed, sql:" + insertStmt);
            }

            //step3 swap tmp table with origin table
            String swapStmt = genSwapStmt(context, tableName, tmpTableName);
            ConnectContext swapResult = execSQL(context, swapStmt);
            LOG.info("exec swap stmt, taskid:{}, stmt:{}, ret:{}, msg:{}", taskId, swapStmt, swapResult.getState(),
                    swapResult.getState().getInfoMessage());
            if (swapResult.getState().getStateType() != QueryState.MysqlStateType.OK) {
                throw new Throwable("swap table failed, sql:" + swapStmt);
            }
            //step4 update task info
            context.getTask().setMessage(insertDataResult.getState().getInfoMessage());
            context.getTask().setState(TaskState.SUCCESS);
            LOG.info("run mtmv task success, task_id:{},jobid:{}", taskId, jobId);
        } catch (AnalysisException e) {
            LOG.warn("run mtmv task failed, taskid:{}, jobid:{}, msg:{}", taskId, jobId, e.getMessage());
            context.getTask().setMessage("run task failed, caused by " + e.getMessage());
            context.getTask().setState(TaskState.FAILED);
        } catch (Throwable e) {
            LOG.warn("run mtmv task failed, taskid:{}, jobid:{}, msg:{}", taskId, jobId, e.getMessage());
            context.getTask().setMessage("run task failed, caused by " + e.getMessage());
            context.getTask().setState(TaskState.FAILED);
        } finally {
            context.getTask().setFinishTime(MTMVUtils.getNowTimeStamp());
            table.mvTaskUnLock();
            //double check
            if (db.isTableExist(tmpTableName)) {
                String dropStml = genDropStml(context, tmpTableName);
                ConnectContext dropResult = execSQL(context, dropStml);
                LOG.info("exec drop table stmt, taskid:{}, stmt:{}, ret:{}, msg:{}", taskId, dropStml,
                        dropResult.getState(), dropResult.getState().getInfoMessage());
            }
        }
    }

    private String genDropStml(MTMVTaskContext context, String tableName) {
        String stmt = "DROP MATERIALIZED VIEW  if exists " + tableName;
        LOG.info("gen drop stmt, taskid:{}, stmt:{}", context.getTask().getTaskId(), stmt);
        return stmt;
    }

    private String genTmpTableName(String tableName) {
        String tmpTableName = FeConstants.TEMP_MATERIZLIZE_DVIEW_PREFIX + tableName;
        return tmpTableName;
    }

    // ALTER TABLE t1 REPLACE WITH TABLE t1_mirror PROPERTIES('swap' = 'false');
    private String genSwapStmt(MTMVTaskContext context, String tableName, String tmpTableName) {
        String stmt = "ALTER TABLE " + tableName + " REPLACE WITH TABLE " + tmpTableName
                + " PROPERTIES('swap' = 'false');";
        LOG.info("gen swap stmt, taskid:{}, stmt:{}", context.getTask().getTaskId(), stmt);
        return stmt;
    }

    private String genInsertIntoStmt(MTMVTaskContext context, String tmpTableName) {
        String query = context.getQuery();
        String stmt = "insert into " + tmpTableName + " " + query;
        stmt = stmt.replaceAll(SystemInfoService.DEFAULT_CLUSTER + ":", "");
        LOG.info("gen insert into stmt, taskid:{}, stmt:{}", context.getTask().getTaskId(), stmt);
        return stmt;
    }

    private String genCreateTempMaterializedViewStmt(MTMVTaskContext context, String tableName, String tmpTableName) {
        try {
            String dbName = context.getTask().getDbName();
            String originViewStmt = getCreateViewStmt(dbName, tableName);
            String tmpViewStmt = convertCreateViewStmt(originViewStmt, tmpTableName);
            LOG.info("gen tmp table stmt, taskid:{}, originstml:{},  stmt:{}", context.getTask().getTaskId(),
                    originViewStmt.replaceAll("\n", " "), tmpViewStmt);
            return tmpViewStmt;
        } catch (Throwable e) {
            LOG.warn("fail to gen tmp table stmt, taskid:{}, msg:{}", context.getTask().getTaskId(), e.getMessage());
            return "";
        }
    }

    //Generate temporary view table statement
    private String convertCreateViewStmt(String stmt, String tmpTable) {
        stmt = stmt.replace("`", "");
        String regex = "CREATE MATERIALIZED VIEW.*\n";
        String replacement = "CREATE MATERIALIZED VIEW " + tmpTable + "\n";
        stmt = stmt.replaceAll(regex, replacement);
        // regex = "BUILD.*\n";
        // stmt = stmt.replaceAll(regex, " BUILD deferred never REFRESH \n");
        stmt = stmt.replaceAll("\n", " ");
        stmt = stmt.replaceAll(SystemInfoService.DEFAULT_CLUSTER + ":", "");
        return stmt;
    }

    // get origin table create stmt from env
    private String getCreateViewStmt(String dbName, String tableName) throws AnalysisException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        DatabaseIf db = ctx.getEnv().getCatalogMgr().getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)
                .getDbOrAnalysisException(dbName);
        TableIf table = db.getTableOrAnalysisException(tableName);
        table.readLock();
        try {
            List<String> createTableStmt = Lists.newArrayList();
            Env.getDdlStmt(table, createTableStmt, null, null, false, true /* hide password */, -1L);
            if (createTableStmt.isEmpty()) {
                return "";
            }
            return createTableStmt.get(0);
        } catch (Throwable e) {
            //throw new AnalysisException(e.getMessage());
        } finally {
            table.readUnlock();
        }
        return "";
    }

    private ConnectContext execSQL(MTMVTaskContext context, String originStmt) throws AnalysisException, DdlException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setThreadLocalInfo();
        String fullDbName = ClusterNamespace
                .getFullName(SystemInfoService.DEFAULT_CLUSTER, context.getTask().getDbName());
        ctx.setDatabase(fullDbName);
        ctx.setQualifiedUser("root");
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("root", "%"));
        ctx.getState().reset();

        List<StatementBase> stmts = null;
        StatementBase parsedStmt = null;
        stmts = parse(ctx, originStmt);
        parsedStmt = stmts.get(0);
        try {
            StmtExecutor executor = new StmtExecutor(ctx, parsedStmt);
            ctx.setExecutor(executor);
            executor.execute();
        } catch (Throwable e) {
            LOG.warn("execSQL failed, taskid:{}, msg:{}, stmt:{}", context.getTask().getTaskId(), e.getMessage(),
                    originStmt);
        } finally {
            LOG.debug("execSQL succ, taskid:{}, stmt:{}", context.getTask().getTaskId(), originStmt);
        }
        return ctx;
    }

    private List<StatementBase> parse(ConnectContext ctx, String originStmt) throws AnalysisException, DdlException {
        // Parse statement with parser generated by CUP&FLEX
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        try {
            return SqlParserUtils.getMultiStmts(parser);
        } catch (Error e) {
            throw new AnalysisException("Please check your sql, we meet an error when parsing.", e);
        } catch (AnalysisException | DdlException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            LOG.debug("origin stmt: {}; Analyze error message: {}", originStmt, parser.getErrorMsg(originStmt), e);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (ArrayStoreException e) {
            throw new AnalysisException("Sql parser can't convert the result to array, please check your sql.", e);
        } catch (Exception e) {
            // TODO(lingbin): we catch 'Exception' to prevent unexpected error,
            // should be removed this try-catch clause future.
            throw new AnalysisException("Internal Error, maybe syntax error or this is a bug");
        }
    }
}
