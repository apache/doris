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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.Lists;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.cluster.ClusterNamespace;


public class MTMVTaskProcessor {
    private static final Logger LOG = LogManager.getLogger(MTMVTaskProcessor.class);
    private ConnectContext context;
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

    void process(MTMVTaskContext context) throws Exception{
        LOG.info("run mv logic here");
        test();
        //insertIntoSelect();
        //swapTable();
        // ALTER TABLE t_fuck REPLACE WITH TABLE t_fuck_shadow PROPERTIES('swap' = 'true');  
    }

    public void test() throws Exception{
        //LOG.info("run mv logic here test");

        //source table 
        // ConnectContext result1=execSQL("DROP MATERIALIZED VIEW  if exists multi_mv;");
        // ConnectContext result2=execSQL("CREATE MATERIALIZED VIEW  multi_mv BUILD IMMEDIATE  REFRESH COMPLETE start with \"2022-10-27 19:35:00\" next  1 second KEY(username)  DISTRIBUTED BY HASH (username)  buckets 1 PROPERTIES ('replication_num' = '1') AS select t_user.username ,t_user.id from t_user;");
        // ConnectContext result3=execSQL("insert into multi_mv select t_user.username ,t_user.id from t_user;");    

        // //temp table 
        // ConnectContext result4=execSQL("DROP MATERIALIZED VIEW  if exists multi_mv_shadow;");
        // ConnectContext result5=execSQL("CREATE MATERIALIZED VIEW  multi_mv_shadow BUILD IMMEDIATE REFRESH COMPLETE start with \"2022-10-27 19:35:00\" next  1 second KEY(username)   DISTRIBUTED BY HASH (username)  buckets 1 PROPERTIES ('replication_num' = '1') AS select t_user.username ,t_user.id from t_user;");
        // ConnectContext result6=execSQL("insert into multi_mv_shadow select t_user.username ,t_user.id from t_user;");  
        // ConnectContext result7=execSQL("insert into multi_mv_shadow select t_user.username ,t_user.id from t_user;");

        // //swap 
        // ConnectContext result8=execSQL("ALTER TABLE multi_mv REPLACE WITH TABLE multi_mv_shadow PROPERTIES('swap' = 'true');"); 



        //insertIntoSelect();
        //swapTable();
        // ALTER TABLE t_fuck REPLACE WITH TABLE t_fuck_shadow PROPERTIES('swap' = 'true');  
    }

    private ConnectContext execSQL(String originStmt)throws AnalysisException, DdlException{
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);

        String fullDbName = ClusterNamespace
                .getFullName(SystemInfoService.DEFAULT_CLUSTER, "test_db");
        ctx.setDatabase(fullDbName);
        ctx.setQualifiedUser("root");
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("root", "%"));
        ctx.getState().reset();

        List<StatementBase> stmts = null;
        StatementBase parsedStmt=null;
        stmts = parse(ctx, originStmt);
        parsedStmt = stmts.get(0);
        // Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
        // //parsedStmt.analyze(analyzer);
        try {
            StmtExecutor executor = new StmtExecutor(ctx, parsedStmt);
            ctx.setExecutor(executor);
            executor.execute(); 
        }catch (Throwable e) {
             LOG.info("execSQL,{},{}", originStmt, e);
        }
        LOG.info("execSQL:{},{},info:{},rows:{}",originStmt, ctx.getState(),ctx.getState().getInfoMessage(),ctx.getState().getAffectedRows());
        return ctx;
    }

    void swapTable() throws AnalysisException, DdlException{
        LOG.info("run mv logic here, start to swapTable");
        String originStmt = "ALTER TABLE multi_mv_10 REPLACE WITH TABLE multi_mv_10_shadow PROPERTIES('swap' = 'true')";
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);

        String fullDbName = ClusterNamespace
                .getFullName(SystemInfoService.DEFAULT_CLUSTER, "test_db");
        ctx.setDatabase(fullDbName);
        ctx.setQualifiedUser("root");
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("root", "%"));
        ctx.getState().reset();

        List<StatementBase> stmts = null;
        StatementBase parsedStmt=null;
        stmts = parse(ctx, originStmt);
        parsedStmt = stmts.get(0);
        // Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
        // //parsedStmt.analyze(analyzer);
        // StmtExecutor executor = new StmtExecutor(ctx, parsedStmt);
        // ctx.setExecutor(executor);
        // executor.execute(); 
        LOG.info("swapTable:{},info:{},rows:{}", ctx.getState(),ctx.getState().getInfoMessage(),ctx.getState().getAffectedRows());
    }

    private void  insertIntoSelect() throws AnalysisException, DdlException {
        LOG.info("run mv logic here, start to insertIntoSelect");
        String originStmt = "insert into multi_mv_10 select t_user.username ,t_user.id from t_user;";
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);

        String fullDbName = ClusterNamespace
                .getFullName(SystemInfoService.DEFAULT_CLUSTER, "test_db");
        ctx.setDatabase(fullDbName);
        ctx.setQualifiedUser("root");
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("root", "%"));
        ctx.getState().reset();

        // String originStmt = "insert into multi_mv_08 select t_user.username ,t_user.id from t_user;";
        // List<StatementBase> stmts = null;
        // StatementBase parsedStmt=null;
        // stmts = parse(ctx, originStmt);
        // parsedStmt = stmts.get(0);
        // // Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
        // // //parsedStmt.analyze(analyzer);
        // StmtExecutor executor = new StmtExecutor(ctx, parsedStmt);
        // ctx.setExecutor(executor);
        // executor.execute(); 
        LOG.info("mtmv execute state:{},info:{},rows:{}", ctx.getState(),ctx.getState().getInfoMessage(),ctx.getState().getAffectedRows());
    }

    private List<StatementBase> parse(ConnectContext ctx, String originStmt) throws AnalysisException, DdlException {
        LOG.debug("the originStmts are: {}", originStmt);
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
