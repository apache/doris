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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * For execute stmt or get create table stmt via http
 */
@RestController
public class StmtExecutionAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(StmtExecutionAction.class);
    private static StatementSubmitter stmtSubmitter = new StatementSubmitter();
    private static final String  NEW_LINE_PATTERN = "[\n\r]";

    private static final String NEW_LINE_REPLACEMENT = " ";

    private static final long DEFAULT_ROW_LIMIT = 1000;
    private static final long MAX_ROW_LIMIT = 10000;

    /**
     * Execute a SQL.
     * Request body:
     * {
     * "is_sync": 1,   // optional
     * "limit" : 1000  // optional
     * "stmt" : "select * from tbl1"   // required
     * }
     */
    @RequestMapping(path = "/api/query/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object executeSQL(@PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response, @RequestBody String body) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        ActionAuthorizationInfo authInfo = checkWithCookie(request, response, false);
        String fullDbName = getFullDbName(dbName);
        if (Config.enable_all_http_auth) {
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.ADMIN);
        }

        if (ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ns = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        Type type = new TypeToken<StmtRequestBody>() {
        }.getType();
        StmtRequestBody stmtRequestBody = new Gson().fromJson(body, type);

        if (Strings.isNullOrEmpty(stmtRequestBody.stmt)) {
            return ResponseEntityBuilder.badRequest("Missing statement request body");
        }
        LOG.info("stmt: {}, isSync:{}, limit: {}", stmtRequestBody.stmt, stmtRequestBody.is_sync,
                stmtRequestBody.limit);

        ConnectContext.get().changeDefaultCatalog(ns);
        ConnectContext.get().setDatabase(fullDbName);

        String streamHeader = request.getHeader("X-Doris-Stream");
        boolean isStream = !("false".equalsIgnoreCase(streamHeader));
        return executeQuery(authInfo, stmtRequestBody.is_sync, stmtRequestBody.limit, stmtRequestBody,
                            response, isStream);
    }


    /**
     * Get all create table stmt of a SQL
     *
     * @param ns
     * @param dbName
     * @param request
     * @param response
     * @param sql plain text of sql
     * @return plain text of create table stmts
     */
    @RequestMapping(path = "/api/query_schema/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object querySchema(@PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response, @RequestBody String sql) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        checkWithCookie(request, response, false);

        if (ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ns = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        if (StringUtils.isNotBlank(sql)) {
            sql = sql.replaceAll(NEW_LINE_PATTERN, NEW_LINE_REPLACEMENT);
        }
        LOG.info("sql: {}", sql);
        ConnectContext.get().changeDefaultCatalog(ns);
        ConnectContext.get().setDatabase(getFullDbName(dbName));
        return getSchema(sql);
    }

    /**
     * Execute a query
     *
     * @param authInfo
     * @param isSync
     * @param limit
     * @param stmtRequestBody
     * @return
     */
    private ResponseEntity executeQuery(ActionAuthorizationInfo authInfo, boolean isSync, long limit,
            StmtRequestBody stmtRequestBody, HttpServletResponse response, boolean isStream) {
        StatementSubmitter.StmtContext stmtCtx = new StatementSubmitter.StmtContext(stmtRequestBody.stmt,
                authInfo.fullUserName, authInfo.password, limit, isStream, response, "");
        Future<ExecutionResultSet> future = stmtSubmitter.submit(stmtCtx);

        if (isSync) {
            try {
                ExecutionResultSet resultSet = future.get();
                // if use stream response, we not need to response an object.
                if (isStream) {
                    return null;
                }
                return ResponseEntityBuilder.ok(resultSet.getResult());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("failed to execute stmt", e);
                return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
            }
        } else {
            return ResponseEntityBuilder.okWithCommonError("Not support async query execution");
        }
    }

    @NotNull
    private String getSchema(String sql) {
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase stmt = null;
        try {
            stmt = SqlParserUtils.getStmt(parser, 0);
            if (!(stmt instanceof QueryStmt)) {
                return "Only support query stmt";
            }
            Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
            QueryStmt queryStmt = (QueryStmt) stmt;
            Map<Long, TableIf> tableMap = Maps.newHashMap();
            Set<String> parentViewNameSet = Sets.newHashSet();
            queryStmt.getTables(analyzer, true, tableMap, parentViewNameSet);

            List<String> createStmts = Lists.newArrayList();
            for (TableIf tbl : tableMap.values()) {
                List<String> createTableStmts = Lists.newArrayList();
                Env.getDdlStmt(tbl, createTableStmts, null, null, false, true, -1L);
                if (!createTableStmts.isEmpty()) {
                    createStmts.add(createTableStmts.get(0));
                }
            }
            return Joiner.on("\n\n").join(createStmts);
        } catch (Exception e) {
            return "Error:" + e.getMessage();
        }
    }

    private static class StmtRequestBody {
        public Boolean is_sync = true; // CHECKSTYLE IGNORE THIS LINE
        public Long limit = DEFAULT_ROW_LIMIT;
        public String stmt;
    }
}
