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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class LoadAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    private ExecuteEnv execEnv = ExecuteEnv.getInstance();

    private int lastSelectedBackendIndex = 0;

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_load", method = RequestMethod.PUT)
    public Object load(HttpServletRequest request, HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        if (Config.disable_mini_load) {
            ResponseEntity entity = ResponseEntityBuilder.notFound("The mini load operation has been"
                    + " disabled by default, if you need to add disable_mini_load=false in fe.conf.");
            return entity;
        } else {
            executeCheckPassword(request, response);
            return executeWithoutPassword(request, response, db, table, false, false);
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load", method = RequestMethod.PUT)
    public Object streamLoad(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
        boolean groupCommit = false;
        String groupCommitStr = request.getHeader("group_commit");
        if (groupCommitStr != null) {
            if (!groupCommitStr.equalsIgnoreCase("async_mode") && !groupCommitStr.equalsIgnoreCase("sync_mode")
                    && !groupCommitStr.equalsIgnoreCase("off_mode")) {
                return new RestBaseResult("Header `group_commit` can only be `sync_mode`, `async_mode` or `off_mode`.");
            }
            if (!groupCommitStr.equalsIgnoreCase("off_mode")) {
                groupCommit = true;
                if (groupCommitStr.equalsIgnoreCase("async_mode")) {
                    try {
                        if (isGroupCommitBlock(db, table)) {
                            String msg = "insert table " + table + " is blocked on schema change";
                            return new RestBaseResult(msg);
                        }
                    } catch (Exception e) {
                        LOG.info("exception:" + e);
                        return new RestBaseResult(e.getMessage());
                    }
                }
            }
        }
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        String authToken = request.getHeader("token");
        // if auth token is not null, check it first
        if (!Strings.isNullOrEmpty(authToken)) {
            if (!checkClusterToken(authToken)) {
                throw new UnauthorizedException("Invalid token: " + authToken);
            }
            return executeWithClusterToken(request, db, table, true);
        } else {
            try {
                executeCheckPassword(request, response);
                return executeWithoutPassword(request, response, db, table, true, groupCommit);
            } finally {
                ConnectContext.remove();
            }
        }
    }

    @RequestMapping(path = "/api/_http_stream", method = RequestMethod.PUT)
    public Object streamLoadWithSql(HttpServletRequest request, HttpServletResponse response) {
        String sql = request.getHeader("sql");
        LOG.info("streaming load sql={}", sql);
        boolean groupCommit = false;
        long tableId = -1;
        String groupCommitStr = request.getHeader("group_commit");
        if (groupCommitStr != null) {
            if (!groupCommitStr.equalsIgnoreCase("async_mode") && !groupCommitStr.equalsIgnoreCase("sync_mode")
                    && !groupCommitStr.equalsIgnoreCase("off_mode")) {
                return new RestBaseResult("Header `group_commit` can only be `sync_mode`, `async_mode` or `off_mode`.");
            }
            if (!groupCommitStr.equalsIgnoreCase("off_mode")) {
                try {
                    groupCommit = true;
                    String[] pair = parseDbAndTb(sql);
                    Database db = Env.getCurrentInternalCatalog()
                            .getDbOrException(pair[0], s -> new TException("database is invalid for dbName: " + s));
                    Table tbl = db.getTableOrException(pair[1], s -> new TException("table is invalid: " + s));
                    tableId = tbl.getId();

                    // async mode needs to write WAL, we need to block load during waiting WAL.
                    if (groupCommitStr.equalsIgnoreCase("async_mode")) {
                        if (isGroupCommitBlock(pair[0], pair[1])) {
                            String msg = "insert table " + pair[1] + " is blocked on schema change";
                            return new RestBaseResult(msg);
                        }

                    }
                } catch (Exception e) {
                    LOG.info("exception:" + e);
                    return new RestBaseResult(e.getMessage());
                }
            }
        }
        executeCheckPassword(request, response);
        try {
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            String label = request.getHeader(LABEL_KEY);
            TNetworkAddress redirectAddr = selectRedirectBackend(request, groupCommit, tableId);

            LOG.info("redirect load action to destination={}, label: {}",
                    redirectAddr.toString(), label);

            RedirectView redirectView = redirectTo(request, redirectAddr);
            return redirectView;
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    private boolean isGroupCommitBlock(String db, String table) throws TException {
        String fullDbName = getFullDbName(db);
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(fullDbName, s -> new TException("database is invalid for dbName: " + s));
        Table tblObj = dbObj.getTableOrException(table, s -> new TException("table is invalid: " + s));
        return Env.getCurrentEnv().getGroupCommitManager().isBlock(tblObj.getId());
    }

    private String[] parseDbAndTb(String sql) throws Exception {
        String[] array = sql.split(" ");
        String tmp = null;
        int count = 0;
        for (String s : array) {
            if (!s.equals("")) {
                count++;
                if (count == 3) {
                    tmp = s;
                    break;
                }
            }
        }
        if (tmp == null) {
            throw new Exception("parse db and tb with wrong sql:" + sql);
        }
        String pairStr = null;
        if (tmp.contains("(")) {
            pairStr = tmp.split("\\(")[0];
        } else {
            pairStr = tmp;
        }
        String[] pair = pairStr.split("\\.");
        if (pair.length != 2) {
            throw new Exception("parse db and tb with wrong sql:" + sql);
        }
        return pair;
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_stream_load_2pc", method = RequestMethod.PUT)
    public Object streamLoad2PC(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        executeCheckPassword(request, response);
        return executeStreamLoad2PC(request, db);
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load_2pc", method = RequestMethod.PUT)
    public Object streamLoad2PC_table(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db,
            @PathVariable(value = TABLE_KEY) String table) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        executeCheckPassword(request, response);
        return executeStreamLoad2PC(request, db);
    }

    // Same as Multi load, to be compatible with http v1's response body,
    // we return error by using RestBaseResult.
    private Object executeWithoutPassword(HttpServletRequest request,
            HttpServletResponse response, String db, String table, boolean isStreamLoad, boolean groupCommit) {
        try {
            String dbName = db;
            String tableName = table;
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(tableName)) {
                return new RestBaseResult("No table selected.");
            }

            String fullDbName = dbName;

            String label = request.getParameter(LABEL_KEY);
            if (isStreamLoad) {
                label = request.getHeader(LABEL_KEY);
            }

            if (!isStreamLoad && Strings.isNullOrEmpty(label)) {
                // for stream load, the label can be generated by system automatically
                return new RestBaseResult("No label selected.");
            }

            // check auth
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);

            TNetworkAddress redirectAddr;
            if (!isStreamLoad && !Strings.isNullOrEmpty(request.getParameter(SUB_LABEL_NAME_PARAM))) {
                // only multi mini load need to redirect to Master, because only Master has the info of table to
                // the Backend which the file exists.
                Object redirectView = redirectToMaster(request, response);
                if (redirectView != null) {
                    return redirectView;
                }
                try {
                    redirectAddr = execEnv.getMultiLoadMgr().redirectAddr(fullDbName, label);
                } catch (DdlException e) {
                    return new RestBaseResult(e.getMessage());
                }
            } else {
                long tableId = -1;
                if (groupCommit) {
                    Optional<?> database = Env.getCurrentEnv().getCurrentCatalog().getDb(dbName);
                    if (!database.isPresent()) {
                        return new RestBaseResult("Database not found.");
                    }

                    Optional<?> olapTable = ((Database) database.get()).getTable(tableName);
                    if (!olapTable.isPresent()) {
                        return new RestBaseResult("OlapTable not found.");
                    }

                    tableId = ((OlapTable) olapTable.get()).getId();
                }
                redirectAddr = selectRedirectBackend(request, groupCommit, tableId);
            }

            LOG.info("redirect load action to destination={}, stream: {}, db: {}, tbl: {}, label: {}",
                    redirectAddr.toString(), isStreamLoad, dbName, tableName, label);

            RedirectView redirectView = redirectTo(request, redirectAddr);
            return redirectView;
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    private Object executeStreamLoad2PC(HttpServletRequest request, String db) {
        try {
            String dbName = db;

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(request.getHeader(TXN_ID_KEY))
                    && Strings.isNullOrEmpty(request.getHeader(LABEL_KEY))) {
                return new RestBaseResult("No transaction id or label selected.");
            }

            String txnOperation = request.getHeader(TXN_OPERATION_KEY);
            if (Strings.isNullOrEmpty(txnOperation)) {
                return new RestBaseResult("No transaction operation(\'commit\' or \'abort\') selected.");
            }

            TNetworkAddress redirectAddr = selectRedirectBackend(request, false, -1);
            LOG.info("redirect stream load 2PC action to destination={}, db: {}, txn: {}, operation: {}",
                    redirectAddr.toString(), dbName, request.getHeader(TXN_ID_KEY), txnOperation);

            RedirectView redirectView = redirectTo(request, redirectAddr);
            return redirectView;

        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    private final synchronized int getLastSelectedBackendIndexAndUpdate() {
        int index = lastSelectedBackendIndex;
        lastSelectedBackendIndex = (index >= Integer.MAX_VALUE - 1) ? 0 : index + 1;
        return index;
    }

    private TNetworkAddress selectRedirectBackend(HttpServletRequest request, boolean groupCommit, long tableId)
            throws LoadException {
        long debugBackendId = DebugPointUtil.getDebugParamOrDefault("LoadAction.selectRedirectBackend.backendId", -1L);
        if (debugBackendId != -1L) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(debugBackendId);
            return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        }
        if (groupCommit && tableId == -1) {
            throw new LoadException("Group commit table id wrong.");
        }
        return selectLocalRedirectBackend(groupCommit, request, tableId);
    }

    private TNetworkAddress selectLocalRedirectBackend(boolean groupCommit, HttpServletRequest request, long tableId)
            throws LoadException {
        Backend backend = null;
        BeSelectionPolicy policy = null;
        String qualifiedUser = ConnectContext.get().getQualifiedUser();
        Set<Tag> userTags = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
        boolean allowResourceTagDowngrade = Env.getCurrentEnv().getAuth().isAllowResourceTagDowngrade(qualifiedUser);
        policy = new BeSelectionPolicy.Builder()
                .addTags(userTags)
                .setAllowResourceTagDowngrade(allowResourceTagDowngrade)
                .setEnableRoundRobin(true)
                .needLoadAvailable().build();
        policy.nextRoundRobinIndex = getLastSelectedBackendIndexAndUpdate();
        List<Long> backendIds;
        if (groupCommit) {
            backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, -1);
        } else {
            backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        }
        if (backendIds.isEmpty()) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        if (groupCommit) {
            backend = selectBackendForGroupCommit(request, tableId);
        } else {
            backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        }
        if (backend == null) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
    }

    // NOTE: This function can only be used for AuditlogPlugin stream load for now.
    // AuditlogPlugin should be re-disigned carefully, and blow method focuses on
    // temporarily addressing the users' needs for audit logs.
    // So this function is not widely tested under general scenario
    private boolean checkClusterToken(String token) {
        try {
            return Env.getCurrentEnv().getLoadManager().getTokenManager().checkAuthToken(token);
        } catch (UserException e) {
            throw new UnauthorizedException(e.getMessage());
        }
    }

    // NOTE: This function can only be used for AuditlogPlugin stream load for now.
    // AuditlogPlugin should be re-disigned carefully, and blow method focuses on
    // temporarily addressing the users' needs for audit logs.
    // So this function is not widely tested under general scenario
    private Object executeWithClusterToken(HttpServletRequest request, String db,
            String table, boolean isStreamLoad) {
        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            ctx.setRemoteIP(request.getRemoteAddr());
            // set user to ADMIN_USER, so that we can get the proper resource tag
            ctx.setQualifiedUser(Auth.ADMIN_USER);
            ctx.setThreadLocalInfo();

            String dbName = db;
            String tableName = table;
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(tableName)) {
                return new RestBaseResult("No table selected.");
            }

            String label = request.getParameter(LABEL_KEY);
            if (isStreamLoad) {
                label = request.getHeader(LABEL_KEY);
            }

            if (!isStreamLoad && Strings.isNullOrEmpty(label)) {
                // for stream load, the label can be generated by system automatically
                return new RestBaseResult("No label selected.");
            }

            TNetworkAddress redirectAddr = selectRedirectBackend(request, false, -1);

            LOG.info("Redirect load action with auth token to destination={},"
                            + "stream: {}, db: {}, tbl: {}, label: {}",
                    redirectAddr.toString(), isStreamLoad, dbName, tableName, label);

            URI urlObj = null;
            URI resultUriObj = null;
            String urlStr = request.getRequestURI();
            String userInfo = null;

            try {
                urlObj = new URI(urlStr);
                resultUriObj = new URI("http", userInfo, redirectAddr.getHostname(),
                        redirectAddr.getPort(), urlObj.getPath(), "", null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            String redirectUrl = resultUriObj.toASCIIString();
            if (!Strings.isNullOrEmpty(request.getQueryString())) {
                redirectUrl += request.getQueryString();
            }
            LOG.info("Redirect url: {}", "http://" + redirectAddr.getHostname() + ":"
                    + redirectAddr.getPort() + urlObj.getPath());
            RedirectView redirectView = new RedirectView(redirectUrl);
            redirectView.setContentType("text/html;charset=utf-8");
            redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);

            return redirectView;
        } catch (Exception e) {
            LOG.warn("Failed to execute stream load with cluster token, {}", e.getMessage(), e);
            return new RestBaseResult(e.getMessage());
        } finally {
            ConnectContext.remove();
        }
    }

    private Backend selectBackendForGroupCommit(HttpServletRequest req, long tableId)
            throws LoadException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        ctx.setRemoteIP(req.getRemoteAddr());
        // We set this variable to fulfill required field 'user' in
        // TMasterOpRequest(FrontendService.thrift)
        ctx.setQualifiedUser(Auth.ADMIN_USER);
        ctx.setThreadLocalInfo();

        Backend backend = null;
        try {
            backend = Env.getCurrentEnv().getGroupCommitManager()
                    .selectBackendForGroupCommit(tableId, ctx);
        } catch (DdlException e) {
            throw new LoadException(e.getMessage(), e);
        }
        return backend;
    }
}
