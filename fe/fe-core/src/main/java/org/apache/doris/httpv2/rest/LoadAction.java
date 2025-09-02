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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.load.loadv2.IngestionLoadJob;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.transaction.BeginTransactionException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class LoadAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    public static final String HEADER_REDIRECT_POLICY = "redirect-policy";

    public static final String REDIRECT_POLICY_PUBLIC_PRIVATE = "public-private";
    public static final String REDIRECT_POLICY_RANDOM_BE = "random-be";
    public static final String REDIRECT_POLICY_DIRECT = "direct";
    public static final String REDIRECT_POLICY_PUBLIC = "public";
    public static final String REDIRECT_POLICY_PRIVATE = "private";

    private ExecuteEnv execEnv = ExecuteEnv.getInstance();

    private int lastSelectedBackendIndex = 0;

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_load", method = RequestMethod.PUT)
    public Object load(HttpServletRequest request, HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
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
        LOG.info("streamload action, db: {}, tbl: {}, headers: {}", db, table, getAllHeaders(request));
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
                            String msg = "insert table " + table + GroupCommitPlanner.SCHEMA_CHANGE;
                            return new RestBaseResult(msg);
                        }
                    } catch (Exception e) {
                        LOG.info("exception:" + e);
                        return new RestBaseResult(e.getMessage());
                    }
                }
            }
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
                            String msg = "insert table " + pair[1] + GroupCommitPlanner.SCHEMA_CHANGE;
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
        LOG.info("streamload action 2PC, db: {}, headers: {}", db, getAllHeaders(request));
        executeCheckPassword(request, response);
        return executeStreamLoad2PC(request, db);
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load_2pc", method = RequestMethod.PUT)
    public Object streamLoad2PC_table(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db,
            @PathVariable(value = TABLE_KEY) String table) {
        LOG.info("streamload action 2PC, db: {}, tbl: {}, headers: {}", db, table, getAllHeaders(request));
        executeCheckPassword(request, response);
        return executeStreamLoad2PC(request, db);
    }

    // Same as Multi load, to be compatible with http v1's response body,
    // we return error by using RestBaseResult.
    private Object executeWithoutPassword(HttpServletRequest request,
            HttpServletResponse response, String db, String table, boolean isStreamLoad, boolean groupCommit) {
        String label = null;
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

            label = isStreamLoad ? request.getHeader(LABEL_KEY) : request.getParameter(LABEL_KEY);
            if (!isStreamLoad && Strings.isNullOrEmpty(label)) {
                // for stream load, the label can be generated by system automatically
                return new RestBaseResult("No label selected.");
            }

            // check auth
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);

            TNetworkAddress redirectAddr;
            if (!isStreamLoad && !Strings.isNullOrEmpty(request.getParameter(SUB_LABEL_NAME_PARAM))) {
                return new RestBaseResult("Multi load is longer supported");
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
                // Handle stream load with potential group commit forwarding
                redirectAddr = handleStreamLoadRedirect(request, groupCommit, tableId, dbName, tableName, label);
            }

            if (LOG.isDebugEnabled()) {
                LOG.info("redirect load action to destination={}, stream: {}, db: {}, tbl: {}, label: {}",
                        redirectAddr.toString(), isStreamLoad, dbName, tableName, label);
            }

            RedirectView redirectView = redirectTo(request, redirectAddr);
            return redirectView;
        } catch (StreamLoadForwardException e) {
            // Special handling for stream load forwarding
            return e.getRedirectView();
        } catch (Exception e) {
            LOG.warn("load failed, stream: {}, db: {}, tbl: {}, label: {}, err: {}",
                    isStreamLoad, db, table, label, e.getMessage());
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

    private String getCloudClusterName(HttpServletRequest request) {
        String cloudClusterName = request.getHeader(SessionVariable.CLOUD_CLUSTER);
        if (!Strings.isNullOrEmpty(cloudClusterName)) {
            return cloudClusterName;
        }

        try {
            cloudClusterName = ConnectContext.get().getCloudCluster();
        } catch (ComputeGroupException e) {
            LOG.warn("get cloud cluster name failed", e);
            return "";
        }
        if (!Strings.isNullOrEmpty(cloudClusterName)) {
            return cloudClusterName;
        }

        return "";
    }

    private TNetworkAddress selectRedirectBackend(HttpServletRequest request, boolean groupCommit, long tableId)
            throws LoadException {
        return selectRedirectBackend(request, groupCommit, tableId, null);
    }

    private TNetworkAddress selectRedirectBackend(HttpServletRequest request, boolean groupCommit, long tableId,
            Backend preSelectedBackend) throws LoadException {
        long debugBackendId = DebugPointUtil.getDebugParamOrDefault("LoadAction.selectRedirectBackend.backendId", -1L);
        if (debugBackendId != -1L) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(debugBackendId);
            return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        }
        if (Config.isCloudMode()) {
            String cloudClusterName = getCloudClusterName(request);
            if (Strings.isNullOrEmpty(cloudClusterName)) {
                throw new LoadException("No cloud cluster name selected.");
            }
            return selectCloudRedirectBackend(cloudClusterName, request, groupCommit, tableId, preSelectedBackend);
        } else {
            if (groupCommit && tableId == -1) {
                throw new LoadException("Group commit table id wrong.");
            }
            return selectLocalRedirectBackend(groupCommit, request, tableId, preSelectedBackend);
        }
    }

    private TNetworkAddress selectLocalRedirectBackend(boolean groupCommit, HttpServletRequest request, long tableId,
            Backend preSelectedBackend) throws LoadException {
        Backend backend = null;
        BeSelectionPolicy policy = null;
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            throw new LoadException("ConnectContext should not be null");
        }
        ComputeGroup computeGroup = ctx.getComputeGroupSafely();
        policy = new BeSelectionPolicy.Builder().setEnableRoundRobin(true).needLoadAvailable().build();
        policy.nextRoundRobinIndex = getLastSelectedBackendIndexAndUpdate();
        List<Long> backendIds;
        int number = groupCommit ? -1 : 1;
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, number, computeGroup.getBackendList());
        if (backendIds.isEmpty()) {
            throw new LoadException(
                    SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy + ", compute group is "
                            + computeGroup.toString());
        }
        if (groupCommit) {
            // Use pre-selected backend if provided to avoid duplicate calls
            backend = preSelectedBackend != null ? preSelectedBackend
                    : selectBackendForGroupCommit("", request, tableId);
        } else {
            backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        }
        if (backend == null) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return selectEndpointByRedirectPolicy(request, backend);
    }

    private TNetworkAddress selectCloudRedirectBackend(String clusterName, HttpServletRequest req, boolean groupCommit,
            long tableId, Backend preSelectedBackend) throws LoadException {
        Backend backend = null;
        if (groupCommit) {
            // Use pre-selected backend if provided to avoid duplicate calls
            backend = preSelectedBackend != null ? preSelectedBackend
                    : selectBackendForGroupCommit(clusterName, req, tableId);
        } else {
            backend = StreamLoadHandler.selectBackend(clusterName);
        }
        return selectEndpointByRedirectPolicy(req, backend);
    }

    /**
     * Selects the endpoint address based on the redirect policy specified in the request header.
     * The available redirect policies are:
     * - DIRECT: Redirects to the backend's host.
     * - PUBLIC: Redirects to the public endpoint of the backend.
     * - PRIVATE: Redirects to the private endpoint of the backend.
     * - PUBLIC_PRIVATE: Redirects based on the host IP or domain. If the  host is a site-local
     *     address, redirects to the private endpoint. Otherwise, redirects to the public endpoint.
     * - DEFAULT: If request host equals to backend's public endpoint, redirects to the public endpoint.
     *     If private endpoint of backend is set, redirects to the private endpoint. Otherwise, redirects
     *     to the backend's host.
     *
     * @param req The HTTP request object.
     * @param backend The backend to redirect to.
     * @return The selected endpoint address.
     * @throws LoadException If there is an error in the redirect policy or endpoint selection.
     */
    private TNetworkAddress selectEndpointByRedirectPolicy(HttpServletRequest req, Backend backend)
            throws LoadException {
        Pair<String, Integer> publicHostPort = null;
        Pair<String, Integer> privateHostPort = null;
        try {
            if (!Strings.isNullOrEmpty(backend.getPublicEndpoint())) {
                publicHostPort = splitHostAndPort(backend.getPublicEndpoint());
            }
        } catch (AnalysisException e) {
            throw new LoadException(e.getMessage());
        }

        try {
            if (!Strings.isNullOrEmpty(backend.getPrivateEndpoint())) {
                privateHostPort = splitHostAndPort(backend.getPrivateEndpoint());
            }
        } catch (AnalysisException e) {
            throw new LoadException(e.getMessage());
        }

        String redirectPolicy = req.getHeader(LoadAction.HEADER_REDIRECT_POLICY);
        redirectPolicy = redirectPolicy == null || redirectPolicy.isEmpty()
                ? Config.streamload_redirect_policy : redirectPolicy;

        String reqHostStr = req.getHeader(HttpHeaderNames.HOST.toString());
        reqHostStr = reqHostStr.replaceAll("\\s+", "");
        if (reqHostStr.isEmpty()) {
            LOG.info("Invalid header host: {}", reqHostStr);
            throw new LoadException("Invalid header host: " + reqHostStr);
        }

        String reqHost = "";
        String[] pair = reqHostStr.split(":");
        if (pair.length == 1) {
            reqHost = pair[0];
        } else if (pair.length == 2) {
            reqHost = pair[0];
        } else {
            LOG.info("Invalid header host: {}", reqHostStr);
            throw new LoadException("Invalid header host: " + reqHost);
        }

        // User specified redirect policy
        if (redirectPolicy != null && (redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_DIRECT)
                || redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_RANDOM_BE))) {
            return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        } else if (redirectPolicy != null && redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_PUBLIC)) {
            if (publicHostPort != null) {
                return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
            }
            throw new LoadException("public endpoint is null, please check be public endpoint config");
        } else if (redirectPolicy != null && redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_PRIVATE)) {
            if (privateHostPort != null) {
                return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
            }
            throw new LoadException("private endpoint is null, please check be private endpoint config");
        } else if (redirectPolicy != null && redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_PUBLIC_PRIVATE)) {
            // redirect with ip
            if (InetAddressValidator.getInstance().isValid(reqHost)) {
                InetAddress addr;
                try {
                    addr = InetAddress.getByName(reqHost);
                } catch (Exception e) {
                    LOG.warn("unknown host expection: {}", e.getMessage());
                    throw new LoadException(e.getMessage());
                }
                if (addr.isSiteLocalAddress() && privateHostPort != null) {
                    return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
                } else if (publicHostPort != null) {
                    return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
                } else {
                    LOG.warn("Invalid ip or wrong cluster, host: {}, public endpoint: {}, private endpoint: {}",
                            reqHostStr, publicHostPort, privateHostPort);
                    throw new LoadException("Invalid header host: " + reqHost);
                }
            }

            // redirect with domain
            if (publicHostPort != null && reqHost.toLowerCase().contains("public")) {
                return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
            } else if (privateHostPort != null) {
                return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
            } else {
                LOG.warn("Invalid host or wrong cluster, host: {}, public endpoint: {}, private endpoint: {}",
                        reqHostStr, publicHostPort, privateHostPort);
                throw new LoadException("Invalid header host: " + reqHost);
            }
        } else {
            if (InetAddressValidator.getInstance().isValid(reqHost)
                    && publicHostPort != null && reqHost.equalsIgnoreCase(publicHostPort.first)) {
                return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
            } else if (privateHostPort != null) {
                return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
            } else {
                return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
            }
        }
    }

    private Pair<String, Integer> splitHostAndPort(String hostPort) throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            LOG.info("empty endpoint");
            throw new AnalysisException("empty endpoint: " + hostPort);
        }

        String[] pair = hostPort.split(":");
        if (pair.length != 2) {
            LOG.info("Invalid endpoint: {}", hostPort);
            throw new AnalysisException("Invalid endpoint: " + hostPort);
        }

        int port = Integer.parseInt(pair[1]);
        if (port <= 0 || port >= 65536) {
            LOG.info("Invalid endpoint port: {}", pair[1]);
            throw new AnalysisException("Invalid endpoint port: " + pair[1]);
        }

        return Pair.of(pair[0], port);
    }

    // NOTE: This function can only be used for AuditlogPlugin stream load for now.
    // AuditlogPlugin should be re-disigned carefully, and blow method focuses on
    // temporarily addressing the users' needs for audit logs.
    // So this function is not widely tested under general scenario
    private boolean checkClusterToken(String token) {
        try {
            return Env.getCurrentEnv().getTokenManager().checkAuthToken(token);
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
            // cloud need
            ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
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

    private String getAllHeaders(HttpServletRequest request) {
        StringBuilder headers = new StringBuilder();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            String headerValue = request.getHeader(headerName);
            headers.append(headerName).append(":").append(headerValue).append(", ");
        }
        return headers.toString();
    }

    private Backend selectBackendForGroupCommit(String clusterName, HttpServletRequest req, long tableId)
            throws LoadException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        ctx.setRemoteIP(req.getRemoteAddr());
        // We set this variable to fulfill required field 'user' in
        // TMasterOpRequest(FrontendService.thrift)
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.setThreadLocalInfo();
        if (Config.isCloudMode()) {
            ctx.setCloudCluster(clusterName);
        }

        Backend backend = null;
        try {
            backend = Env.getCurrentEnv().getGroupCommitManager()
                    .selectBackendForGroupCommit(tableId, ctx);
        } catch (DdlException e) {
            throw new LoadException(e.getMessage(), e);
        }
        return backend;
    }

    /**
     * Request body example:
     * {
     *     "label": "test",
     *     "tableToPartition": {
     *         "tbl_test_spark_load": ["p1","p2"]
     *     },
     *     "properties": {
     *         "strict_mode": "true",
     *         "timeout": 3600000
     *     }
     * }
     *
     */
    @RequestMapping(path = "/api/ingestion_load/{" + CATALOG_KEY + "}/{" + DB_KEY
            + "}/_create", method = RequestMethod.POST)
    public Object createIngestionLoad(HttpServletRequest request, HttpServletResponse response,
                                  @PathVariable(value = CATALOG_KEY) String catalog,
                                  @PathVariable(value = DB_KEY) String db) {
        executeCheckPassword(request, response);

        if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalog)) {
            return ResponseEntityBuilder.okWithCommonError("Only support internal catalog. "
                    + "Current catalog is " + catalog);
        }

        Object redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
        }

        String fullDbName = getFullDbName(db);

        Map<String, Object> resultMap = new HashMap<>();

        try {

            String body = HttpUtils.getBody(request);
            JsonMapper mapper = JsonMapper.builder().build();
            JsonNode jsonNode = mapper.reader().readTree(body);

            String label = jsonNode.get("label").asText();
            Map<String, List<String>> tableToPartition = mapper.reader()
                    .readValue(jsonNode.get("tableToPartition").traverse(),
                            new TypeReference<Map<String, List<String>>>() {
                            });
            List<String> tableNames = new LinkedList<>(tableToPartition.keySet());
            for (String tableName : tableNames) {
                checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);
            }

            Map<String, String> properties = new HashMap<>();
            if (jsonNode.hasNonNull("properties")) {
                properties = mapper.readValue(jsonNode.get("properties").traverse(),
                        new TypeReference<HashMap<String, String>>() {
                        });
            }

            executeCreateAndStartIngestionLoad(fullDbName, label, tableNames, properties, tableToPartition, resultMap,
                    ConnectContext.get().getCurrentUserIdentity());

        } catch (Exception e) {
            LOG.warn("create ingestion load job failed, db: {}, err: {}", db, e.getMessage());
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        return ResponseEntityBuilder.ok(resultMap);

    }

    private void executeCreateAndStartIngestionLoad(String dbName, String label, List<String> tableNames,
                                                Map<String, String> properties,
                                                Map<String, List<String>> tableToPartition,
                                                Map<String, Object> resultMap, UserIdentity userInfo)
            throws DdlException, BeginTransactionException, MetaNotFoundException, AnalysisException,
            QuotaExceedException, LoadException {

        long loadId = -1;
        try {

            LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
            loadId = loadManager.createIngestionLoadJob(dbName, label, tableNames, properties, userInfo);
            IngestionLoadJob loadJob = (IngestionLoadJob) loadManager.getLoadJob(loadId);
            resultMap.put("loadId", loadId);

            long txnId = loadJob.beginTransaction();
            resultMap.put("txnId", txnId);

            Map<String, Object> loadMeta = loadJob.getLoadMeta(tableToPartition);
            resultMap.put("dbId", loadMeta.get("dbId"));
            resultMap.put("signature", loadMeta.get("signature"));
            resultMap.put("tableMeta", loadMeta.get("tableMeta"));

            loadJob.startEtlJob();

        } catch (DdlException | BeginTransactionException | MetaNotFoundException | AnalysisException
                 | QuotaExceedException | LoadException e) {
            LOG.warn("create ingestion load job failed, db: {}, load id: {}, err: {}", dbName, loadId, e.getMessage());
            if (loadId != -1L) {
                try {
                    Env.getCurrentEnv().getLoadManager().getLoadJob(loadId).cancelJob(
                            new FailMsg(FailMsg.CancelType.UNKNOWN, StringUtils.defaultIfBlank(e.getMessage(), "")));
                } catch (DdlException ex) {
                    LOG.warn("cancel ingestion load failed, db: {}, load id: {}, err: {}", dbName, loadId,
                            e.getMessage());
                }
            }
            throw e;
        }

    }

    /**
     * Request body example:
     * {
     *     "statusInfo": {
     *         "msg": "",
     *         "hadoopProperties": "{\"fs.defaultFS\":\"hdfs://hadoop01:8020\",\"hadoop.username\":\"hadoop\"}",
     *         "appId": "local-1723088141438",
     *         "filePathToSize": "{\"hdfs://hadoop01:8020/spark-load/jobs/25054/test/36019/dpp_result.json\":179,
     *         \"hdfs://hadoop01:8020/spark-load/jobs/25054/test/36019/load_meta.json\":3441,\"hdfs://hadoop01:8020
     *         /spark-load/jobs/25054/test/36019/V1.test.25056.29373.25057.0.366242211.parquet\":5745}",
     *         "dppResult": "{\"isSuccess\":true,\"failedReason\":\"\",\"scannedRows\":10,\"fileNumber\":1,
     *         \"fileSize\":2441,\"normalRows\":10,\"abnormalRows\":0,\"unselectRows\":0,\"partialAbnormalRows\":\"[]\",
     *         \"scannedBytes\":0}",
     *         "status": "SUCCESS"
     *     },
     *     "loadId": 36018
     * }
     *
     */
    @RequestMapping(path = "/api/ingestion_load/{" + CATALOG_KEY + "}/{" + DB_KEY
            + "}/_update", method = RequestMethod.POST)
    public Object updateIngestionLoad(HttpServletRequest request, HttpServletResponse response,
                                      @PathVariable(value = CATALOG_KEY) String catalog,
                                      @PathVariable(value = DB_KEY) String db) {
        executeCheckPassword(request, response);

        if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalog)) {
            return ResponseEntityBuilder.okWithCommonError("Only support internal catalog. "
                    + "Current catalog is " + catalog);
        }

        Object redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
        }

        String fullDbName = getFullDbName(db);

        long loadId = -1;
        try {

            String body = HttpUtils.getBody(request);
            JsonMapper mapper = JsonMapper.builder().build();
            JsonNode jsonNode = mapper.readTree(body);
            LoadJob loadJob = null;

            if (jsonNode.hasNonNull("loadId")) {
                loadId = jsonNode.get("loadId").asLong();
                loadJob = Env.getCurrentEnv().getLoadManager().getLoadJob(loadId);
            }

            if (loadJob == null) {
                return ResponseEntityBuilder.okWithCommonError("load job not exists, load id: " + loadId);
            }

            IngestionLoadJob ingestionLoadJob = (IngestionLoadJob) loadJob;
            Set<String> tableNames = ingestionLoadJob.getTableNames();
            for (String tableName : tableNames) {
                checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);
            }
            Map<String, String> statusInfo = mapper.readValue(jsonNode.get("statusInfo").traverse(),
                    new TypeReference<HashMap<String, String>>() {
                    });
            ingestionLoadJob.updateJobStatus(statusInfo);
        } catch (IOException | MetaNotFoundException | UnauthorizedException e) {
            LOG.warn("cancel ingestion load job failed, db: {}, load id: {}, err: {}", db, loadId, e.getMessage());
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        return ResponseEntityBuilder.ok();

    }

    /*
     * Create redirect URL for stream load forward mode.
     *
     * This method constructs the special redirect URL used in the group commit forwarding mechanism:
     *
     * Key modifications to the standard redirect:
     * 1. Path modification: Changes "/_stream_load" to "/_stream_load_forward"
     *    - This tells the receiving BE that it needs to perform additional forwarding
     *    - The "_stream_load_forward" endpoint is specifically designed to handle forwarding logic
     *
     * 2. Forward target parameter: Adds "forward_to=host:port" to the query string
     *    - Specifies the actual target BE node that should process this request
     *    - Ensures all requests for the same table reach the same BE for optimal batching
     *
     * 3. Authentication preservation: Maintains user authentication in the URL if present
     *    - Ensures the forwarded request has proper authentication context
     *
     * Example transformation:
     * Original: http://endpoint:port/api/db/table/_stream_load?param=value
     * Forward:  http://endpoint:port/api/db/table/_stream_load_forward?param=value&forward_to=target_be:port
     *
     * @param request the original HTTP request
     * @param addr the endpoint address to redirect to (public/private endpoint)
     * @param forwardTarget the target BE node in "host:port" format for final processing
     * @return RedirectView configured for stream load forwarding
     */
    private RedirectView redirectToStreamLoadForward(HttpServletRequest request, TNetworkAddress addr,
            String forwardTarget) {
        URI urlObj = null;
        URI resultUriObj = null;
        String urlStr = request.getRequestURI();
        String userInfo = null;
        String modifiedPath = null;

        if (!Strings.isNullOrEmpty(request.getHeader("Authorization"))) {
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            userInfo = ClusterNamespace.getNameFromFullName(authInfo.fullUserName)
                    + ":" + authInfo.password;
        }
        try {
            urlObj = new URI(urlStr);
            // Replace _stream_load with _stream_load_forward in the path
            modifiedPath = urlObj.getPath().replace("/_stream_load", "/_stream_load_forward");
            resultUriObj = new URI("http", userInfo, addr.getHostname(),
                    addr.getPort(), modifiedPath, "", null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String redirectUrl = resultUriObj.toASCIIString();

        // Add forward_to parameter (note: toASCIIString() already includes '?' due to empty query)
        String queryString = request.getQueryString();
        if (!Strings.isNullOrEmpty(queryString)) {
            redirectUrl += queryString + "&forward_to=" + forwardTarget;
        } else {
            redirectUrl += "forward_to=" + forwardTarget;
        }

        LOG.info("Redirect stream load forward url: {}, forward_to: {}",
                "http://" + addr.getHostname() + ":" + addr.getPort() + modifiedPath, forwardTarget);
        RedirectView redirectView = new RedirectView(redirectUrl);
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    /**
     * Handle stream load redirect with optional group commit forwarding.
     *
     * Group Commit Stream Load Forward Mode in Cloud Environment:
     *
     * Problem:
     * Group commit requires that requests for the same table be sent to the same BE node
     * to achieve better batching efficiency. However, in cloud mode with Load Balancer (LB),
     * the LB randomly selects a BE node for forwarding, which breaks the group commit strategy
     * and reduces batching effectiveness.
     *
     * Solution:
     * Implement a two-stage forwarding mechanism:
     * 1. FE redirects to public/private endpoint (LB) as usual
     * 2. BE performs a second forwarding to the actual target BE node that handles the specific table
     *
     * This ensures that all requests for the same table ultimately reach the same BE node,
     * preserving the group commit batching strategy while still utilizing the LB infrastructure.
     *
     * @param request the HTTP request
     * @param groupCommit whether group commit is enabled
     * @param tableId the table ID for group commit
     * @param dbName database name for logging
     * @param tableName table name for logging
     * @param label label for logging
     * @return redirect address for normal redirect
     * @throws StreamLoadForwardException if forward redirect is applied
     * @throws LoadException if redirect selection fails
     */
    private TNetworkAddress handleStreamLoadRedirect(HttpServletRequest request, boolean groupCommit,
            long tableId, String dbName, String tableName, String label) throws LoadException {

        // Check if group commit forwarding is needed
        if (!Config.isCloudMode() || !groupCommit || !Config.enable_group_commit_streamload_be_forward) {
            return selectRedirectBackend(request, groupCommit, tableId);
        }

        String cloudClusterName = getCloudClusterName(request);
        if (Strings.isNullOrEmpty(cloudClusterName)) {
            throw new LoadException("No cloud cluster name selected for group commit forwarding.");
        }

        // Get target backend for group commit
        Backend targetBackend = selectBackendForGroupCommit(cloudClusterName, request, tableId);
        if (targetBackend == null) {
            throw new LoadException("Failed to select target backend for group commit forwarding.");
        }

        // Get redirect address with optimized backend selection
        TNetworkAddress redirectAddr = selectCloudRedirectBackend(cloudClusterName, request, groupCommit, tableId,
                targetBackend);
        TNetworkAddress targetAddr = new TNetworkAddress(targetBackend.getHost(), targetBackend.getHttpPort());

        // Apply forwarding if addresses differ (compare hostname and port directly)
        if (!redirectAddr.getHostname().equals(targetAddr.getHostname())
                || redirectAddr.getPort() != targetAddr.getPort()) {
            // Apply stream load forwarding by throwing StreamLoadForwardException with RedirectView
            String forwardTarget = targetAddr.getHostname() + ":" + targetAddr.getPort();
            RedirectView forwardRedirectView = redirectToStreamLoadForward(request, redirectAddr, forwardTarget);

            LOG.info("Using stream load forward mode for cloud group commit - "
                    + "db: {}, tbl: {}, label: {}, endpoint: {}, forward_to: {}, reason: redirect_differs_from_target",
                    dbName, tableName, label, redirectAddr.toString(), forwardTarget);

            throw new StreamLoadForwardException(forwardRedirectView);
        } else {
            LOG.debug("Skip stream load forward - redirect address matches target backend: {}",
                    redirectAddr.toString());
            return redirectAddr;
        }
    }

    /**
     * Special exception to carry RedirectView for stream load forwarding.
     */
    private static class StreamLoadForwardException extends RuntimeException {
        private final RedirectView redirectView;

        public StreamLoadForwardException(RedirectView redirectView) {
            this.redirectView = redirectView;
        }

        public RedirectView getRedirectView() {
            return redirectView;
        }
    }
}
