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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.net.URI;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class LoadAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    private ExecuteEnv execEnv = ExecuteEnv.getInstance();

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
            return executeWithoutPassword(request, response, db, table, false);
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load", method = RequestMethod.PUT)
    public Object streamLoad(HttpServletRequest request,
                             HttpServletResponse response,
                             @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);
        } catch (UnauthorizedException unauthorizedException) {
            LOG.info("Check password failed, going to check auth token, request: {}", request.toString());
            if (!checkClusterToken(request)) {
                throw unauthorizedException;
            } else {
                return executeWithClusterToken(request, db, table, true);
            }
        }

        return executeWithoutPassword(request, response, db, table, true);
    }

    @RequestMapping(path = "/api/_http_stream",
                        method = RequestMethod.PUT)
    public Object streamLoadWithSql(HttpServletRequest request,
                             HttpServletResponse response) {
        String sql = request.getHeader("sql");
        LOG.info("streaming load sql={}", sql);
        executeCheckPassword(request, response);
        try {
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            final String clusterName = ConnectContext.get().getClusterName();
            if (Strings.isNullOrEmpty(clusterName)) {
                return new RestBaseResult("No cluster selected.");
            }

            String label = request.getHeader(LABEL_KEY);
            TNetworkAddress redirectAddr;
            redirectAddr = selectRedirectBackend(clusterName);

            LOG.info("redirect load action to destination={}, label: {}",
                    redirectAddr.toString(), label);

            RedirectView redirectView = redirectTo(request, redirectAddr);
            return redirectView;
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
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
                                          HttpServletResponse response, String db, String table, boolean isStreamLoad) {
        try {
            String dbName = db;
            String tableName = table;
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            final String clusterName = ConnectContext.get().getClusterName();
            if (Strings.isNullOrEmpty(clusterName)) {
                return new RestBaseResult("No cluster selected.");
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(tableName)) {
                return new RestBaseResult("No table selected.");
            }

            String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

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
                redirectAddr = selectRedirectBackend(clusterName);
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

            final String clusterName = ConnectContext.get().getClusterName();
            if (Strings.isNullOrEmpty(clusterName)) {
                return new RestBaseResult("No cluster selected.");
            }

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

            TNetworkAddress redirectAddr = selectRedirectBackend(clusterName);
            LOG.info("redirect stream load 2PC action to destination={}, db: {}, txn: {}, operation: {}",
                    redirectAddr.toString(), dbName, request.getHeader(TXN_ID_KEY), txnOperation);

            RedirectView redirectView = redirectTo(request, redirectAddr);
            return redirectView;

        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    private TNetworkAddress selectRedirectBackend(String clusterName) throws LoadException {
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
        List<Long> backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (backendIds.isEmpty()) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }

        Backend backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
    }

    // NOTE: This function can only be used for AuditlogPlugin stream load for now.
    // AuditlogPlugin should be re-disigned carefully, and blow method focuses on
    // temporarily addressing the users' needs for audit logs.
    // So this function is not widely tested under general scenario
    private boolean checkClusterToken(HttpServletRequest request) {
        LOG.debug("Checking cluser token, request {}", request.toString());
        String authToken = request.getHeader("token");

        if (Strings.isNullOrEmpty(authToken)) {
            return false;
        }

        return Env.getCurrentEnv().getLoadManager().getTokenManager().checkAuthToken(authToken);
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
            ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
            ctx.setRemoteIP(request.getRemoteAddr());

            String dbName = db;
            String tableName = table;
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            final String clusterName = ConnectContext.get().getClusterName();
            if (Strings.isNullOrEmpty(clusterName)) {
                return new RestBaseResult("No cluster selected.");
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

            TNetworkAddress redirectAddr = selectRedirectBackend(clusterName);

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
            LOG.info("Redirect url: {}", redirectUrl);
            RedirectView redirectView = new RedirectView(redirectUrl);
            redirectView.setContentType("text/html;charset=utf-8");
            redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);

            return redirectView;
        } catch (Exception e) {
            LOG.warn("Failed to execute stream load with cluster token, {}", e);
            return new RestBaseResult(e.getMessage());
        }
    }
}
