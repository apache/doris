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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/rest/v2/manager/logs")
public class LogQueryAction extends RestBaseController {
    /*
     * API:
     *      /rest/v2/manager/logs/query
     *      /rest/v2/manager/logs/_local_query
     *
     * The public query endpoint is ADMIN-only and returns bounded FE/BE log results
     * for diagnosis. The local query endpoint is used for FE fanout and only reads
     * the current FE node.
     */
    private static final Logger LOG = LogManager.getLogger(LogQueryAction.class);
    private static final String AUTHORIZATION = "Authorization";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String FE_LOCAL_QUERY_PATH = "/rest/v2/manager/logs/_local_query";
    private static final String BE_QUERY_PATH = "/api/diagnostics/logs/query";
    private static final int BACKEND_QUERY_TIMEOUT_MS = 10_000;

    private final LogQueryService logQueryService = new LogQueryService();

    @PostMapping("/query")
    public Object queryLogs(HttpServletRequest request, HttpServletResponse response,
            @RequestBody LogQueryService.QueryRequest requestBody) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }
        if (checkForwardToMaster(request)) {
            return forwardToMaster(request, requestBody);
        }

        LogQueryService.QueryRequest normalized;
        try {
            normalized = logQueryService.normalize(requestBody);
        } catch (IllegalArgumentException e) {
            return ResponseEntityBuilder.badRequest(e.getMessage());
        }

        LogQueryService.QueryResponse result = new LogQueryService.QueryResponse();
        result.setRequest(normalized);

        String authorization = request.getHeader(AUTHORIZATION);
        handleFrontendRequests(normalized, authorization, result);
        handleBackendRequests(normalized, authorization, result);
        return ResponseEntityBuilder.ok(result);
    }

    @PostMapping("/_local_query")
    public Object queryLocalFrontendLogs(HttpServletRequest request, HttpServletResponse response,
            @RequestBody LogQueryService.QueryRequest requestBody) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        LogQueryService.QueryRequest normalized;
        try {
            normalized = logQueryService.normalize(requestBody);
        } catch (IllegalArgumentException e) {
            return ResponseEntityBuilder.badRequest(e.getMessage());
        }
        return ResponseEntityBuilder.ok(logQueryService.queryFrontendNode(normalized,
                logQueryService.getCurrentFrontendNode()));
    }

    private void handleFrontendRequests(LogQueryService.QueryRequest requestBody, String authorization,
            LogQueryService.QueryResponse response) {
        LogQueryService.QueryRequest frontendRequest = logQueryService.filterForFrontend(requestBody);
        if (frontendRequest.getLogTypes().isEmpty()) {
            return;
        }

        List<Pair<String, Integer>> targets = getFrontendTargets(frontendRequest.getFrontendNodes());
        String currentNode = logQueryService.getCurrentFrontendNode();
        Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION, authorization);
        headers.put(CONTENT_TYPE, JSON_CONTENT_TYPE);

        for (Pair<String, Integer> target : targets) {
            String node = NetUtils.getHostPortInAccessibleFormat(target.first, target.second);
            try {
                if (node.equals(currentNode)) {
                    response.getResults().addAll(logQueryService.queryFrontendNode(frontendRequest, node).getResults());
                    continue;
                }
                String url = HttpUtils.concatUrl(target, FE_LOCAL_QUERY_PATH, ImmutableMap.of());
                String rawResponse = HttpUtils.doPost(url, headers, frontendRequest);
                String data = HttpUtils.parseResponse(rawResponse);
                LogQueryService.NodeQueryPayload payload = GsonUtils.GSON.fromJson(
                        data, LogQueryService.NodeQueryPayload.class);
                if (payload != null && payload.getResults() != null) {
                    response.getResults().addAll(payload.getResults());
                }
            } catch (Exception e) {
                LOG.warn("failed to query frontend logs from {}", node, e);
                response.getErrors().add(buildError(node, "FE", e.getMessage()));
            }
        }
    }

    private void handleBackendRequests(LogQueryService.QueryRequest requestBody, String authorization,
            LogQueryService.QueryResponse response) {
        LogQueryService.QueryRequest backendRequest = logQueryService.filterForBackend(requestBody);
        if (backendRequest.getLogTypes().isEmpty()) {
            return;
        }

        List<Pair<String, Integer>> targets = getBackendTargets(backendRequest.getBackendNodes());
        Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION, authorization);
        headers.put(CONTENT_TYPE, JSON_CONTENT_TYPE);

        for (Pair<String, Integer> target : targets) {
            String node = NetUtils.getHostPortInAccessibleFormat(target.first, target.second);
            try {
                String url = HttpUtils.concatUrl(target, BE_QUERY_PATH, ImmutableMap.of());
                String rawResponse = HttpUtils.doPost(url, headers, backendRequest, BACKEND_QUERY_TIMEOUT_MS);
                String data = HttpUtils.parseResponse(rawResponse);
                LogQueryService.NodeQueryPayload payload = GsonUtils.GSON.fromJson(
                        data, LogQueryService.NodeQueryPayload.class);
                if (payload != null && payload.getResults() != null) {
                    for (LogQueryService.NodeQueryResult result : payload.getResults()) {
                        result.setNode(node);
                        result.setNodeType("BE");
                    }
                    response.getResults().addAll(payload.getResults());
                }
            } catch (Exception e) {
                LOG.warn("failed to query backend logs from {}", node, e);
                response.getErrors().add(buildError(node, "BE", e.getMessage()));
            }
        }
    }

    private List<Pair<String, Integer>> getFrontendTargets(List<String> requestedNodes) {
        if (requestedNodes != null && !requestedNodes.isEmpty()) {
            return NodeAction.parseHostPort(requestedNodes);
        }
        return Env.getCurrentEnv().getFrontends(null).stream().filter(Frontend::isAlive)
                .map(fe -> Pair.of(fe.getHost(), Config.http_port)).collect(Collectors.toList());
    }

    private List<Pair<String, Integer>> getBackendTargets(List<String> requestedNodes) {
        if (requestedNodes != null && !requestedNodes.isEmpty()) {
            return NodeAction.parseHostPort(requestedNodes);
        }
        return Env.getCurrentSystemInfo().getAllBackendIds(true).stream().map(beId -> {
            Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
            return Pair.of(backend.getHost(), backend.getHttpPort());
        }).collect(Collectors.toList());
    }

    private LogQueryService.NodeQueryError buildError(String node, String nodeType, String message) {
        LogQueryService.NodeQueryError error = new LogQueryService.NodeQueryError();
        error.setNode(node);
        error.setNodeType(nodeType);
        error.setMessage(message);
        return error;
    }
}
