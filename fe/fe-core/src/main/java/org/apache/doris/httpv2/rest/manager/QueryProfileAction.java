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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.CurrentQueryStatementsProcNode;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.ProfileTreePrinter;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.ProfileManager.ProfileElement;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * Used to return query information and query profile.
 * base: /rest/v2/manager/query
 * 1. /query_info
 * 2. /sql/{query_id}
 * 3. /profile/{format}/{query_id}
 * 4. /trace_id/{trace_id}
 * 5. /profile/fragments/{query_id}
 * 6. /current_queries
 * 7. /kill/{query_id}
 */
@RestController
@RequestMapping("/rest/v2/manager/query")
public class QueryProfileAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(QueryProfileAction.class);

    public static final String QUERY_ID = "Query ID";
    public static final String NODE = "FE节点";
    public static final String USER = "查询用户";
    public static final String DEFAULT_DB = "执行数据库";
    public static final String SQL_STATEMENT = "Sql";
    public static final String QUERY_TYPE = "查询类型";
    public static final String START_TIME = "开始时间";
    public static final String END_TIME = "结束时间";
    public static final String TOTAL = "执行时长";
    public static final String QUERY_STATE = "状态";

    private static final String QUERY_ID_PARA = "query_id";
    private static final String SEARCH_PARA = "search";
    private static final String IS_ALL_NODE_PARA = "is_all_node";
    private static final String FRAGMENT_ID = "fragment_id";
    private static final String INSTANCE_ID = "instance_id";

    private static final String FRONTEND = "Frontend";

    public static final ImmutableList<String> QUERY_TITLE_NAMES = new ImmutableList.Builder<String>().add(QUERY_ID)
            .add(NODE).add(USER).add(DEFAULT_DB).add(SQL_STATEMENT).add(QUERY_TYPE).add(START_TIME).add(END_TIME)
            .add(TOTAL).add(QUERY_STATE).build();

    private List<String> requestAllFe(String httpPath, Map<String, String> arguments, String authorization,
            HttpMethod method) {
        List<Pair<String, Integer>> frontends = HttpUtils.getFeList();
        ImmutableMap<String, String> header = ImmutableMap.<String, String>builder()
                .put(NodeAction.AUTHORIZATION, authorization).build();
        List<String> dataList = Lists.newArrayList();
        for (Pair<String, Integer> ipPort : frontends) {
            String url = HttpUtils.concatUrl(ipPort, httpPath, arguments);
            try {
                String data = null;
                if (method == HttpMethod.GET) {
                    data = HttpUtils.parseResponse(HttpUtils.doGet(url, header));
                } else if (method == HttpMethod.POST) {
                    data = HttpUtils.parseResponse(HttpUtils.doPost(url, header, null));
                }
                if (!Strings.isNullOrEmpty(data) && !data.equals("{}")) {
                    dataList.add(data);
                }
            } catch (Exception e) {
                LOG.warn("request url {} error", url, e);
            }
        }
        return dataList;
    }

    @RequestMapping(path = "/query_info", method = RequestMethod.GET)
    public Object queryInfo(HttpServletRequest request, HttpServletResponse response,
                            @RequestParam(value = QUERY_ID_PARA, required = false) String queryId,
                            @RequestParam(value = SEARCH_PARA, required = false) String search,
                            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true")
                                    boolean isAllNode) {
        executeCheckPassword(request, response);

        List<List<String>> queries = Lists.newArrayList();
        if (isAllNode) {
            // Get query information for all fe
            String httpPath = "/rest/v2/manager/query/query_info";
            Map<String, String> arguments = Maps.newHashMap();
            arguments.put(QUERY_ID_PARA, queryId);
            if (!Strings.isNullOrEmpty(search)) {
                try {
                    // search may contain special characters that need to be encoded.
                    search = URLEncoder.encode(search, StandardCharsets.UTF_8.toString());
                } catch (UnsupportedEncodingException ignored) {
                    // Encoding exception, ignore search parameter.
                }
            }
            arguments.put(SEARCH_PARA, search);
            arguments.put(IS_ALL_NODE_PARA, "false");

            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION),
                    HttpMethod.GET);
            for (String data : dataList) {
                try {
                    NodeAction.NodeInfo nodeInfo = GsonUtils.GSON.fromJson(data, new TypeToken<NodeAction.NodeInfo>() {
                    }.getType());
                    queries.addAll(nodeInfo.getRows());
                } catch (Exception e) {
                    LOG.warn("parse query info error: {}", data, e);
                }
            }
            return ResponseEntityBuilder.ok(new NodeAction.NodeInfo(QUERY_TITLE_NAMES, queries));
        }

        Stream<List<String>> queryStream = ProfileManager.getInstance().getAllQueries().stream()
                .filter(profile -> profile.get(1).equalsIgnoreCase("Query"));
        queryStream = filterQueriesByUserAndQueryId(queryStream, queryId);
        queries = queryStream.collect(Collectors.toList());

        // add node information
        for (List<String> query : queries) {
            query.add(1, NetUtils.getHostPortInAccessibleFormat(Env.getCurrentEnv().getSelfNode().getHost(),
                    Config.http_port));
        }

        if (!Strings.isNullOrEmpty(search)) {
            List<List<String>> tempQueries = Lists.newArrayList();
            for (List<String> query : queries) {
                for (String field : query) {
                    if (field.contains(search)) {
                        tempQueries.add(query);
                        break;
                    }
                }
            }
            queries = tempQueries;
        }

        return ResponseEntityBuilder.ok(new NodeAction.NodeInfo(QUERY_TITLE_NAMES, queries));
    }

    // Returns the sql for the specified query id.
    @RequestMapping(path = "/sql/{query_id}", method = RequestMethod.GET)
    public Object queryInfo(HttpServletRequest request, HttpServletResponse response,
                            @PathVariable("query_id") String queryId,
                            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true")
                                    boolean isAllNode) {
        executeCheckPassword(request, response);

        Map<String, String> querySql = Maps.newHashMap();
        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/sql/" + queryId;
            ImmutableMap<String, String> arguments = ImmutableMap.<String, String>builder()
                    .put(IS_ALL_NODE_PARA, "false").build();
            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION),
                    HttpMethod.GET);
            if (!dataList.isEmpty()) {
                try {
                    String sql = JsonParser.parseString(dataList.get(0)).getAsJsonObject().get("sql").getAsString();
                    querySql.put("sql", sql);
                    return ResponseEntityBuilder.ok(querySql);
                } catch (Exception e) {
                    LOG.warn("parse sql error: {}", dataList.get(0), e);
                }
            }
        } else {
            Stream<List<String>> queryStream = ProfileManager.getInstance().getAllQueries().stream();
            queryStream = filterQueriesByUserAndQueryId(queryStream, queryId);
            List<List<String>> queries = queryStream.collect(Collectors.toList());
            if (!queries.isEmpty()) {
                querySql.put("sql", queries.get(0).get(3));
            }
        }
        return ResponseEntityBuilder.ok(querySql);
    }

    private Stream<List<String>> filterQueriesByUserAndQueryId(Stream<List<String>> queryStream, String queryId) {
        // filter by user
        // Only admin or root user can see all profile.
        // Common user can only review the query of their own.
        String user = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
        if (!user.equalsIgnoreCase(Auth.ADMIN_USER) && !user.equalsIgnoreCase(Auth.ROOT_USER)) {
            queryStream = queryStream.filter(q -> q.get(1).equals(user));
        }
        if (!Strings.isNullOrEmpty(queryId)) {
            queryStream = queryStream.filter(query -> query.get(0).equals(queryId));
        }
        return queryStream;
    }

    /**
     * Returns the text profile for the specified query id.
     * There are 3 formats:
     * 1. Text: return the entire profile of the specified query id
     * eg: {"profile": "text_xxx"}
     * <p>
     * 2. Graph: return the profile in ascii graph. If fragmentId and instanceId are specified, it will
     * return the instance profile, otherwise, it will return the fragment profile.
     * eg: {"profile" : "graph_xxx"}
     * <p>
     * 3. Json: return the profile in json. If fragmentId and instanceId are specified, it will
     * return the instance profile, otherwise, it will return the fragment profile.
     * Json format is mainly used for front-end UI drawing.
     * eg: {"profile" : "json_xxx"}
     */
    @RequestMapping(path = "/profile/{format}/{query_id}", method = RequestMethod.GET)
    public Object queryProfileText(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("format") String format, @PathVariable("query_id") String queryId,
            @RequestParam(value = FRAGMENT_ID, required = false) String fragmentId,
            @RequestParam(value = INSTANCE_ID, required = false) String instanceId,
            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true") boolean isAllNode) {
        executeCheckPassword(request, response);

        if (!isAllNode) {
            try {
                checkAuthByUserAndQueryId(queryId);
            } catch (AuthenticationException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }
        }

        if (format.equals("text")) {
            return getTextProfile(request, queryId, isAllNode);
        } else if (format.equals("graph")) {
            return getGraphProfile(request, queryId, fragmentId, instanceId, isAllNode);
        } else if (format.equals("json")) {
            return getJsonProfile(request, queryId, fragmentId, instanceId, isAllNode);
        } else {
            return ResponseEntityBuilder.badRequest("Invalid profile format: " + format);
        }
    }

    /**
     * Get query id by trace id
     *
     * @param request
     * @param response
     * @param traceId
     * @param isAllNode
     * @return
     */
    @RequestMapping(path = "/trace_id/{trace_id}", method = RequestMethod.GET)
    public Object getQueryIdByTraceId(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("trace_id") String traceId,
            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true") boolean isAllNode) {
        executeCheckPassword(request, response);

        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/trace_id/" + traceId;
            ImmutableMap<String, String> arguments =
                    ImmutableMap.<String, String>builder().put(IS_ALL_NODE_PARA, "false").build();
            List<Pair<String, Integer>> frontends = HttpUtils.getFeList();
            ImmutableMap<String, String> header = ImmutableMap.<String, String>builder()
                    .put(NodeAction.AUTHORIZATION, request.getHeader(NodeAction.AUTHORIZATION)).build();
            for (Pair<String, Integer> ipPort : frontends) {
                String url = HttpUtils.concatUrl(ipPort, httpPath, arguments);
                try {
                    String responseJson = HttpUtils.doGet(url, header);
                    int code = JsonParser.parseString(responseJson).getAsJsonObject().get("code").getAsInt();
                    if (code == HttpUtils.REQUEST_SUCCESS_CODE) {
                        return responseJson;
                    }
                } catch (Exception e) {
                    LOG.warn(e);
                }
            }
        } else {
            ExecuteEnv env = ExecuteEnv.getInstance();
            String queryId = env.getScheduler().getQueryIdByTraceId(traceId);
            if (Strings.isNullOrEmpty(queryId)) {
                return ResponseEntityBuilder.badRequest("Not found");
            }

            try {
                checkAuthByUserAndQueryId(queryId);
            } catch (AuthenticationException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }

            return ResponseEntityBuilder.ok(queryId);
        }
        return ResponseEntityBuilder.badRequest("not found query id");
    }

    /**
     *  Query qError.
     */
    @RequestMapping(path = "/qerror/{id}", method = RequestMethod.GET)
    public ResponseEntity<String> getStats(@PathVariable(value = "id") String id) {
        ProfileElement profile = ProfileManager.getInstance().findProfileElementObject(id);
        if (profile == null) {
            return ResponseEntityBuilder.notFound(null);
        }
        StatsErrorEstimator statsErrorEstimator = profile.statsErrorEstimator;
        if (statsErrorEstimator == null) {
            return ResponseEntityBuilder.notFound(null);
        }
        return ResponseEntity.ok(GsonUtils.GSON.toJson(statsErrorEstimator));
    }

    @RequestMapping(path = "/profile/fragments/{query_id}", method = RequestMethod.GET)
    public Object fragments(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("query_id") String queryId,
            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true") boolean isAllNode) {
        executeCheckPassword(request, response);

        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/profile/fragments/" + queryId;
            ImmutableMap<String, String> arguments =
                    ImmutableMap.<String, String>builder().put(IS_ALL_NODE_PARA, "false").build();
            List<Pair<String, Integer>> frontends = HttpUtils.getFeList();
            ImmutableMap<String, String> header = ImmutableMap.<String, String>builder()
                    .put(NodeAction.AUTHORIZATION, request.getHeader(NodeAction.AUTHORIZATION)).build();
            for (Pair<String, Integer> ipPort : frontends) {
                String url = HttpUtils.concatUrl(ipPort, httpPath, arguments);
                try {
                    String responseJson = HttpUtils.doGet(url, header);
                    int code = JsonParser.parseString(responseJson).getAsJsonObject().get("code").getAsInt();
                    if (code == HttpUtils.REQUEST_SUCCESS_CODE) {
                        return responseJson;
                    }
                } catch (Exception e) {
                    LOG.warn(e);
                }
            }
        } else {
            try {
                checkAuthByUserAndQueryId(queryId);
            } catch (AuthenticationException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }

            try {
                return ResponseEntityBuilder.ok(ProfileManager.getInstance().getFragmentsAndInstances(queryId));
            } catch (AnalysisException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }
        }
        return ResponseEntityBuilder.badRequest("not found query id");
    }

    @NotNull
    private ResponseEntity getTextProfile(HttpServletRequest request, String queryId, boolean isAllNode) {
        Map<String, String> profileMap = Maps.newHashMap();
        if (isAllNode) {
            return getProfileFromAllFrontends(request, "text", queryId, "", "");
        } else {
            String profile = ProfileManager.getInstance().getProfile(queryId);
            if (!Strings.isNullOrEmpty(profile)) {
                profileMap.put("profile", profile);
            }
        }
        return ResponseEntityBuilder.ok(profileMap);
    }

    @NotNull
    private ResponseEntity getGraphProfile(HttpServletRequest request, String queryId, String fragmentId,
            String instanceId, boolean isAllNode) {
        Map<String, String> graph = Maps.newHashMap();
        List<String> results;
        if (isAllNode) {
            return getProfileFromAllFrontends(request, "graph", queryId, fragmentId, instanceId);
        } else {
            try {
                if (Strings.isNullOrEmpty(fragmentId) || Strings.isNullOrEmpty(instanceId)) {
                    ProfileTreeNode treeRoot = ProfileManager.getInstance().getFragmentProfileTree(queryId, queryId);
                    results = Lists.newArrayList(ProfileTreePrinter.printFragmentTree(treeRoot));
                } else {
                    ProfileTreeNode treeRoot = ProfileManager.getInstance().getInstanceProfileTree(queryId, queryId,
                            fragmentId, instanceId);
                    results = Lists.newArrayList(ProfileTreePrinter.printInstanceTree(treeRoot));
                }
                graph.put("graph", results.get(0));
            } catch (Exception e) {
                LOG.warn("get profile graph error, queryId:{}, fragementId:{}, instanceId:{}", queryId, fragmentId,
                        instanceId, e);
            }
        }
        return ResponseEntityBuilder.ok(graph);
    }

    @NotNull
    private ResponseEntity getJsonProfile(HttpServletRequest request, String queryId, String fragmentId,
            String instanceId, boolean isAllNode) {
        Map<String, String> graph = Maps.newHashMap();
        if (isAllNode) {
            return getProfileFromAllFrontends(request, "json", queryId, fragmentId, instanceId);
        } else {
            try {
                JSONObject json;
                if (Strings.isNullOrEmpty(fragmentId) || Strings.isNullOrEmpty(instanceId)) {
                    String brief = ProfileManager.getInstance().getProfileBrief(queryId);
                    graph.put("profile", brief);
                } else {
                    ProfileTreeNode treeRoot = ProfileManager.getInstance()
                            .getInstanceProfileTree(queryId, queryId, fragmentId, instanceId);
                    json = ProfileTreePrinter.printFragmentTreeInJson(treeRoot, ProfileTreePrinter.PrintLevel.INSTANCE);
                    graph.put("profile", json.toJSONString());
                }
            } catch (Exception e) {
                LOG.warn("get profile graph error, queryId:{}, fragementId:{}, instanceId:{}", queryId, fragmentId,
                        instanceId, e);
            }
        }
        return ResponseEntityBuilder.ok(graph);
    }

    @NotNull
    private ResponseEntity getProfileFromAllFrontends(HttpServletRequest request, String format, String queryId,
            String fragmentId, String instanceId) {
        String httpPath = "/rest/v2/manager/query/profile/" + format + "/" + queryId;
        ImmutableMap.Builder<String, String> builder =
                ImmutableMap.<String, String>builder().put(IS_ALL_NODE_PARA, "false");
        if (!Strings.isNullOrEmpty(fragmentId)) {
            builder.put(FRAGMENT_ID, fragmentId);
        }
        if (!Strings.isNullOrEmpty(instanceId)) {
            builder.put(INSTANCE_ID, instanceId);
        }
        List<String> dataList = requestAllFe(httpPath, builder.build(), request.getHeader(NodeAction.AUTHORIZATION),
                HttpMethod.GET);
        Map<String, String> result = Maps.newHashMap();
        if (!dataList.isEmpty()) {
            try {
                String key = format.equals("graph") ? "graph" : "profile";
                String profile = JsonParser.parseString(dataList.get(0)).getAsJsonObject().get(key).getAsString();
                result.put(key, profile);
            } catch (Exception e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }
        }
        return ResponseEntityBuilder.ok(result);
    }

    private void checkAuthByUserAndQueryId(String queryId) throws AuthenticationException {
        String user = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
        if (!user.equalsIgnoreCase(Auth.ADMIN_USER) && !user.equalsIgnoreCase(Auth.ROOT_USER)) {
            ProfileManager.getInstance().checkAuthByUserAndQueryId(user, queryId);
        }
    }

    /**
     * return the result of CurrentQueryStatementsProcNode.
     *
     * @param request
     * @param response
     * @param isAllNode
     * @return
     */
    @RequestMapping(path = "/current_queries", method = RequestMethod.GET)
    public Object currentQueries(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true") boolean isAllNode) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        if (isAllNode) {
            // Get current queries from all FE
            String httpPath = "/rest/v2/manager/query/current_queries";
            Map<String, String> arguments = Maps.newHashMap();
            arguments.put(IS_ALL_NODE_PARA, "false");
            List<List<String>> queries = Lists.newArrayList();
            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION),
                    HttpMethod.GET);
            for (String data : dataList) {
                try {
                    NodeAction.NodeInfo nodeInfo = GsonUtils.GSON.fromJson(data, new TypeToken<NodeAction.NodeInfo>() {
                    }.getType());
                    queries.addAll(nodeInfo.getRows());
                } catch (Exception e) {
                    LOG.warn("parse query info error: {}", data, e);
                }
            }
            List<String> titles = Lists.newArrayList(CurrentQueryStatementsProcNode.TITLE_NAMES);
            titles.add(0, FRONTEND);
            return ResponseEntityBuilder.ok(new NodeAction.NodeInfo(titles, queries));
        } else {
            try {
                CurrentQueryStatementsProcNode node = new CurrentQueryStatementsProcNode();
                ProcResult result = node.fetchResult();
                // add frontend info at first column.
                List<String> titles = Lists.newArrayList(CurrentQueryStatementsProcNode.TITLE_NAMES);
                titles.add(0, FRONTEND);
                List<List<String>> rows = result.getRows();
                String feIp = FrontendOptions.getLocalHostAddress();
                for (List<String> row : rows) {
                    row.add(0, feIp);
                }
                return ResponseEntityBuilder.ok(new NodeAction.NodeInfo(titles, rows));
            } catch (AnalysisException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }
        }
    }

    /**
     * kill queries with specific query id
     *
     * @param request
     * @param response
     * @param queryId
     * @return
     */
    @RequestMapping(path = "/kill/{query_id}", method = RequestMethod.POST)
    public Object killQuery(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("query_id") String queryId,
            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true") boolean isAllNode) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        if (isAllNode) {
            // Get current queries from all FE
            String httpPath = "/rest/v2/manager/query/kill/" + queryId;
            Map<String, String> arguments = Maps.newHashMap();
            arguments.put(IS_ALL_NODE_PARA, "false");
            requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION), HttpMethod.POST);
            return ResponseEntityBuilder.ok();
        }

        ExecuteEnv env = ExecuteEnv.getInstance();
        env.getScheduler().cancelQuery(queryId);
        return ResponseEntityBuilder.ok();
    }
}
