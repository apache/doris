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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.ProfileTreePrinter;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * Used to return query information and query profile.
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

    public static final ImmutableList<String> QUERY_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add(QUERY_ID).add(NODE).add(USER).add(DEFAULT_DB).add(SQL_STATEMENT)
            .add(QUERY_TYPE).add(START_TIME).add(END_TIME).add(TOTAL).add(QUERY_STATE)
            .build();

    private List<String> requestAllFe(String httpPath, Map<String, String> arguments, String authorization) {
        List<Pair<String, Integer>> frontends = HttpUtils.getFeList();
        ImmutableMap<String, String> header = ImmutableMap.<String, String>builder()
                .put(NodeAction.AUTHORIZATION, authorization).build();
        List<String> dataList = Lists.newArrayList();
        for (Pair<String, Integer> ipPort : frontends) {
            String url = HttpUtils.concatUrl(ipPort, httpPath, arguments);
            try {
                String data = HttpUtils.parseResponse(HttpUtils.doGet(url, header));
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
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

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

            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION));
            for (String data : dataList) {
                try {
                    NodeAction.NodeInfo nodeInfo = GsonUtils.GSON.fromJson(data,
                            new TypeToken<NodeAction.NodeInfo>() {
                            }.getType());
                    queries.addAll(nodeInfo.getRows());
                } catch (Exception e) {
                    LOG.warn("parse query info error: {}", data, e);
                }
            }
            return ResponseEntityBuilder.ok(new NodeAction.NodeInfo(QUERY_TITLE_NAMES, queries));
        }

        queries = ProfileManager.getInstance().getAllQueries().stream()
                .filter(profile -> profile.get(4).equals("Query")).collect(Collectors.toList());
        if (!Strings.isNullOrEmpty(queryId)) {
            queries = queries.stream().filter(q -> q.get(0).equals(queryId)).collect(Collectors.toList());
        }

        // add node information
        for (List<String> query : queries) {
            query.add(1, Catalog.getCurrentCatalog().getSelfNode().first + ":" + Config.http_port);
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
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, String> querySql = Maps.newHashMap();
        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/sql/" + queryId;
            ImmutableMap<String, String> arguments = ImmutableMap.<String, String>builder()
                    .put(IS_ALL_NODE_PARA, "false").build();
            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION));
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
            List<List<String>> queries = ProfileManager.getInstance().getAllQueries().stream()
                    .filter(query -> query.get(0).equals(queryId)).collect(Collectors.toList());
            if (!queries.isEmpty()) {
                querySql.put("sql", queries.get(0).get(3));
            }
        }
        return ResponseEntityBuilder.ok(querySql);
    }

    // Returns the text profile for the specified query id.
    @RequestMapping(path = "/profile/text/{query_id}", method = RequestMethod.GET)
    public Object queryProfileText(HttpServletRequest request, HttpServletResponse response,
                                   @PathVariable("query_id") String queryId,
                                   @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true")
                                           boolean isAllNode) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, String> profileMap = Maps.newHashMap();
        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/profile/text/" + queryId;
            ImmutableMap<String, String> arguments = ImmutableMap.<String, String>builder()
                    .put(IS_ALL_NODE_PARA, "false").build();
            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION));
            if (!dataList.isEmpty()) {
                try {
                    String profile =
                            JsonParser.parseString(dataList.get(0)).getAsJsonObject().get("profile").getAsString();
                    profileMap.put("profile", profile);
                    return ResponseEntityBuilder.ok(profileMap);
                } catch (Exception e) {
                    LOG.warn("parse profile text error: {}", dataList.get(0), e);
                }
            }
        } else {
            String profile = ProfileManager.getInstance().getProfile(queryId);
            if (!Strings.isNullOrEmpty(profile)) {
                profileMap.put("profile", profile);
            }
        }
        return ResponseEntityBuilder.ok(profileMap);
    }

    // Returns the fragments and instances for the specified query id.
    // [
    //		{
    //			"fragment_id":"",
    //			"time":"",
    //			"instance_id":[
    //				""
    //			]
    //		}
    // ]
    @RequestMapping(path = "/profile/fragments/{query_id}", method = RequestMethod.GET)
    public Object fragments(HttpServletRequest request, HttpServletResponse response,
                            @PathVariable("query_id") String queryId,
                            @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true")
                                    boolean isAllNode) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/profile/fragments/" + queryId;
            ImmutableMap<String, String> arguments = ImmutableMap.<String, String>builder()
                    .put(IS_ALL_NODE_PARA, "false").build();
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
                return ResponseEntityBuilder.ok(ProfileManager.getInstance().getFragmentsAndInstances(queryId));
            } catch (AnalysisException e) {
                return ResponseEntityBuilder.badRequest(e.getMessage());
            }
        }
        return ResponseEntityBuilder.badRequest("not found query id");
    }

    // Returns the graph profile for the specified query id.
    @RequestMapping(path = "/profile/graph/{query_id}", method = RequestMethod.GET)
    public Object queryProfileGraph(HttpServletRequest request, HttpServletResponse response,
                                    @PathVariable("query_id") String queryId,
                                    @RequestParam(value = FRAGMENT_ID, required = false) String fragmentId,
                                    @RequestParam(value = INSTANCE_ID, required = false) String instanceId,
                                    @RequestParam(value = IS_ALL_NODE_PARA, required = false, defaultValue = "true")
                                            boolean isAllNode) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, String> graph = Maps.newHashMap();
        List<String> results;

        if (isAllNode) {
            String httpPath = "/rest/v2/manager/query/profile/graph/" + queryId;
            Map<String, String> arguments = Maps.newHashMap();
            arguments.put(FRAGMENT_ID, fragmentId);
            arguments.put(INSTANCE_ID, instanceId);
            arguments.put(IS_ALL_NODE_PARA, "false");
            List<String> dataList = requestAllFe(httpPath, arguments, request.getHeader(NodeAction.AUTHORIZATION));
            if (!dataList.isEmpty()) {
                try {
                    String profileGraph =
                            JsonParser.parseString(dataList.get(0)).getAsJsonObject().get("graph").getAsString();
                    graph.put("graph", profileGraph);
                    return ResponseEntityBuilder.ok(graph);
                } catch (Exception e) {
                    LOG.warn("parse profile graph error: {}", dataList.get(0), e);
                }
            }
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
                LOG.warn("get profile graph error, queryId:{}, fragementId:{}, instanceId:{}",
                        queryId, fragmentId, instanceId, e);
            }
        }
        return ResponseEntityBuilder.ok(graph);
    }
}