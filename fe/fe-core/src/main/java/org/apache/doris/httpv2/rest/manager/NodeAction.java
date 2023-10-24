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
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.httpv2.rest.SetConfigAction;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * Used to return all node information, configuration information and modify node config.
 */
@RestController
@RequestMapping("/rest/v2/manager/node")
public class NodeAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(NodeAction.class);
    private static final Pattern PATTERN = Pattern.compile(":");

    public static final String AUTHORIZATION = "Authorization";
    private static final int HTTP_WAIT_TIME_SECONDS = 2;

    public static final String CONFIG = "配置项";
    public static final String NODE_IP_PORT = "节点";
    public static final String NODE_TYPE = "节点类型";
    public static final String CONFIG_TYPE = "配置值类型";
    public static final String MASTER_ONLY = "MasterOnly";
    public static final String CONFIG_VALUE = "配置值";
    public static final String IS_MUTABLE = "可修改";

    public static final ImmutableList<String> FE_CONFIG_TITLE_NAMES = new ImmutableList.Builder<String>().add(CONFIG)
            .add(NODE_IP_PORT).add(NODE_TYPE).add(CONFIG_TYPE).add(MASTER_ONLY).add(CONFIG_VALUE).add(IS_MUTABLE)
            .build();

    public static final ImmutableList<String> BE_CONFIG_TITLE_NAMES = new ImmutableList.Builder<String>().add(CONFIG)
            .add(NODE_IP_PORT).add(NODE_TYPE).add(CONFIG_TYPE).add(CONFIG_VALUE).add(IS_MUTABLE).build();

    private Object httpExecutorLock = new Object();
    private static volatile ExecutorService httpExecutor = null;

    // Returns all fe information, similar to 'show frontends'.
    @RequestMapping(path = "/frontends", method = RequestMethod.GET)
    public Object frontends_info(HttpServletRequest request, HttpServletResponse response) throws Exception {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        return fetchNodeInfo(request, response, "/frontends");
    }

    // Returns all be information, similar to 'show backends'.
    @RequestMapping(path = "/backends", method = RequestMethod.GET)
    public Object backends_info(HttpServletRequest request, HttpServletResponse response) throws Exception {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        return fetchNodeInfo(request, response, "/backends");
    }

    // Returns all broker information, similar to 'show broker'.
    @RequestMapping(path = "/brokers", method = RequestMethod.GET)
    public Object brokers_info(HttpServletRequest request, HttpServletResponse response) throws Exception {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        return fetchNodeInfo(request, response, "/brokers");
    }

    // {
    //   "column_names": [
    //     ""
    //   ],
    //   "rows": [
    //     [
    //       ""
    //     ]
    //   ]
    // }
    private Object fetchNodeInfo(HttpServletRequest request, HttpServletResponse response, String procPath)
            throws Exception {
        try {
            if (!Env.getCurrentEnv().isMaster()) {
                return redirectToMasterOrException(request, response);
            }

            ProcResult procResult = ProcService.getInstance().open(procPath).fetchResult();
            List<String> columnNames = Lists.newArrayList(procResult.getColumnNames());
            return ResponseEntityBuilder.ok(new NodeInfo(columnNames, procResult.getRows()));
        } catch (Exception e) {
            LOG.warn(e);
            throw e;
        }
    }

    @Getter
    @Setter
    public static class NodeInfo {
        public List<String> columnNames;
        public List<List<String>> rows;

        public NodeInfo(List<String> columnNames, List<List<String>> rows) {
            this.columnNames = columnNames;
            this.rows = rows;
        }
    }

    // Return fe and be all configuration names.
    // {
    //   "frontend": [
    //     ""
    //   ],
    //   "backend": [
    //     ""
    //   ]
    // }
    @RequestMapping(path = "/configuration_name", method = RequestMethod.GET)
    public Object configurationName(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, List<String>> result = Maps.newHashMap();
        try {
            result.put("frontend", Lists.newArrayList(Config.dump().keySet()));

            List<String> beConfigNames = Lists.newArrayList();
            List<Long> beIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (!beIds.isEmpty()) {
                Backend be = Env.getCurrentSystemInfo().getBackend(beIds.get(0));
                String url = "http://" + NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHttpPort())
                        + "/api/show_config";
                String questResult = HttpUtils.doGet(url, null);
                List<List<String>> configs = GsonUtils.GSON.fromJson(questResult, new TypeToken<List<List<String>>>() {
                }.getType());
                for (List<String> config : configs) {
                    beConfigNames.add(config.get(0));
                }
            }
            result.put("backend", beConfigNames);
        } catch (Exception e) {
            LOG.warn(e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
        return ResponseEntityBuilder.ok(result);
    }

    // Return all fe and be nodes.
    // {
    //   "frontend": [
    //     "host:httpPort"
    //   ],
    //   "backend": [
    //     "host:httpPort""
    //   ]
    // }
    @RequestMapping(path = "/node_list", method = RequestMethod.GET)
    public Object nodeList(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, List<String>> result = Maps.newHashMap();
        result.put("frontend", getFeList());
        result.put("backend", getBeList());
        return ResponseEntityBuilder.ok(result);
    }

    private static List<String> getFeList() {
        return Env.getCurrentEnv().getFrontends(null).stream()
                .map(fe -> NetUtils.getHostPortInAccessibleFormat(fe.getHost(), Config.http_port))
                .collect(Collectors.toList());
    }

    private static List<String> getBeList() {
        return Env.getCurrentSystemInfo().getAllBackendIds(false).stream().map(beId -> {
            Backend be = Env.getCurrentSystemInfo().getBackend(beId);
            return NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHttpPort());
        }).collect(Collectors.toList());
    }

    /*
     * this http interface is used to return configuration information requested by other fe.
     */
    @RequestMapping(path = "/config", method = RequestMethod.GET)
    public Object config(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        List<List<String>> configs = ConfigBase.getConfigInfo(null);
        // Sort all configs by config key.
        configs.sort(Comparator.comparing(o -> o.get(0)));

        // reorder the fields
        List<List<String>> results = Lists.newArrayList();
        for (List<String> config : configs) {
            List<String> list = Lists.newArrayList();
            list.add(config.get(0));
            list.add(config.get(2));
            list.add(config.get(4));
            list.add(config.get(1));
            list.add(config.get(3));
            results.add(list);
        }
        return results;
    }

    // Return the configuration information of fe or be.
    //
    // for fe:
    // {
    //   "column_names": [
    //     "配置项",
    //     "节点",
    //     "节点类型",
    //     "配置类型",
    //     "仅master",
    //     "配置值",
    //     "可修改"
    //   ],
    //   "rows": [
    //     [
    //       ""
    //     ]
    //   ]
    // }
    //
    // for be:
    // {
    //   "column_names": [
    //     "配置项",
    //     "节点",
    //     "节点类型",
    //     "配置类型",
    //     "配置值",
    //     "可修改"
    //   ],
    //   "rows": [
    //     [
    //       ""
    //     ]
    //   ]
    // }
    @RequestMapping(path = "/configuration_info", method = RequestMethod.POST)
    public Object configurationInfo(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = "type") String type,
            @RequestBody(required = false) ConfigInfoRequestBody requestBody) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        initHttpExecutor();

        if (requestBody == null) {
            requestBody = new ConfigInfoRequestBody();
        }
        List<Pair<String, Integer>> hostPorts;
        if (type.equalsIgnoreCase("fe")) {
            if (requestBody.getNodes() != null && !requestBody.getNodes().isEmpty()) {
                hostPorts = parseHostPort(requestBody.getNodes());
            } else {
                hostPorts = parseHostPort(getFeList());
            }

            List<Map.Entry<String, Integer>> errNodes = Lists.newArrayList();
            List<List<String>> data = handleConfigurationInfo(hostPorts, request.getHeader(AUTHORIZATION),
                    "/rest/v2/manager/node/config", "FE", requestBody.getConfNames(), errNodes);
            if (!errNodes.isEmpty()) {
                LOG.warn("Failed to get fe node configuration information from:{}", errNodes.toString());
            }
            return ResponseEntityBuilder.ok(new NodeInfo(FE_CONFIG_TITLE_NAMES, data));
        } else if (type.equalsIgnoreCase("be")) {
            if (requestBody.getNodes() != null && !requestBody.getNodes().isEmpty()) {
                hostPorts = parseHostPort(requestBody.getNodes());
            } else {
                hostPorts = parseHostPort(getBeList());
            }

            List<Map.Entry<String, Integer>> errNodes = Lists.newArrayList();
            List<List<String>> data = handleConfigurationInfo(hostPorts, request.getHeader(AUTHORIZATION),
                    "/api/show_config", "BE", requestBody.getConfNames(), errNodes);
            if (!errNodes.isEmpty()) {
                LOG.warn("Failed to get be node configuration information from:{}", errNodes.toString());
            }
            return ResponseEntityBuilder.ok(new NodeInfo(BE_CONFIG_TITLE_NAMES, data));
        }
        return ResponseEntityBuilder.badRequest(
                "Unsupported type: " + type + ". Only types of fe or be are " + "supported");
    }

    // Use thread pool to concurrently fetch configuration information from specified fe or be nodes.
    private List<List<String>> handleConfigurationInfo(List<Pair<String, Integer>> hostPorts, String authorization,
            String questPath, String nodeType, List<String> confNames, List<Map.Entry<String, Integer>> errNodes) {
        // The configuration information returned by each node is a List<List<String>> type,
        // configInfoTotal is used to store the configuration information of all nodes.
        List<List<List<String>>> configInfoTotal = Lists.newArrayList();
        MarkedCountDownLatch<String, Integer> configRequestDoneSignal = new MarkedCountDownLatch<>(hostPorts.size());
        for (int i = 0; i < hostPorts.size(); ++i) {
            configInfoTotal.add(Lists.newArrayList());

            Pair<String, Integer> hostPort = hostPorts.get(i);
            String address = NetUtils.getHostPortInAccessibleFormat(hostPort.first, hostPort.second);
            configRequestDoneSignal.addMark(address, -1);
            String url = "http://" + address + questPath;
            httpExecutor.submit(
                    new HttpConfigInfoTask(url, hostPort, authorization, nodeType, confNames, configRequestDoneSignal,
                            configInfoTotal.get(i)));
        }
        List<List<String>> resultConfigs = Lists.newArrayList();
        try {
            configRequestDoneSignal.await(HTTP_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
            for (List<List<String>> lists : configInfoTotal) {
                resultConfigs.addAll(lists);
            }
        } catch (InterruptedException e) {
            errNodes.addAll(configRequestDoneSignal.getLeftMarks());
        }

        return resultConfigs;
    }

    private void initHttpExecutor() {
        if (httpExecutor == null) {
            synchronized (httpExecutorLock) {
                if (httpExecutor == null) {
                    httpExecutor = ThreadPoolManager.newDaemonFixedThreadPool(5, 100, "node-config-update-pool", true);
                }
            }
        }
    }

    static List<Pair<String, Integer>> parseHostPort(List<String> nodes) {
        List<Pair<String, Integer>> hostPorts = Lists.newArrayList();
        for (String node : nodes) {
            try {
                Pair<String, Integer> ipPort = SystemInfoService.validateHostAndPort(node);
                hostPorts.add(ipPort);
            } catch (Exception e) {
                LOG.warn(e);
            }
        }
        return hostPorts;
    }

    private class HttpConfigInfoTask implements Runnable {
        private String url;
        private Pair<String, Integer> hostPort;
        private String authorization;
        private String nodeType;
        private List<String> confNames;
        private MarkedCountDownLatch<String, Integer> configRequestDoneSignal;
        private List<List<String>> config;

        public HttpConfigInfoTask(String url, Pair<String, Integer> hostPort, String authorization, String nodeType,
                List<String> confNames, MarkedCountDownLatch<String, Integer> configRequestDoneSignal,
                List<List<String>> config) {
            this.url = url;
            this.hostPort = hostPort;
            this.authorization = authorization;
            this.nodeType = nodeType;
            this.confNames = confNames;
            this.configRequestDoneSignal = configRequestDoneSignal;
            this.config = config;
        }

        @Override
        public void run() {
            String configInfo;
            try {
                configInfo = HttpUtils.doGet(url,
                        ImmutableMap.<String, String>builder().put(AUTHORIZATION, authorization).build());
                List<List<String>> configs = GsonUtils.GSON.fromJson(configInfo, new TypeToken<List<List<String>>>() {
                }.getType());
                for (List<String> conf : configs) {
                    if (confNames == null || confNames.isEmpty() || confNames.contains(conf.get(0))) {
                        addConfig(conf);
                    }
                }
                configRequestDoneSignal.markedCountDown(NetUtils
                        .getHostPortInAccessibleFormat(hostPort.first, hostPort.second), -1);
            } catch (Exception e) {
                LOG.warn("get config from {}:{} failed.", hostPort.first, hostPort.second, e);
                configRequestDoneSignal.countDown();
            }
        }

        private void addConfig(List<String> conf) {
            conf.add(1, NetUtils
                    .getHostPortInAccessibleFormat(hostPort.first, hostPort.second));
            conf.add(2, nodeType);
            config.add(conf);
        }
    }

    // Modify fe configuration.
    //
    // request body:
    // {
    //   "config_name":{
    //     "node":[
    //       ""
    //     ],
    //     "value":"",
    //     "persist":""
    //   }
    // }
    //
    // return data:
    // {
    //   "failed":[
    //     {
    //       "config_name":"",
    //       "value"="",
    //       "node":"",
    //       "err_info":""
    //     }
    //   ]
    //  }
    @RequestMapping(path = "/set_config/fe", method = RequestMethod.POST)
    public Object setConfigFe(HttpServletRequest request, HttpServletResponse response,
            @RequestBody Map<String, SetConfigRequestBody> requestBody) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        List<Map<String, String>> failedTotal = Lists.newArrayList();
        List<NodeConfigs> nodeConfigList = parseSetConfigNodes(requestBody, failedTotal);
        List<Pair<String, Integer>> aliveFe = Env.getCurrentEnv().getFrontends(null).stream().filter(Frontend::isAlive)
                .map(fe -> Pair.of(fe.getHost(), Config.http_port)).collect(Collectors.toList());
        checkNodeIsAlive(nodeConfigList, aliveFe, failedTotal);

        Map<String, String> header = Maps.newHashMap();
        header.put(AUTHORIZATION, request.getHeader(AUTHORIZATION));

        for (NodeConfigs nodeConfigs : nodeConfigList) {
            if (!nodeConfigs.getConfigs(true).isEmpty()) {
                String url = concatFeSetConfigUrl(nodeConfigs, true);
                try {
                    String responsePersist = HttpUtils.doGet(url, header);
                    parseFeSetConfigResponse(responsePersist, nodeConfigs.getHostPort(), failedTotal);
                } catch (Exception e) {
                    addSetConfigErrNode(nodeConfigs.getConfigs(true), nodeConfigs.getHostPort(), e.getMessage(),
                            failedTotal);
                }
            }
            if (!nodeConfigs.getConfigs(false).isEmpty()) {
                String url = concatFeSetConfigUrl(nodeConfigs, false);
                try {
                    String responseTemp = HttpUtils.doGet(url, header);
                    parseFeSetConfigResponse(responseTemp, nodeConfigs.getHostPort(), failedTotal);
                } catch (Exception e) {
                    addSetConfigErrNode(nodeConfigs.getConfigs(false), nodeConfigs.getHostPort(), e.getMessage(),
                            failedTotal);
                }
            }

        }
        Map<String, List<Map<String, String>>> data = Maps.newHashMap();
        data.put("failed", failedTotal);
        return ResponseEntityBuilder.ok(data);
    }

    private void addSetConfigErrNode(Map<String, String> configs, Pair<String, Integer> hostPort, String err,
            List<Map<String, String>> failedTotal) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            Map<String, String> failed = Maps.newHashMap();
            addFailedConfig(entry.getKey(), entry.getValue(), NetUtils
                    .getHostPortInAccessibleFormat(hostPort.first, hostPort.second), err, failed);
            failedTotal.add(failed);
        }
    }

    private void parseFeSetConfigResponse(String response, Pair<String, Integer> hostPort,
            List<Map<String, String>> failedTotal) throws Exception {
        JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
        if (jsonObject.get("code").getAsInt() != HttpUtils.REQUEST_SUCCESS_CODE) {
            throw new Exception(jsonObject.get("msg").getAsString());
        }
        SetConfigAction.SetConfigEntity setConfigEntity = GsonUtils.GSON.fromJson(
                jsonObject.get("data").getAsJsonObject(), SetConfigAction.SetConfigEntity.class);
        for (SetConfigAction.ErrConfig errConfig : setConfigEntity.getErrConfigs()) {
            Map<String, String> failed = Maps.newHashMap();
            addFailedConfig(errConfig.getConfigName(), errConfig.getConfigValue(),
                    NetUtils.getHostPortInAccessibleFormat(hostPort.first, hostPort.second), errConfig.getErrInfo(),
                    failed);
            failedTotal.add(failed);
        }
    }

    private static void addFailedConfig(String configName, String value, String node, String errInfo,
            Map<String, String> failed) {
        failed.put("config_name", configName);
        failed.put("value", value);
        failed.put("node", node);
        failed.put("err_info", errInfo);
    }

    private String concatFeSetConfigUrl(NodeConfigs nodeConfigs, boolean isPersist) {
        StringBuilder sb = new StringBuilder();
        Pair<String, Integer> hostPort = nodeConfigs.getHostPort();
        sb.append("http://").append(hostPort.first).append(":").append(hostPort.second).append("/api/_set_config");
        Map<String, String> configs = nodeConfigs.getConfigs(isPersist);
        boolean addAnd = false;
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            if (addAnd) {
                sb.append("&");
            } else {
                sb.append("?");
                addAnd = true;
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        if (isPersist) {
            sb.append("&persist=true&reset_persist=false");
        }
        return sb.toString();
    }

    // Modify fe configuration.
    // The request body and return data are in the same format as fe
    @RequestMapping(path = "/set_config/be", method = RequestMethod.POST)
    public Object setConfigBe(HttpServletRequest request, HttpServletResponse response,
            @RequestBody Map<String, SetConfigRequestBody> requestBody) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        List<Map<String, String>> failedTotal = Lists.newArrayList();
        List<NodeConfigs> nodeConfigList = parseSetConfigNodes(requestBody, failedTotal);
        List<Pair<String, Integer>> aliveBe = Env.getCurrentSystemInfo().getAllBackendIds(true).stream().map(beId -> {
            Backend be = Env.getCurrentSystemInfo().getBackend(beId);
            return Pair.of(be.getHost(), be.getHttpPort());
        }).collect(Collectors.toList());
        checkNodeIsAlive(nodeConfigList, aliveBe, failedTotal);

        handleBeSetConfig(nodeConfigList, request.getHeader(AUTHORIZATION), failedTotal);
        failedTotal = failedTotal.stream().filter(e -> !e.isEmpty()).collect(Collectors.toList());

        Map<String, List<Map<String, String>>> data = Maps.newHashMap();
        data.put("failed", failedTotal);
        return ResponseEntityBuilder.ok(data);
    }

    @PostMapping("/{action}/be")
    public Object operateBackend(HttpServletRequest request, HttpServletResponse response, @PathVariable String action,
            @RequestBody BackendReqInfo reqInfo) {
        try {
            if (!Env.getCurrentEnv().isMaster()) {
                return redirectToMasterOrException(request, response);
            }

            List<String> hostPorts = reqInfo.getHostPorts();
            List<HostInfo> hostInfos = new ArrayList<>();
            for (String hostPort : hostPorts) {
                hostInfos.add(SystemInfoService.getHostAndPort(hostPort));
            }
            SystemInfoService currentSystemInfo = Env.getCurrentSystemInfo();
            if ("ADD".equals(action)) {
                Map<String, String> properties;
                if (reqInfo.getProperties() == null) {
                    properties = new HashMap<>();
                } else {
                    properties = reqInfo.getProperties();
                }
                Map<String, String> tagMap = PropertyAnalyzer.analyzeBackendTagsProperties(properties,
                        Tag.DEFAULT_BACKEND_TAG);
                currentSystemInfo.addBackends(hostInfos, tagMap);
            } else if ("DROP".equals(action)) {
                currentSystemInfo.dropBackends(hostInfos);
            } else if ("DECOMMISSION".equals(action)) {
                ImmutableMap<Long, Backend> backendsInCluster = currentSystemInfo.getAllBackendsMap();
                backendsInCluster.forEach((k, v) -> {
                    hostInfos.stream()
                            .filter(h -> v.getHost().equals(h.getHost()) && v.getHeartbeatPort() == h.getPort())
                            .findFirst().ifPresent(h -> {
                                v.setDecommissioned(true);
                                Env.getCurrentEnv().getEditLog().logBackendStateChange(v);
                            });
                });
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        return ResponseEntityBuilder.ok();
    }

    @PostMapping("/{action}/fe")
    public Object operateFrontends(HttpServletRequest request, HttpServletResponse response,
            @PathVariable String action, @RequestBody FrontendReqInfo reqInfo) {
        try {
            if (!Env.getCurrentEnv().isMaster()) {
                return redirectToMasterOrException(request, response);
            }

            String role = reqInfo.getRole();
            Env currentEnv = Env.getCurrentEnv();
            FrontendNodeType frontendNodeType;
            if (FrontendNodeType.FOLLOWER.name().equals(role)) {
                frontendNodeType = FrontendNodeType.FOLLOWER;
            } else {
                frontendNodeType = FrontendNodeType.OBSERVER;
            }
            HostInfo info = SystemInfoService.getHostAndPort(reqInfo.getHostPort());
            if ("ADD".equals(action)) {
                currentEnv.addFrontend(frontendNodeType, Collections.singletonList(info));
            } else if ("DROP".equals(action)) {
                currentEnv.dropFrontend(frontendNodeType, Collections.singletonList(info));
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        return ResponseEntityBuilder.ok();
    }

    @Data
    private static class BackendReqInfo {

        private List<String> hostPorts;

        private Map<String, String> properties;
    }

    @Data
    private static class FrontendReqInfo {

        private String role;

        private String hostPort;
    }

    // Parsing request body into List<NodeConfigs>
    private List<NodeConfigs> parseSetConfigNodes(Map<String, SetConfigRequestBody> requestBody,
            List<Map<String, String>> errNodes) {
        List<NodeConfigs> nodeConfigsList = Lists.newArrayList();
        for (String configName : requestBody.keySet()) {
            SetConfigRequestBody configPara = requestBody.get(configName);
            String value = configPara.getValue();
            boolean persist = configPara.isPersist();
            if (value == null || configPara.getNodes() == null) {
                continue;
            }

            for (String node : configPara.getNodes()) {
                Pair<String, Integer> ipPort;
                try {
                    ipPort = SystemInfoService.validateHostAndPort(node);
                } catch (Exception e) {
                    Map<String, String> failed = Maps.newHashMap();
                    addFailedConfig(configName, configPara.getValue(), node, "node invalid", failed);
                    errNodes.add(failed);
                    continue;
                }
                boolean find = false;
                for (NodeConfigs nodeConfigs : nodeConfigsList) {
                    Pair<String, Integer> hostPort = nodeConfigs.getHostPort();
                    if (ipPort.first.equals(hostPort.first) && ipPort.second.equals(hostPort.second)) {
                        find = true;
                        nodeConfigs.addConfig(configName, value, persist);
                    }
                }
                if (!find) {
                    NodeConfigs newNodeConfigs = new NodeConfigs(ipPort.first, ipPort.second);
                    nodeConfigsList.add(newNodeConfigs);
                    newNodeConfigs.addConfig(configName, value, persist);
                }
            }
        }
        return nodeConfigsList;
    }

    private void checkNodeIsAlive(List<NodeConfigs> nodeConfigsList, List<Pair<String, Integer>> aliveNodes,
            List<Map<String, String>> failedNodes) {
        Iterator<NodeConfigs> it = nodeConfigsList.iterator();
        while (it.hasNext()) {
            NodeConfigs node = it.next();
            boolean isExist = false;
            for (Pair<String, Integer> aliveHostPort : aliveNodes) {
                if (aliveHostPort.first.equals(node.getHostPort().first) && aliveHostPort.second.equals(
                        node.getHostPort().second)) {
                    isExist = true;
                    break;
                }
            }
            if (!isExist) {
                addSetConfigErrNode(node.getConfigs(true), node.getHostPort(), "Node does not exist or is not alive",
                        failedNodes);
                addSetConfigErrNode(node.getConfigs(false), node.getHostPort(), "Node does not exist or is not alive",
                        failedNodes);
                it.remove();
            }
        }
    }

    private List<Map<String, String>> handleBeSetConfig(List<NodeConfigs> nodeConfigList, String authorization,
            List<Map<String, String>> failedTotal) {
        initHttpExecutor();

        int configNum = nodeConfigList.stream().mapToInt(e -> e.getConfigs(true).size() + e.getConfigs(false).size())
                .sum();
        MarkedCountDownLatch<String, Integer> beSetConfigCountDownSignal = new MarkedCountDownLatch<>(configNum);
        for (NodeConfigs nodeConfigs : nodeConfigList) {
            submitBeSetConfigTask(nodeConfigs, true, authorization, beSetConfigCountDownSignal, failedTotal);
            submitBeSetConfigTask(nodeConfigs, false, authorization, beSetConfigCountDownSignal, failedTotal);
        }
        try {
            beSetConfigCountDownSignal.await(HTTP_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("set be config exception:", e);
        } finally {
            List<Map.Entry<String, Integer>> leftNode = beSetConfigCountDownSignal.getLeftMarks();
            for (Map.Entry<String, Integer> failedNode : leftNode) {
                Map<String, String> failed = parseNodeConfig(failedNode.getKey());
                if (!failed.isEmpty()) {
                    failed.put("err_info", "Connection timeout");
                    failedTotal.add(failed);
                }
            }
        }
        return failedTotal;
    }

    private void submitBeSetConfigTask(NodeConfigs nodeConfigs, boolean isPersist, String authorization,
            MarkedCountDownLatch<String, Integer> beSetConfigCountDownSignal, List<Map<String, String>> failedTotal) {
        if (!nodeConfigs.getConfigs(isPersist).isEmpty()) {
            for (Map.Entry<String, String> entry : nodeConfigs.getConfigs(isPersist).entrySet()) {
                failedTotal.add(Maps.newHashMap());
                Pair<String, Integer> hostPort = nodeConfigs.getHostPort();
                beSetConfigCountDownSignal.addMark(
                        concatNodeConfig(hostPort.first, hostPort.second, entry.getKey(), entry.getValue()), -1);

                String url = concatBeSetConfigUrl(hostPort.first, hostPort.second, entry.getKey(), entry.getValue(),
                        isPersist);
                httpExecutor.submit(
                        new HttpSetConfigTask(url, hostPort, authorization, entry.getKey(), entry.getValue(),
                                beSetConfigCountDownSignal, failedTotal.get(failedTotal.size() - 1)));
            }
        }
    }

    private String concatBeSetConfigUrl(String host, Integer port, String configName, String configValue,
            boolean isPersist) {
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append("http://").append(host).append(":").append(port).append("/api/update_config").append("?")
                .append(configName).append("=").append(configValue);
        if (isPersist) {
            stringBuffer.append("&persist=true");
        }
        return stringBuffer.toString();
    }

    private String concatNodeConfig(String host, Integer port, String configName, String configValue) {
        return NetUtils
                .getHostPortInAccessibleFormat(host, port) + ":" + configName + ":" + configValue;
    }

    private Map<String, String> parseNodeConfig(String nodeConfig) {
        Map<String, String> map = Maps.newHashMap();
        String[] splitStrings = PATTERN.split(nodeConfig);
        if (splitStrings.length == 4) {
            addFailedConfig(splitStrings[2], splitStrings[3], splitStrings[0] + ":" + splitStrings[1], "", map);
        }
        return map;
    }

    private class HttpSetConfigTask implements Runnable {
        private String url;
        private Pair<String, Integer> hostPort;
        private String authorization;
        private String configName;
        private String configValue;
        private MarkedCountDownLatch<String, Integer> beSetConfigDoneSignal;
        private Map<String, String> failed;

        public HttpSetConfigTask(String url, Pair<String, Integer> hostPort, String authorization, String configName,
                String configValue, MarkedCountDownLatch<String, Integer> beSetConfigDoneSignal,
                Map<String, String> failed) {
            this.url = url;
            this.hostPort = hostPort;
            this.authorization = authorization;
            this.configName = configName;
            this.configValue = configValue;
            this.beSetConfigDoneSignal = beSetConfigDoneSignal;
            this.failed = failed;
        }

        @Override
        public void run() {
            try {
                String response = HttpUtils.doPost(url,
                        ImmutableMap.<String, String>builder().put(AUTHORIZATION, authorization).build(), null);
                JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
                String status = jsonObject.get("status").getAsString();
                if (!status.equals("OK")) {
                    addFailedConfig(configName, configValue, NetUtils
                            .getHostPortInAccessibleFormat(hostPort.first, hostPort.second),
                            jsonObject.get("msg").getAsString(), failed);
                }
                beSetConfigDoneSignal.markedCountDown(
                        concatNodeConfig(hostPort.first, hostPort.second, configName, configValue), -1);
            } catch (Exception e) {
                LOG.warn("set be:{} config:{} failed.", NetUtils
                        .getHostPortInAccessibleFormat(hostPort.first, hostPort.second),
                        configName + "=" + configValue, e);
                beSetConfigDoneSignal.countDown();
            }
        }
    }

    // Store persistent and non-persistent configuration information that needs to be modified on the node.
    public static class NodeConfigs {
        private Pair<String, Integer> hostPort;
        private Map<String, String> persistConfigs;
        private Map<String, String> nonPersistConfigs;

        public NodeConfigs(String host, Integer httpPort) {
            hostPort = Pair.of(host, httpPort);
            persistConfigs = Maps.newHashMap();
            nonPersistConfigs = Maps.newHashMap();
        }

        public Pair<String, Integer> getHostPort() {
            return hostPort;
        }

        public void addConfig(String name, String value, boolean persist) {
            if (persist) {
                persistConfigs.put(name, value);
            } else {
                nonPersistConfigs.put(name, value);
            }
        }

        public Map<String, String> getConfigs(boolean isPersist) {
            return isPersist ? persistConfigs : nonPersistConfigs;
        }

    }

    @Getter
    @Setter
    public static class ConfigInfoRequestBody {
        @JsonProperty("conf_name")
        public List<String> confNames;

        @JsonProperty("node")
        public List<String> nodes;
    }

    @Getter
    @Setter
    public static class SetConfigRequestBody {
        @JsonProperty("node")
        private List<String> nodes;

        private String value;

        private boolean persist;
    }
}
