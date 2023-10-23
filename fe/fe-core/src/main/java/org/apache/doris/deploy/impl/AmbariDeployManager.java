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

package org.apache.doris.deploy.impl;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.deploy.DeployManager;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.List;

/*
 * Required env variables:
 *
 *  FE_EXIST_HOSTS={{fe_hosts}}
 *  FE_INIT_NUMBER={{fe_init_number}}
 *  ENV_AMBARI_HOST={{ambari_server_host}}
 *  ENV_AMBARI_PORT={{ambari_server_port}}
 *  ENV_AMBARI_CLUSTER={{cluster_name}}
 *  ENV_AMBARI_SERVICE_NAME=PALO
 *  ENV_AMBARI_FE_COMPONENTS=PALO_FE    (electableFeServiceGroup)
 *  ENV_AMBARI_BE_COMPONENTS=PALO_BE    (backendServiceGroup)
 *  ENV_AMBARI_BROKER_COMPONENTS=PALO_BROKER    (brokerServiceGroup)
 *  ENV_AMBARI_FE_COMPONENTS_CONFIG=palo-fe-node
 *  ENV_AMBARI_BE_COMPONENTS_CONFIG=palo-be-node
 *  ENV_AMBARI_BROKER_COMPONENTS_CONFIG=palo-broker-node
 */
public class AmbariDeployManager extends DeployManager {
    private static final Logger LOG = LogManager.getLogger(AmbariDeployManager.class);

    // env var
    public static final String ENV_AUTH_INFO = "ENV_AUTH_INFO"; // username:passwd
    public static final String ENV_AMBARI_HOST = "ENV_AMBARI_HOST";
    public static final String ENV_AMBARI_PORT = "ENV_AMBARI_PORT";
    public static final String ENV_AMBARI_CLUSTER = "ENV_AMBARI_CLUSTER"; // BDP
    public static final String ENV_AMBARI_SERVICE_NAME = "ENV_AMBARI_SERVICE_NAME"; // PALO

    public static final String ENV_AMBARI_FE_COMPONENTS = "ENV_AMBARI_FE_COMPONENTS"; // PALO_FE
    public static final String ENV_AMBARI_BE_COMPONENTS = "ENV_AMBARI_BE_COMPONENTS";
    public static final String ENV_AMBARI_BROKER_COMPONENTS = "ENV_AMBARI_BROKER_COMPONENTS";
    public static final String ENV_AMBARI_CN_COMPONENTS = "ENV_AMBARI_CN_COMPONENTS";

    // used for getting config info from blueprint
    public static final String ENV_AMBARI_FE_COMPONENTS_CONFIG = "ENV_AMBARI_FE_COMPONENTS_CONFIG"; // palo-fe-node
    public static final String ENV_AMBARI_BE_COMPONENTS_CONFIG = "ENV_AMBARI_BE_COMPONENTS_CONFIG";
    public static final String ENV_AMBARI_BROKER_COMPONENTS_CONFIG = "ENV_AMBARI_BROKER_COMPONENTS_CONFIG";

    // url
    public static final String URL_BLUEPRINT = "http://%s/api/v1/clusters/%s?format=blueprint";
    public static final String URL_COMPONENTS = "http://%s/api/v1/clusters/%s/services/%s/components/%s";

    // keywords in json
    public static final String KEY_BE_HEARTBEAT_PORT = "be_heartbeat_service_port";
    public static final String KEY_FE_EDIT_LOG_PORT = "fe_edit_log_port";
    public static final String KEY_BROKER_IPC_PORT = "broker_ipc_port";
    public static final String KEY_BROKER_NAME = "broker_name";
    public static final String KEY_HOST_COMPONENTS = "host_components";
    public static final String KEY_HOST_ROLES = "HostRoles";
    public static final String KEY_HOST_NAME = "host_name";

    private String authInfo;
    private String encodedAuthInfo;
    private String ambariUrl;
    private String clusterName;
    private String serviceName;
    private String blueprintUrl;

    private String feConfigNode;
    private String beConfigNode;
    private String brokerConfigNode;

    // reset it every cycle
    private String blueprintJson;

    public AmbariDeployManager(Env env, long intervalMs) {
        super(env, intervalMs);
        initEnvVariables(ENV_AMBARI_FE_COMPONENTS, "", ENV_AMBARI_BE_COMPONENTS, ENV_AMBARI_BROKER_COMPONENTS,
                ENV_AMBARI_CN_COMPONENTS);
    }

    public AmbariDeployManager() {
        super(null, -1);
    }

    @Override
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup, String envCnServiceGroup) {
        super.initEnvVariables(envElectableFeServiceGroup, envObserverFeServiceGroup, envBackendServiceGroup,
                envBrokerServiceGroup, envCnServiceGroup);

        this.feConfigNode = Strings.nullToEmpty(System.getenv(ENV_AMBARI_FE_COMPONENTS_CONFIG));
        this.beConfigNode = Strings.nullToEmpty(System.getenv(ENV_AMBARI_BE_COMPONENTS_CONFIG));
        this.brokerConfigNode = Strings.nullToEmpty(System.getenv(ENV_AMBARI_BROKER_COMPONENTS_CONFIG));

        if (Strings.isNullOrEmpty(feConfigNode) || Strings.isNullOrEmpty(beConfigNode)) {
            LOG.error("failed to get fe config node: {} or be config node: {}. env var: {}, {}",
                    feConfigNode, beConfigNode, ENV_AMBARI_FE_COMPONENTS_CONFIG, ENV_AMBARI_BE_COMPONENTS_CONFIG);
            System.exit(-1);
        }

        if (Strings.isNullOrEmpty(brokerConfigNode)) {
            LOG.warn("can not get broker config node from env var: {}", ENV_AMBARI_BROKER_COMPONENTS_CONFIG);
            nodeTypeAttrMap.get(NodeType.BROKER).setHasService(false);
        }

        LOG.info("get fe, be and broker config node name: {}, {}, {}",
                feConfigNode, beConfigNode, brokerConfigNode);

        // 1. auth info
        authInfo = System.getenv(ENV_AUTH_INFO);
        LOG.info("get ambari auth info: {}", authInfo);
        encodedAuthInfo = Base64.encodeBase64String(authInfo.getBytes());

        // 2. ambari url
        String ambariHost = System.getenv(ENV_AMBARI_HOST);
        String ambariPort = System.getenv(ENV_AMBARI_PORT);
        if (Strings.isNullOrEmpty(ambariHost) || Strings.isNullOrEmpty(ambariPort)) {
            LOG.error("failed to get ambari host {} or ambari port {}", ambariHost, ambariPort);
            System.exit(-1);
        }

        int port = -1;
        try {
            port = Integer.valueOf(ambariPort);
        } catch (NumberFormatException e) {
            LOG.error("invalid ambari port format: {}", ambariPort);
            System.exit(-1);
        }

        ambariUrl = String.format("%s:%d", ambariHost, port);
        LOG.info("get ambari url: {}", ambariUrl);

        // 3. cluster name
        clusterName = System.getenv(ENV_AMBARI_CLUSTER);
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.error("failed to get ambari cluster", clusterName);
            System.exit(-1);
        }

        // 4. service name
        serviceName = System.getenv(ENV_AMBARI_SERVICE_NAME);
        if (Strings.isNullOrEmpty(serviceName)) {
            LOG.error("failed to get ambari service name", serviceName);
            System.exit(-1);
        }

        // 5. blueprint url
        blueprintUrl = String.format(URL_BLUEPRINT, ambariUrl, clusterName);
        LOG.info("get ambari blueprint url: {}", blueprintUrl);

        // 6. init first because we need blueprintJson when getting helper node
        if (!init()) {
            System.exit(-1);
        }
    }

    @Override
    protected boolean init() {
        super.init();

        // get blueprint once.
        blueprintJson = Util.getResultForUrl(blueprintUrl, encodedAuthInfo, 2000, 2000);
        if (blueprintJson == null) {
            return false;
        }

        return true;
    }

    @Override
    protected List<SystemInfoService.HostInfo> getGroupHostInfos(NodeType nodeType) {
        int port = -1;
        if (nodeType == NodeType.ELECTABLE) {
            port = getFeEditLogPort();
        } else if (nodeType == NodeType.BACKEND) {
            port = getBeHeartbeatPort();
        } else {
            LOG.warn("unknown type: {}", nodeType.name());
            return null;
        }
        if (port == -1) {
            LOG.warn("failed to get port of component: {}", nodeType.name());
            return null;
        }
        String groupName = nodeTypeAttrMap.get(nodeType).getServiceName();
        List<String> hostnames = getHostnamesFromComponentsJson(groupName);
        List<SystemInfoService.HostInfo> hostPorts = Lists.newArrayListWithCapacity(hostnames.size());
        for (String hostname : hostnames) {
            Pair<String, Integer> hostPort = null;
            try {
                hostPort = SystemInfoService.validateHostAndPort(NetUtils
                        .getHostPortInAccessibleFormat(hostname, port));
            } catch (AnalysisException e) {
                LOG.warn("Invalid host port format: {}:{}", hostname, port, e);
                continue;
            }
            hostPorts.add(new SystemInfoService.HostInfo(hostPort.first, hostPort.second));
        }

        LOG.info("get {} hosts from ambari: {}", groupName, hostPorts);
        return hostPorts;
    }

    @Override
    protected String getBrokerName() {
        return getPropertyFromBlueprint(brokerConfigNode, KEY_BROKER_NAME);
    }

    private Integer getFeEditLogPort() {
        return getPort(feConfigNode, KEY_FE_EDIT_LOG_PORT);
    }

    private Integer getBeHeartbeatPort() {
        return getPort(beConfigNode, KEY_BE_HEARTBEAT_PORT);
    }

    private Integer getBrokerIpcPort() {
        return getPort(brokerConfigNode, KEY_BROKER_IPC_PORT);
    }

    private Integer getPort(String configNodeName, String portName) {
        String portStr = getPropertyFromBlueprint(configNodeName, portName);
        if (portStr == null) {
            return -1;
        }

        Integer port = -1;
        try {
            port = Integer.valueOf(portStr);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid port format: {} of port {}", portStr, portName);
            return -1;
        }
        return port;
    }

    private List<String> getHostnamesFromComponentsJson(String componentName) {
        String urlStr = String.format(URL_COMPONENTS, ambariUrl, clusterName, serviceName, componentName);
        String componentsJson = Util.getResultForUrl(urlStr, encodedAuthInfo, 2000, 2000);
        if (componentsJson == null) {
            return null;
        }

        List<String> hostnames = Lists.newArrayList();
        JSONObject componentsObj = (JSONObject) JSONValue.parse(componentsJson);
        JSONArray componentsArray = (JSONArray) componentsObj.get(KEY_HOST_COMPONENTS);
        for (Object component : componentsArray) {
            JSONObject componentObj = (JSONObject) component;
            try {
                JSONObject roleObj = (JSONObject) componentObj.get(KEY_HOST_ROLES);
                String hostname = (String) roleObj.get(KEY_HOST_NAME);
                hostnames.add(hostname);
            } catch (Exception e) {
                // nothing
            }
        }

        return hostnames;
    }

    private String getPropertyFromBlueprint(String configNodeName, String propName) {
        Preconditions.checkNotNull(blueprintJson);
        String resProp = null;
        JSONObject root = (JSONObject) JSONValue.parse(blueprintJson);
        JSONArray confArray = (JSONArray) root.get("configurations");
        for (Object object : confArray) {
            JSONObject jobj = (JSONObject) object;
            try {
                JSONObject comNameObj = (JSONObject) jobj.get(configNodeName);
                JSONObject propObj = (JSONObject) comNameObj.get("properties");
                resProp = (String) propObj.get(propName);
            } catch (Exception e) {
                // nothing
            }
        }
        if (resProp == null) {
            LOG.warn("failed to get component {} property {}", configNodeName, propName);
        }
        return resProp;
    }
}
