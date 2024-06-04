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
import org.apache.doris.common.Config;
import org.apache.doris.deploy.DeployManager;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointPort;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import jline.internal.Log;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class K8sDeployManager extends DeployManager {
    private static final Logger LOG = LogManager.getLogger(K8sDeployManager.class);

    public static final String ENV_APP_NAMESPACE = "APP_NAMESPACE";
    public static final String ENV_DOMAIN_LTD = "DOMAIN_LTD";
    public static final String DEFAULT_APP_NAMESPACE = "default";
    public static final String DEFAULT_DOMAIN_LTD = "svc.cluster.local";
    // each SERVICE (FE/BE/OBSERVER/BROKER) represents a module of Doris, such as Frontends, Backends, ...
    // and each service has a name in k8s.
    public static final String ENV_FE_SERVICE = "FE_SERVICE";
    public static final String ENV_FE_OBSERVER_SERVICE = "FE_OBSERVER_SERVICE";
    public static final String ENV_BE_SERVICE = "BE_SERVICE";
    public static final String ENV_BROKER_SERVICE = "BROKER_SERVICE";
    public static final String ENV_CN_SERVICE = "CN_SERVICE";

    public static final String ENV_FE_STATEFULSET = "FE_STATEFULSET";
    public static final String ENV_FE_OBSERVER_STATEFULSET = "FE_OBSERVER_STATEFULSET";
    public static final String ENV_BE_STATEFULSET = "BE_STATEFULSET";
    public static final String ENV_BROKER_STATEFULSET = "BROKER_STATEFULSET";
    public static final String ENV_CN_STATEFULSET = "CN_STATEFULSET";

    public static final String FE_PORT = "edit-log-port"; // k8s only support -, not _
    public static final String BE_PORT = "heartbeat-port";
    public static final String BROKER_PORT = "broker-port";

    // corresponding to the environment variable ENV_APP_NAMESPACE.
    // App represents a Palo cluster in K8s, and has a namespace, and default namespace is 'default'
    private String appNamespace;
    private String domainLTD;
    private KubernetesClient client = null;
    private Watch statefulSetWatch = null;
    // =======for test only==========
    public static final String K8S_CA_CERT_FILE = "cce-ca.pem";
    public static final String K8S_CLIENT_CERT_FILE = "cce-admin.pem";
    public static final String K8S_CLIENT_KEY_FILE = "cce-admin-key.pem";

    public static final String TEST_MASTER_URL = "https://127.0.0.1:1111/";
    public static final String TEST_NAMESPACE = "default";
    public static final String TEST_SERVICENAME = "palo-fe";
    // =======for test only==========

    public K8sDeployManager(Env env, long intervalMs) {
        // if enable fqdn,we wait for k8s to actively push the statefulset change
        super(env, intervalMs, Config.enable_fqdn_mode);
        initEnvVariables(ENV_FE_SERVICE, ENV_FE_OBSERVER_SERVICE, ENV_BE_SERVICE, ENV_BROKER_SERVICE, ENV_CN_SERVICE);
    }

    @Override
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup, String envCnServiceGroup) {
        super.initEnvVariables(envElectableFeServiceGroup, envObserverFeServiceGroup, envBackendServiceGroup,
                envBrokerServiceGroup, envCnServiceGroup);

        // namespace
        appNamespace = Strings.nullToEmpty(System.getenv(ENV_APP_NAMESPACE));

        if (Strings.isNullOrEmpty(appNamespace)) {
            appNamespace = DEFAULT_APP_NAMESPACE;
        }

        LOG.info("use namespace: {}", appNamespace);

        domainLTD = Strings.nullToEmpty(System.getenv(ENV_DOMAIN_LTD));

        if (Strings.isNullOrEmpty(domainLTD)) {
            domainLTD = DEFAULT_DOMAIN_LTD;
        }

        LOG.info("use domainLTD: {}", domainLTD);

        if (Config.enable_fqdn_mode) {
            //Fill NodeTypeAttr.subAttr1 with statefulName
            //If serviceName is configured, the corresponding statefulSetName must be configured
            for (NodeType nodeType : NodeType.values()) {
                NodeTypeAttr nodeTypeAttr = nodeTypeAttrMap.get(nodeType);
                if (nodeTypeAttr.hasService()) {
                    String statefulSetEnvName = getStatefulSetEnvName(nodeType);
                    Log.info("Env name of: {} is: {}", nodeType.name(), statefulSetEnvName);
                    String statefulSetName = Strings.nullToEmpty(System.getenv(statefulSetEnvName));
                    if (Strings.isNullOrEmpty(statefulSetName)) {
                        LOG.error("failed to init statefulSetName: {}", statefulSetEnvName);
                        System.exit(-1);
                    }
                    LOG.info("use statefulSetName: {}, {}", nodeType.name(), statefulSetName);
                    nodeTypeAttr.setSubAttr(statefulSetName);
                }
            }
        }

    }

    @Override
    public void startListenerInternal() {
        statefulSetWatch = getWatch(client());
        LOG.info("Start listen statefulSets change event.");
    }

    private String getStatefulSetEnvName(NodeType nodeType) {
        switch (nodeType) {
            case ELECTABLE:
                return ENV_FE_STATEFULSET;
            case OBSERVER:
                return ENV_FE_OBSERVER_STATEFULSET;
            case BACKEND:
                return ENV_BE_STATEFULSET;
            case BACKEND_CN:
                return ENV_CN_STATEFULSET;
            case BROKER:
                return ENV_BROKER_STATEFULSET;
            default:
                return null;
        }
    }

    @Override
    protected List<HostInfo> getGroupHostInfos(NodeType nodeType) {
        if (Config.enable_fqdn_mode) {
            return getGroupHostInfosByStatefulSet(nodeType);
        } else {
            return getGroupHostInfosByEndpoint(nodeType);
        }
    }


    private List<HostInfo> getGroupHostInfosByStatefulSet(NodeType nodeType) {
        String statefulSetName = nodeTypeAttrMap.get(nodeType).getSubAttr();
        Preconditions.checkNotNull(statefulSetName);
        StatefulSet statefulSet = statefulSet(appNamespace, nodeTypeAttrMap.get(nodeType).getSubAttr());
        if (statefulSet == null) {
            LOG.warn("get null statefulSet in namespace {}, statefulSetName: {}", appNamespace, statefulSetName);
            return null;
        }
        return getHostInfosByNum(nodeType, statefulSet.getSpec().getReplicas());
    }

    private List<HostInfo> getGroupHostInfosByEndpoint(NodeType nodeType) {
        // get portName
        String portName = getPortName(nodeType);
        Preconditions.checkNotNull(portName);
        // get serviceName
        String serviceName = nodeTypeAttrMap.get(nodeType).getServiceName();
        Preconditions.checkNotNull(serviceName);

        // get endpoint
        Endpoints endpoints = endpoints(appNamespace, serviceName);
        if (endpoints == null) {
            // endpoints may be null if service does not exist;
            LOG.warn("get null endpoints of namespace: {} in service: {}", appNamespace, serviceName);
            return null;
        }

        // get host port
        List<HostInfo> result = Lists.newArrayList();
        List<EndpointSubset> subsets = endpoints.getSubsets();
        for (EndpointSubset subset : subsets) {
            Integer port = -1;
            List<EndpointPort> ports = subset.getPorts();
            for (EndpointPort eport : ports) {
                if (eport.getName().equals(portName)) {
                    port = eport.getPort();
                    break;
                }
            }
            if (port == -1) {
                LOG.warn("failed to get {} port", portName);
                return null;
            }

            List<EndpointAddress> addrs = subset.getAddresses();
            for (EndpointAddress eaddr : addrs) {
                result.add(new HostInfo(eaddr.getIp(), port));
            }
        }

        LOG.info("get host port from group: {}: {}", serviceName, result);
        return result;
    }

    // The rules for the domain name of k8s are $(podName).$(servicename).$(namespace).svc.cluster.local
    // and The podName rule of k8s is $(statefulset name) - $(sequence number)
    // see https://www.cnblogs.com/xiaokantianse/p/14267987.html#_label1_4
    public String getDomainName(NodeType nodeType, int index) {
        String statefulSetName = nodeTypeAttrMap.get(nodeType).getSubAttr();
        Preconditions.checkNotNull(statefulSetName);
        String serviceName = nodeTypeAttrMap.get(nodeType).getServiceName();
        Preconditions.checkNotNull(serviceName);

        StringBuilder builder = new StringBuilder();
        builder.append(statefulSetName + "-" + index);
        builder.append(".");
        builder.append(serviceName);
        builder.append(".");
        builder.append(appNamespace);
        builder.append(".");
        builder.append(domainLTD);
        return builder.toString();
    }

    private Endpoints endpoints(String namespace, String serviceName) {
        try {
            return client().endpoints().inNamespace(namespace).withName(serviceName).get();
        } catch (Exception e) {
            LOG.warn("encounter exception when get endpoint from namespace {}, service: {}",
                    appNamespace, serviceName, e);
            return null;
        }

    }

    public Service service(String namespace, String serviceName) {
        try {
            return client().services().inNamespace(namespace).withName(serviceName).get();
        } catch (Exception e) {
            LOG.warn("encounter exception when get service from namespace {}, service: {}",
                    appNamespace, serviceName, e);
            return null;
        }
    }

    public StatefulSet statefulSet(String namespace, String statefulSetName) {
        try {
            return client().apps().statefulSets().inNamespace(namespace).withName(statefulSetName).get();
        } catch (Exception e) {
            LOG.warn("encounter exception when get statefulSet from namespace {}, statefulSet: {}",
                    appNamespace, statefulSetName, e);
            return null;
        }
    }

    private void dealEvent(String statefulsetName, Integer num) {
        NodeType nodeType = getNodeType(statefulsetName);
        if (nodeType == null) {
            return;
        }
        List<HostInfo> hostInfosByNum = getHostInfosByNum(nodeType, num);
        if (hostInfosByNum == null) {
            return;
        }
        Event event = new Event(nodeType, hostInfosByNum);
        nodeChangeQueue.offer(event);
    }

    public List<HostInfo> getHostInfosByNum(NodeType nodeType, Integer num) {
        int servicePort = getServicePort(nodeType);
        if (servicePort == -1) {
            LOG.warn("get servicePort failed,{}", nodeType.name());
            return null;
        }
        List<HostInfo> hostInfos = Lists.newArrayList();
        for (int i = 0; i < num; i++) {
            String domainName = getDomainName(nodeType, i);
            hostInfos.add(new HostInfo(domainName, servicePort));
            if (LOG.isDebugEnabled()) {
                LOG.debug("get hostInfo from domainName: {}, hostInfo: {}", domainName, hostInfos.get(i).toString());
            }
        }
        return hostInfos;
    }

    private int getServicePort(NodeType nodeType) {
        Integer port = -1;
        String serviceName = nodeTypeAttrMap.get(nodeType).getServiceName();
        Preconditions.checkNotNull(serviceName);
        Service service = service(appNamespace, serviceName);
        if (service == null) {
            LOG.warn("get null service in namespace: {}, serviceName: {}", appNamespace, serviceName);
            return port;
        }
        String portName = getPortName(nodeType);
        Preconditions.checkNotNull(portName);
        List<ServicePort> ports = service.getSpec().getPorts();
        for (ServicePort servicePort : ports) {
            if (servicePort.getName().equals(portName)) {
                port = servicePort.getPort();
                break;
            }
        }
        return port;
    }

    private String getPortName(NodeType nodeType) {
        switch (nodeType) {
            case BROKER:
                return BROKER_PORT;
            case ELECTABLE:
            case OBSERVER:
                return FE_PORT;
            case BACKEND:
            case BACKEND_CN:
                return BE_PORT;
            default:
                return null;
        }
    }


    private NodeType getNodeType(String ststefulSetName) {
        if (StringUtils.isEmpty(ststefulSetName)) {
            return null;
        }
        for (Map.Entry<NodeType, NodeTypeAttr> entry : nodeTypeAttrMap.entrySet()) {
            if (ststefulSetName.equals(entry.getValue().getSubAttr())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private synchronized KubernetesClient client() {
        if (client != null) {
            return client;
        }

        try {
            if (Config.with_k8s_certs) {
                // for test only
                ConfigBuilder configBuilder = new ConfigBuilder().withMasterUrl(TEST_MASTER_URL)
                        .withTrustCerts(true)
                        .withCaCertFile(K8S_CA_CERT_FILE).withClientCertFile(K8S_CLIENT_CERT_FILE)
                        .withClientKeyFile(K8S_CLIENT_KEY_FILE);
                client = new DefaultKubernetesClient(configBuilder.build());
            } else {
                // When accessing k8s api within the pod, no params need to be provided.
                client = new DefaultKubernetesClient();
            }
        } catch (KubernetesClientException e) {
            LOG.warn("failed to get k8s client.", e);
            throw e;
        }

        return client;
    }

    private Watch getWatch(KubernetesClient client) {
        return client.apps().statefulSets().inNamespace(appNamespace).watch(new Watcher<StatefulSet>() {

            @Override
            public void onClose(WatcherException e) {
                LOG.warn("Watch error received: {}.", e.getMessage());
            }

            @Override
            public void eventReceived(Action action, StatefulSet statefulSet) {
                LOG.info("Watch event received {}: {}: {}", action.name(), statefulSet.getMetadata().getName(),
                        statefulSet.getSpec().getReplicas());
                dealEvent(statefulSet.getMetadata().getName(), statefulSet.getSpec().getReplicas());
            }

            @Override
            public void onClose() {
                LOG.info("Watch gracefully closed.");
            }
        });
    }

    public void close() {
        if (statefulSetWatch != null) {
            statefulSetWatch.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
