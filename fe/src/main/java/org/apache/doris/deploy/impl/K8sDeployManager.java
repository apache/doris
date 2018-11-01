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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.deploy.DeployManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointPort;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;

public class K8sDeployManager extends DeployManager {
    private static final Logger LOG = LogManager.getLogger(K8sDeployManager.class);
    
    public static final String ENV_APP_NAMESPACE = "APP_NAMESPACE";
    // each SERVICE (FE/BE/OBSERVER/BROKER) represents a module of Palo, such as Frontends, Backends, ...
    // and each service has a name in k8s.
    public static final String ENV_FE_SERVICE = "FE_SERVICE";
    public static final String ENV_FE_OBSERVER_SERVICE = "FE_OBSERVER_SERVICE";
    public static final String ENV_BE_SERVICE = "BE_SERVICE";
    public static final String ENV_BROKER_SERVICE = "BROKER_SERVICE";
    
    // we arbitrarily set all broker name as what ENV_BROKER_NAME specified.
    public static final String ENV_BROKER_NAME = "BROKER_NAME";

    public static final String FE_PORT = "edit-log-port"; // k8s only support -, not _
    public static final String BE_PORT = "heartbeat-port";
    public static final String BROKER_PORT = "broker-port";

    // corresponding to the environment variable ENV_APP_NAMESPACE.
    // App represents a Palo cluster in K8s, and has a namespace, and default namespace is 'default'
    private String appNamespace;
    private KubernetesClient client = null;

    // =======for test only==========
    public static final String K8S_CA_CERT_FILE = "cce-ca.pem";
    public static final String K8S_CLIENT_CERT_FILE = "cce-admin.pem";
    public static final String K8S_CLIENT_KEY_FILE = "cce-admin-key.pem";
    
    public static final String TEST_MASTER_URL = "https://127.0.0.1:1111/";
    public static final String TEST_NAMESPACE = "default";
    public static final String TEST_SERVICENAME = "palo-fe";
    // =======for test only==========

    public K8sDeployManager(Catalog catalog, long intervalMs) {
        super(catalog, intervalMs);
        initEnvVariables(ENV_FE_SERVICE, ENV_FE_OBSERVER_SERVICE, ENV_BE_SERVICE, ENV_BROKER_SERVICE);
    }

    @Override
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup) {
        super.initEnvVariables(envElectableFeServiceGroup, envObserverFeServiceGroup, envBackendServiceGroup,
                   envBrokerServiceGroup);

        // namespace
        appNamespace = Strings.nullToEmpty(System.getenv(ENV_APP_NAMESPACE));

        if (Strings.isNullOrEmpty(appNamespace)) {
            LOG.error("failed to init namespace: " + ENV_APP_NAMESPACE);
            System.exit(-1);
        }

        LOG.info("get namespace: {}", appNamespace);
    }

    @Override
    protected List<Pair<String, Integer>> getGroupHostPorts(String groupName) {
        // 1. get namespace and port name
        String portName = null;
        if (groupName.equals(electableFeServiceGroup)) {
            portName = FE_PORT;
        } else if (groupName.equals(observerFeServiceGroup)) {
            portName = FE_PORT;
        } else if (groupName.equals(backendServiceGroup)) {
            portName = BE_PORT;
        } else if (groupName.equals(brokerServiceGroup)) {
            portName = BROKER_PORT;
        } else {
            LOG.warn("unknown service group name: {}", groupName);
            return null;
        }
        Preconditions.checkNotNull(appNamespace);
        Preconditions.checkNotNull(portName);

        // 2. get endpoint
        Endpoints endpoints = null;
        try {
            endpoints = endpoints(appNamespace, groupName);
        } catch (Exception e) {
            LOG.warn("encounter exception when get endpoint from namespace {}, service: {}",
                     appNamespace, groupName, e);
            return null;
        }
        if (endpoints == null) {
            // endpoints may be null if service does not exist;
            LOG.warn("get null endpoints of namespace {} in service: {}", appNamespace, groupName);
            return null;
        }

        // 3. get host port
        List<Pair<String, Integer>> result = Lists.newArrayList();
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
                result.add(Pair.create(eaddr.getIp(), port));
            }
        }

        LOG.info("get host port from group: {}: {}", groupName, result);
        return result;
    }

    @Override
    protected Map<String, List<Pair<String, Integer>>> getBrokerGroupHostPorts() {
        List<Pair<String, Integer>> hostPorts = getGroupHostPorts(brokerServiceGroup);
        if (hostPorts == null) {
            return null;
        }
        final String brokerName = System.getenv(ENV_BROKER_NAME);
        if (Strings.isNullOrEmpty(brokerName)) {
            LOG.error("failed to get broker name from env: {}", ENV_BROKER_NAME);
            System.exit(-1);
        }

        Map<String, List<Pair<String, Integer>>> brokers = Maps.newHashMap();
        brokers.put(brokerName, hostPorts);
        LOG.info("get brokers from k8s: {}", brokers);
        return brokers;
    }

    private Endpoints endpoints(String namespace, String serviceName) throws Exception {
        return client().endpoints().inNamespace(namespace).withName(serviceName).get();
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

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
