package com.baidu.palo.deploy.impl;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.Pair;
import com.baidu.palo.deploy.DeployManager;

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
    
    public static final String ENV_FE_SERVICE_NAME = "FE_SERVICE_NAME";
    public static final String ENV_FE_NAMESPACE = "FE_NAMESPACE";
    public static final String ENV_FE_OBSERVER_SERVICE_NAME = "FE_OBSERVER_SERVICE_NAME";
    public static final String ENV_FE_OBSERVER_NAMESPACE = "FE_OBSERVER_NAMESPACE";
    public static final String ENV_BE_SERVICE_NAME = "BE_SERVICE_NAME";
    public static final String ENV_BE_NAMESPACE = "BE_NAMESPACE";
    public static final String ENV_BROKER_SERVICE_NAME = "BROKER_SERVICE_NAME";
    public static final String ENV_BROKER_NAMESPACE = "BROKER_NAMESPACE";
    
    public static final String ENV_BROKER_NAME = "BROKER_NAME";

    public static final String FE_PORT = "edit-log-port"; // k8s only support -, not _
    public static final String BE_PORT = "heartbeat-port";
    public static final String BROKER_PORT = "broker-port";

    private String feNamespace;
    private String observerNamespace;
    private String beNamespace;
    private String brokerNamespace;
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
        initEnvVariables(ENV_FE_SERVICE_NAME, ENV_FE_OBSERVER_SERVICE_NAME, ENV_BE_SERVICE_NAME,
                         ENV_BROKER_SERVICE_NAME);
    }

    @Override
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup) {
        super.initEnvVariables(envElectableFeServiceGroup, envObserverFeServiceGroup, envBackendServiceGroup,
                   envBrokerServiceGroup);

        // namespaces
        feNamespace = Strings.nullToEmpty(System.getenv(ENV_FE_NAMESPACE));
        beNamespace = Strings.nullToEmpty(System.getenv(ENV_BE_NAMESPACE));

        // FE and BE namespace must exist
        if (Strings.isNullOrEmpty(feNamespace) || Strings.isNullOrEmpty(beNamespace)) {
            LOG.error("failed to init namespace. feNamespace: {}, beNamespace: {}",
                     feNamespace, observerNamespace, beNamespace);
            System.exit(-1);
        }

        // observer namespace
        observerNamespace = Strings.nullToEmpty(System.getenv(ENV_FE_OBSERVER_NAMESPACE));
        if (Strings.isNullOrEmpty(observerNamespace)) {
            LOG.warn("failed to init observer namespace.");
            hasObserverService = false;
        }

        brokerNamespace = Strings.nullToEmpty(System.getenv(ENV_BROKER_NAMESPACE));
        if (Strings.isNullOrEmpty(brokerNamespace)) {
            LOG.warn("failed to init broker namespace.");
            hasBrokerService = false;
        }

        LOG.info("get namespace. feNamespace: {}, observerNamespace: {}, beNamespace: {}, brokerNamespace: {}",
                 feNamespace, observerNamespace, beNamespace, brokerNamespace);
    }

    @Override
    protected List<Pair<String, Integer>> getGroupHostPorts(String groupName) {
        // 1. get namespace and port name
        String namespace = null;
        String portName = null;
        if (groupName.equals(electableFeServiceGroup)) {
            namespace = feNamespace;
            portName = FE_PORT;
        } else if (groupName.equals(observerFeServiceGroup)) {
            namespace = observerNamespace;
            portName = FE_PORT;
        } else if (groupName.equals(backendServiceGroup)) {
            namespace = beNamespace;
            portName = BE_PORT;
        } else if (groupName.equals(brokerServiceGroup)) {
            namespace = brokerNamespace;
            portName = BROKER_PORT;
        } else {
            LOG.warn("unknown service group name: {}", groupName);
            return null;
        }
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(portName);

        // 2. get endpoint
        Endpoints endpoints = null;
        try {
            endpoints = endpoints(namespace, groupName);
        } catch (Exception e) {
            LOG.warn("encounter exception when get endpoint from namespace {}, service: {}",
                     namespace, groupName, e);
            return null;
        }
        if (endpoints == null) {
            // endpoints may be null if service does not exist;
            LOG.warn("get null endpoints of namespace {} in service: {}", namespace, groupName);
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
