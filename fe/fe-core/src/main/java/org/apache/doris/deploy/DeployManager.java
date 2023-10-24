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

package org.apache.doris.deploy;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.deploy.impl.LocalFileDeployManager;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/*
 * This deploy manager is to support Kubernetes, Ambari or other system for automating deployment.
 * The deploy manager will try to get the helper node when initialize catalog.
 * When this FE is transfer to Master, it will start a polling thread to detect the node change of at most 4
 * service groups in remote deployment system:
 *
 *      electableFeServiceGroup: contains Master and Follower FE
 *      backendServiceGroup: contains Backends
 *      observerFeServiceGroup:  contains Observer FE (optional, k8s only)
 *      brokerServiceGroup: contains Broker (optional, Ambari only)
 *
 * When node changing is detected, the deploy manager will try to ADD or DROP the new or missing node.
 *
 * Current support operations:
 *
 * A. Startup
 * 1. Start 1 Frontend(FE), and automatically transfer to the single startup Master.
 * 2. Start 3 FEs, they will reach a consensus on choosing first FE in node list as startup Master.
 *
 * B. Expansion
 * 1. With 1 existing FE(Master), add 2 FEs to reach HA.
 * 2. With 1 or 3 existing FE(Master + Follower), add more FE(observer).
 * 3. With 1 or 3 existing FE(Master + Follower), add more Backends(BE).
 * 3. With 1 or 3 existing FE(Master + Follower), add more Broker.
 *
 * C. Shrink
 * 1. With 3 existing FEs, drop 2 FEs.
 * 2. With 1 or 3 existing FE(Master + Follower), drop existing FE(observer).
 * 3. With 1 or 3 existing FE(Master + Follower), drop existing BE.
 * 3. With 1 or 3 existing FE(Master + Follower), drop existing Broker.
 *
 * Environment variables:
 *
 * FE_EXIST_ENDPOINT:
 *      he existing FE(Master + Follower) before the new FE start up.
 *      The main reason of this var is to indicate whether there is already an alive Master
 *      or the consensus of who is master is needed.
 *
 * FE_INIT_NUMBER:
 *      Number of newly start up FE(Master + Follower), can only be 1 or 3.
 *
 * Only one of FE_EXIST_ENDPOINT and FE_INIT_NUMBER need to be set.
 *
 * eg:
 *
 *  1. Start 1 FE as a single Master
 *      set FE_EXIST_ENDPOINT as empty
 *      set FE_INIT_NUMBER = 1
 *
 *  2. Start 3 FE(Master + Follower)
 *      set FE_EXIST_ENDPOINT as empty
 *      set FE_INIT_NUMBER = 3
 *
 *  3. With 1 existing FE(Master), add 2 FEs to reach HA.
 *      set FE_EXIST_ENDPOINT=existing_fe_host:edit_log_port
 *      set FE_INIT_NUMBER as empty
 *
 */
public class DeployManager extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DeployManager.class);

    public static final String ENV_FE_EXIST_ENDPOINT = "FE_EXIST_ENDPOINT";
    public static final String ENV_FE_INIT_NUMBER = "FE_INIT_NUMBER";
    // we arbitrarily set all broker name as what ENV_BROKER_NAME specified.
    public static final String ENV_BROKER_NAME = "BROKER_NAME";

    public enum NodeType {
        ELECTABLE, OBSERVER, BACKEND, BROKER, BACKEND_CN
    }

    protected Env env;

    // Host identifier -> missing counter
    // eg:
    // In k8s, when a node is down, the endpoint may be disappear immediately in service group.
    // But the k8s will start it up again soon.
    // In the gap between node's down and startup, the deploy manager may detect the missing node and
    // do the unnecessary dropping operations.
    // So we use this map to count the continuous detected down times, if the continuous down time is more
    // then MAX_MISSING_TIME, we considered this node as down permanently.
    protected Map<String, Integer> counterMap = Maps.newHashMap();
    protected Integer maxMissingTime = 5;
    //if 'true',Actively pull node information from external systems.
    //if 'false',The external system actively pushes the node change information,
    // and only needs to listen to 'nodeChangeQueue'
    protected boolean listenRequired;
    protected BlockingQueue<Event> nodeChangeQueue;
    protected Map<NodeType, NodeTypeAttr> nodeTypeAttrMap = Maps.newHashMap();
    private boolean isRunning;

    public DeployManager(Env env, long intervalMs) {
        this(env, intervalMs, false);
    }

    public DeployManager(Env env, long intervalMs, boolean listenRequired) {
        super("deployManager", intervalMs);
        this.env = env;
        this.listenRequired = listenRequired;
        this.isRunning = false;
        if (listenRequired) {
            this.maxMissingTime = 0;
            this.nodeChangeQueue = Queues.newLinkedBlockingDeque();
        }
        // init NodeTypeAttr for each NodeType,so when get NodeTypeAttr by NodeType,we assume not null
        for (NodeType nodeType : NodeType.values()) {
            nodeTypeAttrMap.put(nodeType, new NodeTypeAttr(false));
        }
    }

    // Init all environment variables.
    // Derived Class can override this method to init more private env variables,
    // but must class the parent's init at first.
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup, String envCnServiceGroup) {

        String electableFeServiceGroup = Strings.nullToEmpty(System.getenv(envElectableFeServiceGroup));
        String observerFeServiceGroup = Strings.nullToEmpty(System.getenv(envObserverFeServiceGroup));
        String backendServiceGroup = Strings.nullToEmpty(System.getenv(envBackendServiceGroup));
        String brokerServiceGroup = Strings.nullToEmpty(System.getenv(envBrokerServiceGroup));
        String cnServiceGroup = Strings.nullToEmpty(System.getenv(envCnServiceGroup));

        LOG.info("get deploy env: {}, {}, {}, {}, {}", envElectableFeServiceGroup, envObserverFeServiceGroup,
                envBackendServiceGroup, envBrokerServiceGroup, envCnServiceGroup);

        // check if we have electable service
        if (!Strings.isNullOrEmpty(electableFeServiceGroup)) {
            LOG.info("Electable service group is found");
            nodeTypeAttrMap.get(NodeType.ELECTABLE).setHasService(true);
            nodeTypeAttrMap.get(NodeType.ELECTABLE).setServiceName(electableFeServiceGroup);
        }

        // check if we have observer service
        if (!Strings.isNullOrEmpty(observerFeServiceGroup)) {
            LOG.info("Observer service group is found");
            nodeTypeAttrMap.get(NodeType.OBSERVER).setHasService(true);
            nodeTypeAttrMap.get(NodeType.OBSERVER).setServiceName(observerFeServiceGroup);
        }

        // check if we have backend service
        if (!Strings.isNullOrEmpty(backendServiceGroup)) {
            LOG.info("Backend service group is found");
            nodeTypeAttrMap.get(NodeType.BACKEND).setHasService(true);
            nodeTypeAttrMap.get(NodeType.BACKEND).setServiceName(backendServiceGroup);
        }

        // check if we have broker service
        if (!Strings.isNullOrEmpty(brokerServiceGroup)) {
            LOG.info("Broker service group is found");
            nodeTypeAttrMap.get(NodeType.BROKER).setHasService(true);
            nodeTypeAttrMap.get(NodeType.BROKER).setServiceName(brokerServiceGroup);
        }

        // check if we have cn service
        if (!Strings.isNullOrEmpty(cnServiceGroup)) {
            LOG.info("Cn service group is found");
            nodeTypeAttrMap.get(NodeType.BACKEND_CN).setHasService(true);
            nodeTypeAttrMap.get(NodeType.BACKEND_CN).setServiceName(cnServiceGroup);
        }

        LOG.info("get electableFeServiceGroup: {}, observerFeServiceGroup: {}, backendServiceGroup: {}"
                        + " brokerServiceGroup: {}, cnServiceGroup: {}",
                electableFeServiceGroup, observerFeServiceGroup, backendServiceGroup, brokerServiceGroup,
                cnServiceGroup);
    }

    public void startListener() {
        if (listenRequired) {
            startListenerInternal();
        }
    }

    public void startListenerInternal() {
        throw new NotImplementedException("startListenerInternal not implemented");
    }

    // Call init before each runOneCycle
    // Default is do nothing. Can be override in derived class
    // return false if init failed.
    protected boolean init() {
        return true;
    }

    // Get all host port pairs from specified group.
    // Must implement in derived class.
    // If encounter errors, return null
    protected List<HostInfo> getGroupHostInfos(NodeType nodeType) {
        throw new NotImplementedException("getGroupHostInfos not implemented");
    }

    protected String getBrokerName() {
        String brokerName = System.getenv(ENV_BROKER_NAME);
        if (Strings.isNullOrEmpty(brokerName)) {
            LOG.error("failed to get broker name from env: {}", ENV_BROKER_NAME);
            System.exit(-1);
        }
        return brokerName;
    }

    public List<HostInfo> getHelperNodes() {
        String existFeHosts = System.getenv(ENV_FE_EXIST_ENDPOINT);
        if (!Strings.isNullOrEmpty(existFeHosts)) {
            // Some Frontends already exist in service group.
            // We consider them as helper node
            List<HostInfo> helperNodes = Lists.newArrayList();
            String[] splittedHosts = existFeHosts.split(",");
            for (String host : splittedHosts) {
                try {
                    helperNodes.add(SystemInfoService.getHostAndPort(host));
                } catch (AnalysisException e) {
                    LOG.error("Invalid exist fe hosts: {}. will exit", existFeHosts);
                    System.exit(-1);
                }
            }

            return helperNodes;
        }

        // No Frontend exist before.
        // This should be the every first time to start up the Frontend.
        // We use the following strategy to determine which one should be the master:
        // 1. get num of FE from environment variable FE_NUM
        // 2. get electable frontend hosts from electable service group
        // 3. sort electable frontend hosts
        // 4. choose the first host as master candidate

        // 1. get num of fe
        final String numOfFeStr = System.getenv(ENV_FE_INIT_NUMBER);
        if (Strings.isNullOrEmpty(numOfFeStr)) {
            LOG.error("No init FE num is specified. will exit");
            System.exit(-1);
        }

        Integer numOfFe = -1;
        try {
            numOfFe = Integer.valueOf(numOfFeStr);
        } catch (NumberFormatException e) {
            LOG.error("Invalid format of num of fe: {}. will exit", numOfFeStr);
            System.exit(-1);
        }
        LOG.info("get init num of fe from env: {}", numOfFe);

        // 2. get electable fe host from remote
        boolean ok = true;
        List<HostInfo> feHostInfos = null;
        while (true) {
            try {
                feHostInfos = getGroupHostInfos(NodeType.ELECTABLE);
                if (feHostInfos == null) {
                    ok = false;
                } else if (feHostInfos.size() != numOfFe) {
                    LOG.error("num of fe get from remote [{}] does not equal to the expected num: {}",
                            feHostInfos, numOfFe);
                    ok = false;
                } else {
                    ok = true;
                }
            } catch (Exception e) {
                LOG.error("failed to get electable fe hosts from remote.", e);
                ok = false;
            }

            if (!ok) {
                // Sleep 5 seconds and try again
                try {
                    Thread.sleep(5000);
                    continue;
                } catch (InterruptedException e) {
                    LOG.error("get InterruptedException when sleep", e);
                    System.exit(-1);
                }
            }

            LOG.info("get electable fe host from remote: {}", feHostInfos);
            break;
        }

        // 3. sort fe host list
        Collections.sort(feHostInfos);
        LOG.info("sorted fe host list: {}", feHostInfos);

        // 4. return the first one as helper
        return Lists.newArrayList(new HostInfo(feHostInfos.get(0).getHost(),
                feHostInfos.get(0).getPort()));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (Config.enable_deploy_manager.equals("disable")) {
            LOG.warn("Config enable_deploy_manager is disable. Exit deploy manager");
            exit();
            return;
        }
        // 0. init
        if (!init()) {
            return;
        }

        if (isRunning) {
            LOG.warn("Last task not finished, ignore current task.");
            return;
        }
        isRunning = true;

        if (listenRequired && processQueue()) {
            isRunning = false;
            return;
        }
        try {
            processPolling();
        } catch (Exception e) {
            LOG.warn("failed to process polling", e);
        } finally {
            isRunning = false;
        }
    }

    private void processPolling() {
        for (NodeType nodeType : NodeType.values()) {
            NodeTypeAttr nodeTypeAttr = nodeTypeAttrMap.get(nodeType);
            if (!nodeTypeAttr.hasService) {
                continue;
            }
            List<HostInfo> remoteHosts = getGroupHostInfos(nodeType);
            LOG.debug("get serviceName: {},remoteHosts: {}", nodeTypeAttr.getServiceName(), remoteHosts);
            process(nodeType, remoteHosts);
        }
    }

    private boolean processQueue() {
        Event event = nodeChangeQueue.poll();
        if (event == null) {
            return false;
        }
        process(event.getNodeType(), event.getHostInfos());
        return true;
    }

    private void process(NodeType nodeType, List<HostInfo> remoteHosts) {
        if (remoteHosts == null) {
            return;
        }
        if (nodeType == NodeType.ELECTABLE && remoteHosts.isEmpty()) {
            LOG.warn("electable fe service is empty, which should not happen");
            return;
        }
        List<HostInfo> localHosts = getLocalHosts(nodeType);
        inspectNodeChange(remoteHosts, localHosts, nodeType);
    }

    private List<HostInfo> getLocalHosts(NodeType nodeType) {
        switch (nodeType) {
            case ELECTABLE:
                List<Frontend> localElectableFeAddrs = env.getFrontends(FrontendNodeType.FOLLOWER);
                return this.convertFesToHostInfos(localElectableFeAddrs);
            case OBSERVER:
                List<Frontend> localObserverFeAddrs = env.getFrontends(FrontendNodeType.OBSERVER);
                return this.convertFesToHostInfos(localObserverFeAddrs);
            case BACKEND:
                List<Backend> localBackends = Env.getCurrentSystemInfo().getMixBackends();
                return this.convertBesToHostInfos(localBackends);
            case BACKEND_CN:
                List<Backend> localCns = Env.getCurrentSystemInfo().getCnBackends();
                return this.convertBesToHostInfos(localCns);
            case BROKER:
                List<FsBroker> localBrokers = env.getBrokerMgr().getBrokerListMap().get(getBrokerName());
                if (localBrokers == null) {
                    localBrokers = Lists.newArrayList();
                }
                return convertBrokersToHostInfos(localBrokers);
            default:
                break;
        }
        return null;
    }

    private boolean needDrop(boolean found, HostInfo localHostInfo) {
        if (found) {
            counterMap.remove(localHostInfo.getIdent());
            return false;
        } else {
            if (maxMissingTime <= 0) {
                return true;
            }
            // Check the detected downtime
            if (!counterMap.containsKey(localHostInfo.getIdent())) {
                // First detected downtime. Add to the map and ignore
                LOG.warn("downtime node: {} detected times: 1",
                        localHostInfo);
                counterMap.put(localHostInfo.getIdent(), 1);
                return false;
            } else {
                int times = counterMap.get(localHostInfo.getIdent());
                if (times < maxMissingTime) {
                    LOG.warn("downtime node: {} detected times: {}",
                            localHostInfo, times + 1);
                    counterMap.put(localHostInfo.getIdent(), times + 1);
                    return false;
                } else {
                    // Reset the counter map and do the dropping operation
                    LOG.warn("downtime node: {} detected times: {}. drop it",
                            localHostInfo, times + 1);
                    counterMap.remove(localHostInfo.getIdent());
                    return true;
                }
            }
        }
    }

    /*
     * Inspect the node change.
     * 1. Check if there are some nodes need to be dropped.
     * 2. Check if there are some nodes need to be added.
     *
     * Return true if something changed
     */
    private void inspectNodeChange(List<HostInfo> remoteHostInfos,
            List<HostInfo> localHostInfos,
            NodeType nodeType) {

        if (LOG.isDebugEnabled()) {
            for (HostInfo hostInfo : remoteHostInfos) {
                LOG.debug("inspectNodeChange: remote host info: {}", hostInfo);
            }

            for (HostInfo hostInfo : localHostInfos) {
                LOG.debug("inspectNodeChange: local host info: {}", hostInfo);
            }
        }

        // 2.1 Find local node which need to be dropped.
        List<HostInfo> dropHostInfos = new ArrayList<>(localHostInfos);
        for (HostInfo localHostInfo : localHostInfos) {
            HostInfo foundHostInfo = getFromHostInfos(remoteHostInfos, localHostInfo);
            boolean needDrop = needDrop(foundHostInfo != null, localHostInfo);
            if (needDrop) {
                if (this instanceof LocalFileDeployManager && Config.disable_local_deploy_manager_drop_node) {
                    LOG.warn("For now, Local File Deploy Manager dose not handle shrinking operations");
                    dropHostInfos.remove(foundHostInfo);
                    continue;
                }
                dealDropLocal(dropHostInfos, nodeType);
            }
        }

        // 2.2. Find remote node which need to be added.
        List<HostInfo> addRemoteHostInfos = new ArrayList<>();
        for (HostInfo remoteHostInfo : remoteHostInfos) {
            HostInfo foundHostInfo = getFromHostInfos(localHostInfos, remoteHostInfo);
            if (foundHostInfo == null) {
                addRemoteHostInfos.add(remoteHostInfo);
            }
        }
        dealAddRemote(addRemoteHostInfos, nodeType);
    }

    private void dealDropLocal(List<HostInfo> localHostInfo, NodeType nodeType) {
        for (HostInfo hostInfo : localHostInfo) {
            // Double check if is itself
            if (isSelf(hostInfo)) {
                // This is itself. Shut down now.
                LOG.error("self host {} does not exist in remote hosts. master is: {}:{}. Showdown.",
                        localHostInfo, env.getMasterHost(), Config.edit_log_port);
                System.exit(-1);
            }
        }

        // Can not find local host from remote host list,
        // which means this node should be dropped.
        try {
            switch (nodeType) {
                case ELECTABLE:
                    env.dropFrontend(FrontendNodeType.FOLLOWER, localHostInfo);
                    break;
                case OBSERVER:
                    env.dropFrontend(FrontendNodeType.OBSERVER, localHostInfo);
                    break;
                case BACKEND:
                case BACKEND_CN:
                    Env.getCurrentSystemInfo().dropBackends(localHostInfo);
                    break;
                case BROKER:
                    List<Pair<String, Integer>> hostInfos = localHostInfo.stream().map(hostInfo
                            -> Pair.of(hostInfo.getHost(), hostInfo.getPort())).collect(Collectors.toList());
                    env.getBrokerMgr().dropBrokers(getBrokerName(), hostInfos);
                    break;
                default:
                    break;
            }
        } catch (DdlException e) {
            LOG.error("Failed to drop {} nodes.", nodeType, e);
        }

        LOG.info("Finished to drop {} nodes", nodeType);
    }

    private void dealAddRemote(List<HostInfo> localHostInfo, NodeType nodeType) {
        try {
            switch (nodeType) {
                case ELECTABLE:
                    env.addFrontend(FrontendNodeType.FOLLOWER, localHostInfo);
                    break;
                case OBSERVER:
                    env.addFrontend(FrontendNodeType.OBSERVER, localHostInfo);
                    break;
                case BACKEND:
                case BACKEND_CN:
                    Env.getCurrentSystemInfo().addBackends(localHostInfo, false);
                    break;
                case BROKER:
                    List<Pair<String, Integer>> hostInfos = localHostInfo.stream().map(hostInfo
                            -> Pair.of(hostInfo.getHost(), hostInfo.getPort())).collect(Collectors.toList());
                    env.getBrokerMgr().addBrokers(getBrokerName(), hostInfos);
                    break;
                default:
                    break;
            }
        } catch (UserException e) {
            LOG.error("Failed to add {} nodes", nodeType, e);
        }

        LOG.info("Finished to add {} nodes.", nodeType);
    }

    // Get host port pair from pair list. Return null if not found
    // when hostName,compare hostname,otherwise compare ip
    private HostInfo getFromHostInfos(List<HostInfo> hostInfos, HostInfo hostInfo) {
        for (HostInfo h : hostInfos) {
            if (hostInfo.getHost().equals(h.getHost()) && hostInfo.getPort() == (h.getPort())) {
                return hostInfo;
            }
        }
        return null;
    }

    private List<HostInfo> convertFesToHostInfos(List<Frontend> frontends) {
        List<HostInfo> hostPortPair = Lists.newArrayList();
        for (Frontend fe : frontends) {
            hostPortPair.add(convertToHostInfo(fe));
        }
        return hostPortPair;
    }

    private List<HostInfo> convertBrokersToHostInfos(List<FsBroker> brokers) {
        List<HostInfo> hostPortPair = Lists.newArrayList();
        for (FsBroker broker : brokers) {
            hostPortPair.add(convertToHostInfo(broker));
        }
        return hostPortPair;
    }

    private List<HostInfo> convertBesToHostInfos(List<Backend> backends) {
        List<HostInfo> hostPortPair = Lists.newArrayList();
        for (Backend fe : backends) {
            hostPortPair.add(convertToHostInfo(fe));
        }
        return hostPortPair;
    }

    private HostInfo convertToHostInfo(Frontend frontend) {
        return new HostInfo(frontend.getHost(), frontend.getEditLogPort());
    }

    private HostInfo convertToHostInfo(FsBroker broker) {
        return new HostInfo(broker.host, broker.port);
    }

    private HostInfo convertToHostInfo(Backend backend) {
        return new HostInfo(backend.getHost(), backend.getHeartbeatPort());
    }

    private boolean isSelf(HostInfo hostInfo) {
        if (env.getMasterHost().equals(hostInfo.getHost()) && Config.edit_log_port == hostInfo.getPort()) {
            return true;
        }
        return false;
    }

    protected class Event {
        private NodeType nodeType;
        private List<HostInfo> hostInfos;

        public Event(NodeType nodeType, List<HostInfo> hostInfos) {
            this.nodeType = nodeType;
            this.hostInfos = hostInfos;
        }

        public NodeType getNodeType() {
            return nodeType;
        }

        public List<HostInfo> getHostInfos() {
            return hostInfos;
        }

        @Override
        public String toString() {
            return "Event{"
                    + "nodeType=" + nodeType
                    + ", hostInfos=" + hostInfos
                    + '}';
        }
    }

    protected class NodeTypeAttr {
        private boolean hasService;
        private String serviceName;
        private String subAttr;

        public NodeTypeAttr(boolean hasService) {
            this.hasService = hasService;
        }

        public boolean hasService() {
            return hasService;
        }

        public void setHasService(boolean hasService) {
            this.hasService = hasService;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public String getSubAttr() {
            return subAttr;
        }

        public void setSubAttr(String subAttr) {
            this.subAttr = subAttr;
        }
    }
}
