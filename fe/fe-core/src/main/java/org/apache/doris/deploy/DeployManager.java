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
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    public enum NodeType {
        ELECTABLE, OBSERVER, BACKEND, BROKER, BACKEND_CN
    }

    protected Env env;

    protected String electableFeServiceGroup;
    protected String observerFeServiceGroup;
    protected String backendServiceGroup;
    protected String brokerServiceGroup;
    protected String cnServiceGroup;

    protected boolean hasElectableService = false;
    protected boolean hasBackendService = false;
    protected boolean hasObserverService = false;
    protected boolean hasBrokerService = false;
    protected boolean hasCnService = false;

    // Host identifier -> missing counter
    // eg:
    // In k8s, when a node is down, the endpoint may be disappear immediately in service group.
    // But the k8s will start it up again soon.
    // In the gap between node's down and startup, the deploy manager may detect the missing node and
    // do the unnecessary dropping operations.
    // So we use this map to count the continuous detected down times, if the continuous down time is more
    // then MAX_MISSING_TIME, we considered this node as down permanently.
    protected Map<String, Integer> counterMap = Maps.newHashMap();
    // k8s pod delete and will recreate, so we need to wait for a while，otherwise we will drop node by mistake
    protected static final Integer MAX_MISSING_TIME = 60;

    public DeployManager(Env env, long intervalMs) {
        super("deployManager", intervalMs);
        this.env = env;
    }

    // Init all environment variables.
    // Derived Class can override this method to init more private env variables,
    // but must class the parent's init at first.
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup, String envCnServiceGroup) {

        this.electableFeServiceGroup = Strings.nullToEmpty(System.getenv(envElectableFeServiceGroup));
        this.observerFeServiceGroup = Strings.nullToEmpty(System.getenv(envObserverFeServiceGroup));
        this.backendServiceGroup = Strings.nullToEmpty(System.getenv(envBackendServiceGroup));
        this.brokerServiceGroup = Strings.nullToEmpty(System.getenv(envBrokerServiceGroup));
        this.cnServiceGroup = Strings.nullToEmpty(System.getenv(envCnServiceGroup));

        LOG.info("get deploy env: {}, {}, {}, {}, {}", envElectableFeServiceGroup, envObserverFeServiceGroup,
                envBackendServiceGroup, envBrokerServiceGroup, envCnServiceGroup);

        // check if we have electable service
        if (!Strings.isNullOrEmpty(electableFeServiceGroup)) {
            LOG.info("Electable service group is found");
            hasElectableService = true;
        }

        // check if we have backend service
        if (!Strings.isNullOrEmpty(backendServiceGroup)) {
            LOG.info("Backend service group is found");
            hasBackendService = true;
        }

        // check if we have observer service
        if (!Strings.isNullOrEmpty(observerFeServiceGroup)) {
            LOG.info("Observer service group is found");
            hasObserverService = true;
        }

        // check if we have broker service
        if (!Strings.isNullOrEmpty(brokerServiceGroup)) {
            LOG.info("Broker service group is found");
            hasBrokerService = true;
        }

        // check if we have cn service
        if (!Strings.isNullOrEmpty(cnServiceGroup)) {
            LOG.info("Cn service group is found");
            hasCnService = true;
        }

        LOG.info("get electableFeServiceGroup: {}, observerFeServiceGroup: {}, backendServiceGroup: {}"
                        + " brokerServiceGroup: {}, cnServiceGroup: {}",
                electableFeServiceGroup, observerFeServiceGroup, backendServiceGroup, brokerServiceGroup,
                cnServiceGroup);
    }

    // Call init before each runOneCycle
    // Default is do nothing. Can be override in derived class
    // return false if init failed.
    protected boolean init() {
        return true;
    }

    // get electable fe
    protected List<SystemInfoService.HostInfo> getElectableGroupHostInfos() {
        Preconditions.checkState(!Strings.isNullOrEmpty(electableFeServiceGroup));
        return getGroupHostInfos(electableFeServiceGroup);
    }

    // get observer fe
    protected List<SystemInfoService.HostInfo> getObserverGroupHostInfos() {
        Preconditions.checkState(!Strings.isNullOrEmpty(observerFeServiceGroup));
        return getGroupHostInfos(observerFeServiceGroup);
    }

    // get backend
    protected List<SystemInfoService.HostInfo> getBackendGroupHostInfos() {
        Preconditions.checkState(!Strings.isNullOrEmpty(backendServiceGroup));
        return getGroupHostInfos(backendServiceGroup);
    }

    // get cn
    protected List<SystemInfoService.HostInfo> getCnGroupHostInfos() {
        Preconditions.checkState(!Strings.isNullOrEmpty(cnServiceGroup));
        return getGroupHostInfos(cnServiceGroup);
    }

    // Get all host port pairs from specified group.
    // Must implement in derived class.
    // If encounter errors, return null
    protected List<SystemInfoService.HostInfo> getGroupHostInfos(String groupName) {
        throw new NotImplementedException();
    }

    // get broker
    // return (broker name -> list of broker host port)
    protected Map<String, List<SystemInfoService.HostInfo>> getBrokerGroupHostInfos() {
        throw new NotImplementedException();
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
                    helperNodes.add(SystemInfoService.getIpHostAndPort(host, true));
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
        List<SystemInfoService.HostInfo> feHostInfos = null;
        while (true) {
            try {
                feHostInfos = getElectableGroupHostInfos();
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
        return Lists.newArrayList(new HostInfo(feHostInfos.get(0).getIp(), feHostInfos.get(0).getHostName(),
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

        // 1. Check the electable fe service group
        if (hasElectableService) {
            List<SystemInfoService.HostInfo> remoteElectableFeHosts = getElectableGroupHostInfos();
            if (remoteElectableFeHosts == null) {
                return;
            }
            LOG.debug("get electable fe hosts {} from electable fe service group: {}",
                    remoteElectableFeHosts, electableFeServiceGroup);
            if (remoteElectableFeHosts.isEmpty()) {
                LOG.error("electable fe service group {} is empty, which should not happen", electableFeServiceGroup);
                return;
            }

            // 1.1 Check if self is in electable fe service group
            SystemInfoService.HostInfo selfHostInfo = getFromHostInfos(remoteElectableFeHosts,
                    new SystemInfoService.HostInfo(env.getMasterIp(), env.getMasterHostName(), Config.edit_log_port));
            if (selfHostInfo == null) {
                // The running of this deploy manager means this node is considered self as Master.
                // If it self does not exist in electable fe service group, it should shut it self down.
                LOG.warn("self host {} is not in electable fe service group {}. Exit now.",
                        env.getMasterIp(), electableFeServiceGroup);
                System.exit(-1);
            }

            // 1.2 Check the change of electable fe service group
            List<Frontend> localElectableFeAddrs = env.getFrontends(FrontendNodeType.FOLLOWER);
            List<SystemInfoService.HostInfo> localElectableFeHostInfos = this
                    .convertFesToHostInfos(localElectableFeAddrs);
            LOG.debug("get local electable hosts: {}", localElectableFeHostInfos);
            if (inspectNodeChange(remoteElectableFeHosts, localElectableFeHostInfos, NodeType.ELECTABLE)) {
                return;
            }
        }

        // 2. Check the backend service group
        if (hasBackendService) {
            BE_BLOCK: {
                List<SystemInfoService.HostInfo> remoteBackendHosts = getBackendGroupHostInfos();
                if (remoteBackendHosts == null) {
                    break BE_BLOCK;
                }
                LOG.debug("get remote backend hosts: {}", remoteBackendHosts);
                List<Backend> localBackends = Env.getCurrentSystemInfo()
                        .getClusterMixBackends(SystemInfoService.DEFAULT_CLUSTER);
                List<SystemInfoService.HostInfo> localBackendHostInfos = this.convertBesToHostInfos(localBackends);
                LOG.debug("get local backend addrs: {}", localBackendHostInfos);
                if (inspectNodeChange(remoteBackendHosts, localBackendHostInfos, NodeType.BACKEND)) {
                    return;
                }
            }
        }

        // 3. Check the cn service group
        if (hasCnService) {
            CN_BLOCK: {
                List<SystemInfoService.HostInfo> remoteCnHosts = getCnGroupHostInfos();
                if (remoteCnHosts == null) {
                    break CN_BLOCK;
                }
                LOG.debug("get remote cn hosts: {}", remoteCnHosts);
                List<Backend> localCns = Env.getCurrentSystemInfo()
                        .getClusterCnBackends(SystemInfoService.DEFAULT_CLUSTER);
                List<SystemInfoService.HostInfo> localCnHostInfos = this.convertBesToHostInfos(localCns);
                LOG.debug("get local cn addrs: {}", localCnHostInfos);
                if (inspectNodeChange(remoteCnHosts, localCnHostInfos, NodeType.BACKEND_CN)) {
                    return;
                }
            }
        }

        if (hasObserverService) {
            OB_BLOCK: {
                // 3. Check the observer fe service group
                List<SystemInfoService.HostInfo> remoteObserverFeHosts = getObserverGroupHostInfos();
                if (remoteObserverFeHosts == null) {
                    break OB_BLOCK;
                }
                LOG.debug("get remote observer fe hosts: {}", remoteObserverFeHosts);
                List<Frontend> localObserverFeAddrs = env.getFrontends(FrontendNodeType.OBSERVER);
                List<SystemInfoService.HostInfo> localObserverFeHosts = this
                        .convertFesToHostInfos(localObserverFeAddrs);
                LOG.debug("get local observer fe hosts: {}", localObserverFeHosts);
                if (inspectNodeChange(remoteObserverFeHosts, localObserverFeHosts, NodeType.OBSERVER)) {
                    return;
                }
            }
        }

        if (hasBrokerService) {
            BROKER_BLOCK: {
                // 4. Check the broker service group
                Map<String, List<SystemInfoService.HostInfo>> remoteBrokerHosts = getBrokerGroupHostInfos();
                if (remoteBrokerHosts == null) {
                    break BROKER_BLOCK;
                }

                Map<String, List<FsBroker>> localBrokers = env.getBrokerMgr().getBrokerListMap();

                // 1. find missing brokers
                for (Map.Entry<String, List<FsBroker>> entry : localBrokers.entrySet()) {
                    String brokerName = entry.getKey();
                    if (remoteBrokerHosts.containsKey(brokerName)) {
                        List<FsBroker> localList = entry.getValue();
                        List<SystemInfoService.HostInfo> remoteList = remoteBrokerHosts.get(brokerName);

                        // 1.1 found missing broker host
                        for (FsBroker addr : localList) {
                            SystemInfoService.HostInfo foundHost = getFromHostInfos(remoteList,
                                    new SystemInfoService.HostInfo(addr.ip, null, addr.port));
                            if (foundHost == null) {
                                List<Pair<String, Integer>> list = Lists.newArrayList();
                                list.add(Pair.of(addr.ip, addr.port));
                                try {
                                    env.getBrokerMgr().dropBrokers(brokerName, list);
                                    LOG.info("drop broker {}:{} with name: {}",
                                            addr.ip, addr.port, brokerName);
                                } catch (DdlException e) {
                                    LOG.warn("failed to drop broker {}:{} with name: {}",
                                            addr.ip, addr.port, brokerName, e);
                                    continue;
                                }
                            }
                        }

                        // 1.2 add new broker host
                        for (SystemInfoService.HostInfo pair : remoteList) {
                            FsBroker foundAddr = getHostFromBrokerAddrs(localList, pair.getIp(), pair.getPort());
                            if (foundAddr == null) {
                                // add new broker
                                List<Pair<String, Integer>> list = Lists.newArrayList();
                                list.add(Pair.of(pair.getIp(), pair.getPort()));
                                try {
                                    env.getBrokerMgr().addBrokers(brokerName, list);
                                    LOG.info("add broker {}:{} with name {}", pair.getIp(), pair.getPort(), brokerName);
                                } catch (DdlException e) {
                                    LOG.warn("failed to add broker {}:{} with name {}",
                                            pair.getIp(), pair.getPort(), brokerName);
                                    continue;
                                }
                            }
                        }

                    } else {
                        // broker with this name does not exist in remote. drop all
                        try {
                            env.getBrokerMgr().dropAllBroker(brokerName);
                            LOG.info("drop all brokers with name: {}", brokerName);
                        } catch (DdlException e) {
                            LOG.warn("failed to drop all brokers with name: {}", brokerName, e);
                            continue;
                        }
                    }
                } // end for

                // 2. add new brokers
                for (Map.Entry<String, List<SystemInfoService.HostInfo>> entry : remoteBrokerHosts.entrySet()) {
                    String remoteBrokerName = entry.getKey();
                    if (!localBrokers.containsKey(remoteBrokerName)) {
                        // add new brokers
                        try {
                            env.getBrokerMgr()
                                    .addBrokers(remoteBrokerName, convertHostInfosToIpPortPair(entry.getValue()));
                            LOG.info("add brokers {} with name {}", entry.getValue(), remoteBrokerName);
                        } catch (DdlException e) {
                            LOG.info("failed to add brokers {} with name {}",
                                    entry.getValue(), remoteBrokerName, e);
                            continue;
                        }
                    }
                }
            } // end of BROKER BLOCK
        }
    }

    private FsBroker getHostFromBrokerAddrs(List<FsBroker> addrList,
            String ip, Integer port) {
        for (FsBroker brokerAddress : addrList) {
            if (brokerAddress.ip.equals(ip) && brokerAddress.port == port) {
                return brokerAddress;
            }
        }
        return null;
    }

    /*
     * Inspect the node change.
     * 1. Check if there are some nodes need to be dropped.
     * 2. Check if there are some nodes need to be added.
     *
     * We only handle one change at a time.
     * Return true if something changed
     */
    private boolean inspectNodeChange(List<SystemInfoService.HostInfo> remoteHostInfos,
            List<SystemInfoService.HostInfo> localHostInfos,
            NodeType nodeType) {

        // 2.1 Find local node which need to be dropped.
        for (SystemInfoService.HostInfo localHostInfo : localHostInfos) {
            String localIp = localHostInfo.getIp();
            Integer localPort = localHostInfo.getPort();
            String localHostName = localHostInfo.getHostName();
            SystemInfoService.HostInfo foundHostInfo = getFromHostInfos(remoteHostInfos, localHostInfo);
            if (foundHostInfo != null) {
                if (counterMap.containsKey(localHostInfo.getIdent())) {
                    counterMap.remove(localHostInfo.getIdent());
                }
            } else {
                // Double check if is it self
                if (isSelf(localHostInfo)) {
                    // This is it self. Shut down now.
                    LOG.error("self host {}:{} does not exist in remote hosts. Showdown.");
                    System.exit(-1);
                }

                // Check the detected downtime
                if (!counterMap.containsKey(localHostInfo.getIdent())) {
                    // First detected downtime. Add to the map and ignore
                    LOG.warn("downtime of {} node: {} detected times: 1",
                            nodeType.name(), localHostInfo);
                    counterMap.put(localHostInfo.getIdent(), 1);
                    return false;
                } else {
                    int times = counterMap.get(localHostInfo.getIdent());
                    if (times < MAX_MISSING_TIME) {
                        LOG.warn("downtime of {} node: {} detected times: {}",
                                nodeType.name(), localHostInfo, times + 1);
                        counterMap.put(localHostInfo.getIdent(), times + 1);
                        return false;
                    } else {
                        // Reset the counter map and do the dropping operation
                        LOG.warn("downtime of {} node: {} detected times: {}. drop it",
                                nodeType.name(), localHostInfo, times + 1);
                        counterMap.remove(localHostInfo.getIdent());
                    }
                }

                // Can not find local host from remote host list,
                // which means this node should be dropped.
                try {
                    switch (nodeType) {
                        case ELECTABLE:
                            env.dropFrontend(FrontendNodeType.FOLLOWER, localIp, localHostName, localPort);
                            break;
                        case OBSERVER:
                            env.dropFrontend(FrontendNodeType.OBSERVER, localIp, localHostName, localPort);
                            break;
                        case BACKEND:
                        case BACKEND_CN:
                            Env.getCurrentSystemInfo().dropBackend(localIp, localHostName, localPort);
                            break;
                        default:
                            break;
                    }
                } catch (DdlException e) {
                    LOG.error("Failed to drop {} node: {}:{}", nodeType, localIp, localPort, e);
                    // return true is a conservative behavior. we do not expect any exception here.
                    return true;
                }

                LOG.info("Finished to drop {} node: {}:{}", nodeType, localIp, localPort);
                return true;
            }
        }

        // 2.2. Find remote node which need to be added.
        for (SystemInfoService.HostInfo remoteHostInfo : remoteHostInfos) {
            String remoteIp = remoteHostInfo.getIp();
            Integer remotePort = remoteHostInfo.getPort();
            String remoteHostName = remoteHostInfo.getHostName();
            SystemInfoService.HostInfo foundHostInfo = getFromHostInfos(localHostInfos, remoteHostInfo);
            if (foundHostInfo == null) {
                // Can not find remote host in local hosts,
                // which means this remote host need to be added.
                try {
                    switch (nodeType) {
                        case ELECTABLE:
                            env.addFrontend(FrontendNodeType.FOLLOWER, remoteIp, remoteHostName, remotePort);
                            break;
                        case OBSERVER:
                            env.addFrontend(FrontendNodeType.OBSERVER, remoteIp, remoteHostName, remotePort);
                            break;
                        case BACKEND:
                        case BACKEND_CN:
                            List<SystemInfoService.HostInfo> newBackends = Lists.newArrayList();
                            newBackends.add(new SystemInfoService.HostInfo(remoteIp, remoteHostName, remotePort));
                            Env.getCurrentSystemInfo().addBackends(newBackends, false);
                            break;
                        default:
                            break;
                    }
                } catch (UserException e) {
                    LOG.error("Failed to add {} node: {}:{}", nodeType, remoteIp, remotePort, e);
                    return true;
                }

                LOG.info("Finished to add {} node: {}:{}", nodeType, remoteIp, remotePort);
                return true;
            }
        }

        return false;
    }

    private List<Pair<String, Integer>> convertHostInfosToIpPortPair(List<SystemInfoService.HostInfo> hostInfos) {
        ArrayList<Pair<String, Integer>> pairs = Lists.newArrayList();
        for (SystemInfoService.HostInfo e : hostInfos) {
            pairs.add(Pair.of(e.getIp(), e.getPort()));
        }
        return pairs;
    }

    // Get host port pair from pair list. Return null if not found
    // when hostName,compare hostname,otherwise compare ip
    private SystemInfoService.HostInfo getFromHostInfos(List<SystemInfoService.HostInfo> hostInfos,
            SystemInfoService.HostInfo hostInfo) {
        for (SystemInfoService.HostInfo h : hostInfos) {
            if (StringUtils.isEmpty(hostInfo.hostName) || StringUtils.isEmpty(h.hostName)) {
                if (hostInfo.getIp().equals(h.getIp()) && hostInfo.getPort() == (h.getPort())) {
                    return hostInfo;
                }
            } else {
                if (hostInfo.getHostName().equals(h.getHostName()) && hostInfo.getPort() == (h.getPort())) {
                    return hostInfo;
                }
            }

        }
        return null;
    }

    private List<SystemInfoService.HostInfo> convertFesToHostInfos(List<Frontend> frontends) {
        List<SystemInfoService.HostInfo> hostPortPair = Lists.newArrayList();
        for (Frontend fe : frontends) {
            hostPortPair.add(convertToHostInfo(fe));
        }
        return hostPortPair;
    }

    private List<SystemInfoService.HostInfo> convertBesToHostInfos(List<Backend> backends) {
        List<SystemInfoService.HostInfo> hostPortPair = Lists.newArrayList();
        for (Backend fe : backends) {
            hostPortPair.add(convertToHostInfo(fe));
        }
        return hostPortPair;
    }

    private SystemInfoService.HostInfo convertToHostInfo(Frontend frontend) {
        return new SystemInfoService.HostInfo(frontend.getIp(), frontend.getHostName(), frontend.getEditLogPort());
    }

    private SystemInfoService.HostInfo convertToHostInfo(Backend backend) {
        return new SystemInfoService.HostInfo(backend.getIp(), backend.getHostName(), backend.getHeartbeatPort());
    }

    private boolean isSelf(SystemInfoService.HostInfo hostInfo) {
        if (Config.edit_log_port == hostInfo.getPort()) {
            // master host name may not same as local host name, so we should compare ip here
            if (env.getMasterHostName() != null && env.getMasterHostName().equals(hostInfo.getHostName())) {
                return true;
            }
            if (env.getMasterIp().equals(hostInfo.getIp())) {
                return true;
            }
        }
        return false;
    }
}
