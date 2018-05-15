package com.baidu.palo.deploy;

import com.baidu.palo.catalog.BrokerMgr.BrokerAddress;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.ha.FrontendNodeType;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.Frontend;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
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
 * FE_EXIST_ENTPOINT:
 *      he existing FE(Master + Follower) before the new FE start up.
 *      The main reason of this var is to indicate whether there is already an alive Master 
 *      or the consensus of who is master is needed.
 *      
 * FE_INIT_NUMBER:
 *      Number of newly start up FE(Master + Follower), can only be 1 or 3.
 * 
 * Only one of FE_EXIST_ENTPOINT and FE_INIT_NUMBER need to be set.
 * 
 * eg:
 * 
 *  1. Start 1 FE as a single Master
 *      set FE_EXIST_ENTPOINT as empty
 *      set FE_INIT_NUMBER = 1
 *      
 *  2. Start 3 FE(Master + Follower)
 *      set FE_EXIST_ENTPOINT as empty
 *      set FE_INIT_NUMBER = 3
 *      
 *  3. With 1 existing FE(Master), add 2 FEs to reach HA.
 *      set FE_EXIST_ENTPOINT=existing_fe_host:edit_log_port
 *      set FE_INIT_NUMBER as empty
 * 
 */
public class DeployManager extends Daemon {
    private static final Logger LOG = LogManager.getLogger(DeployManager.class);
    
    public static final String ENV_FE_EXIST_ENTPOINT = "FE_EXIST_ENTPOINT";
    public static final String ENV_FE_INIT_NUMBER = "FE_INIT_NUMBER";

    public enum NodeType {
        ELECTABLE, OBSERVER, BACKEND
    }

    protected Catalog catalog;

    protected String electableFeServiceGroup;
    protected String observerFeServiceGroup;
    protected String backendServiceGroup;
    protected String brokerServiceGroup;

    protected boolean hasObserverService = false;
    protected boolean hasBrokerService = false;

    // Host identifier -> missing counter
    // eg:
    // In k8s, when a node is down, the endpoint may be disappear immediately in service group.
    // But the k8s will start it up again soon.
    // In the gap between node's down and startup, the deploy manager may detect the missing node and
    // do the unnecessary dropping operations.
    // So we use this map to count the continuous detected down times, if the continuous down time is more
    // then MAX_MISSING_TIME, we considered this node as down permanently.
    protected Map<String, Integer> counterMap = Maps.newHashMap();
    protected static final Integer MAX_MISSING_TIME = 3;

    public DeployManager(Catalog catalog, long intervalMs) {
        super("deplotManager", intervalMs);
        this.catalog = catalog;
    }

    // Init all environment variables.
    // Derived Class can override this method to init more private env variables,
    // but must class the parent's init at first.
    protected void initEnvVariables(String envElectableFeServiceGroup, String envObserverFeServiceGroup,
            String envBackendServiceGroup, String envBrokerServiceGroup) {

        this.electableFeServiceGroup = Strings.nullToEmpty(System.getenv(envElectableFeServiceGroup));
        this.observerFeServiceGroup = Strings.nullToEmpty(System.getenv(envObserverFeServiceGroup));
        this.backendServiceGroup = Strings.nullToEmpty(System.getenv(envBackendServiceGroup));
        this.brokerServiceGroup = Strings.nullToEmpty(System.getenv(envBrokerServiceGroup));

        LOG.info("get deploy env: {}, {}, {}, {}", envElectableFeServiceGroup, envObserverFeServiceGroup,
                 envBackendServiceGroup, envBrokerServiceGroup);

        // electableFeServiceGroup and backendServiceGroup must exist
        if (Strings.isNullOrEmpty(electableFeServiceGroup) || Strings.isNullOrEmpty(backendServiceGroup)) {
            LOG.warn("failed to init service group name."
                    + " electableFeServiceGroup: {}, backendServiceGroup: {}",
                     electableFeServiceGroup, backendServiceGroup);
            System.exit(-1);
        }

        // check if we have observer and broker service
        if (!Strings.isNullOrEmpty(observerFeServiceGroup)) {
            LOG.info("Observer service group is found");
            hasObserverService = true;
        }

        if (!Strings.isNullOrEmpty(brokerServiceGroup)) {
            LOG.info("Broker service group is found");
            hasBrokerService = true;
        }

        LOG.info("get electableFeServiceGroup: {}, observerFeServiceGroup: {}, backendServiceGroup: {}"
                + " brokerServiceGroup: {}",
                 electableFeServiceGroup, observerFeServiceGroup, backendServiceGroup, brokerServiceGroup);
    }

    // Call init before each runOneCycle
    // Default is do nothing. Can be override in derived class
    // return false if init failed.
    protected boolean init() {
        return true;
    }

    // get electable fe
    protected List<Pair<String, Integer>> getElectableGroupHostPorts() {
        Preconditions.checkState(!Strings.isNullOrEmpty(electableFeServiceGroup));
        return getGroupHostPorts(electableFeServiceGroup);
    }

    // get observer fe
    protected List<Pair<String, Integer>> getObserverGroupHostPorts() {
        Preconditions.checkState(!Strings.isNullOrEmpty(observerFeServiceGroup));
        return getGroupHostPorts(observerFeServiceGroup);
    }

    // get backend
    protected List<Pair<String, Integer>> getBackendGroupHostPorts() {
        Preconditions.checkState(!Strings.isNullOrEmpty(backendServiceGroup));
        return getGroupHostPorts(backendServiceGroup);
    }

    // Get all host port pairs from specified group.
    // Must implement in derived class.
    // If encounter errors, return null
    protected List<Pair<String, Integer>> getGroupHostPorts(String groupName) {
        throw new NotImplementedException();
    }

    // get broker
    // return (broker name -> list of broker host port)
    protected Map<String, List<Pair<String, Integer>>> getBrokerGroupHostPorts() {
        throw new NotImplementedException();
    }

    public Pair<String, Integer> getHelperNode() {
        final String existFeHosts = System.getenv(ENV_FE_EXIST_ENTPOINT);
        if (!Strings.isNullOrEmpty(existFeHosts)) {
            // Some Frontends already exist in service group.
            // Arbitrarily choose the first one as helper node to start up
            String[] splittedHosts = existFeHosts.split(",");
            String[] splittedHostPort = splittedHosts[0].split(":");
            if (splittedHostPort.length != 2) {
                LOG.error("Invalid exist fe hosts: {}. will exit", existFeHosts);
                System.exit(-1);
            }
            Integer port = -1;
            try {
                port = Integer.valueOf(splittedHostPort[1]);
            } catch (NumberFormatException e) {
                LOG.error("Invalid exist fe hosts: {}. will exit", existFeHosts);
                System.exit(-1);
            }
            return Pair.create(splittedHostPort[0], port);
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
        List<Pair<String, Integer>> feHostPorts = null;
        while(true) {
            try {
                feHostPorts = getElectableGroupHostPorts();
                if (feHostPorts == null) {
                    ok = false;
                } else if (feHostPorts.size() != numOfFe) {
                    LOG.error("num of fe get from remote [{}] does not equal to the expected num: {}",
                              feHostPorts, numOfFe);
                    ok = false;
                } else {
                    ok = true;
                }
            } catch (Exception e) {
                LOG.error("failed to get elecetable fe hosts from remote.", e);
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
            
            LOG.info("get electable fe host from remote: {}", feHostPorts);
            break;
        }

        // 3. sort fe host list
        Collections.sort(feHostPorts, new PairComparator<>());
        LOG.info("sorted fe host list: {}", feHostPorts);

        // 4. return the first one as helper
        return feHostPorts.get(0);
    }

    @Override
    protected void runOneCycle() {
        if (Config.enable_deploy_manager.equals("disable")) {
            LOG.warn("Config enable_deploy_manager is disable. Exit deploy manager");
            exit();
            return;
        }

        if (!catalog.isMaster()) {
            LOG.warn("This is not the Master FE. Exit deply manager");
            exit();
            return;
        }

        // 0. init
        if (!init()) {
            return;
        }

        // 1. Check the electable fe service group
        List<Pair<String, Integer>> remoteElectableFeHosts = getElectableGroupHostPorts();
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
        Pair<String, Integer> selfHost = getHostFromPairList(remoteElectableFeHosts, catalog.getMasterIp(),
                                                             Config.edit_log_port);
        if (selfHost == null) {
            // The running of this deploy manager means this node is considered self as Master.
            // If it self does not exist in electable fe service group, it should shut it self down.
            LOG.warn("Self host {} is not in electable fe service group {}. Exit now.",
                     selfHost, electableFeServiceGroup);
            System.exit(-1);
        }

        // 1.2 Check the change of electable fe service group
        List<Frontend> localElectableFeAddrs = catalog.getFrontends(FrontendNodeType.FOLLOWER);
        List<Pair<String, Integer>> localElectableFeHosts = convertToHostPortPair(localElectableFeAddrs);
        LOG.debug("get local electable hosts: {}", localElectableFeHosts);
        inspectNodeChange(remoteElectableFeHosts, localElectableFeHosts, NodeType.ELECTABLE);

        // 2. Check the backend service group
        BE_BLOCK: {
            List<Pair<String, Integer>> remoteBackendHosts = getBackendGroupHostPorts();
            if (remoteBackendHosts == null) {
                break BE_BLOCK;
            }
            LOG.debug("get remote backend hosts: {}", remoteBackendHosts);
            List<Backend> localBackends = Catalog.getCurrentSystemInfo().getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
            List<Pair<String, Integer>> localBackendHosts = Lists.newArrayList();
            for (Backend backend : localBackends) {
                localBackendHosts.add(Pair.create(backend.getHost(), backend.getHeartbeatPort()));
            }
            LOG.debug("get local backend addrs: {}", localBackendHosts);
            inspectNodeChange(remoteBackendHosts, localBackendHosts, NodeType.BACKEND);
        }

        if (hasObserverService) {
            OB_BLOCK: {
                // 3. Check the observer fe service group
                List<Pair<String, Integer>> remoteObserverFeHosts = getObserverGroupHostPorts();
                if (remoteObserverFeHosts == null) {
                    break OB_BLOCK;
                }
                LOG.debug("get remote observer fe hosts: {}", remoteObserverFeHosts);
                List<Frontend> localObserverFeAddrs = catalog.getFrontends(FrontendNodeType.OBSERVER);
                List<Pair<String, Integer>> localObserverFeHosts = convertToHostPortPair(localObserverFeAddrs);
                LOG.debug("get local observer fe hosts: {}", localObserverFeHosts);
                inspectNodeChange(remoteObserverFeHosts, localObserverFeHosts, NodeType.OBSERVER);
            }
        }

        if (hasBrokerService) {
            BROKER_BLOCK: {
                // 4. Check the broker service group
                Map<String, List<Pair<String, Integer>>> remoteBrokerHosts = getBrokerGroupHostPorts();
                if (remoteBrokerHosts == null) {
                    break BROKER_BLOCK;
                }

                Map<String, List<BrokerAddress>> localBrokers = catalog.getBrokerMgr().getAddressListMap();

                // 1. find missing brokers
                for (Map.Entry<String, List<BrokerAddress>> entry : localBrokers.entrySet()) {
                    String brokerName = entry.getKey();
                    if (remoteBrokerHosts.containsKey(brokerName)) {
                        List<BrokerAddress> localList = entry.getValue();
                        List<Pair<String, Integer>> remoteList = remoteBrokerHosts.get(brokerName);

                        // 1.1 found missing broker host
                        for (BrokerAddress addr : localList) {
                            Pair<String, Integer> foundHost = getHostFromPairList(remoteList, addr.ip, addr.port);
                            if (foundHost == null) {
                                List<Pair<String, Integer>> list = Lists.newArrayList();
                                list.add(Pair.create(addr.ip, addr.port));
                                try {
                                    catalog.getBrokerMgr().dropBrokers(brokerName, list);
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
                        for (Pair<String, Integer> pair : remoteList) {
                            BrokerAddress foundAddr = getHostFromBrokerAddrs(localList, pair.first, pair.second);
                            if (foundAddr == null) {
                                // add new broker
                                List<Pair<String, Integer>> list = Lists.newArrayList();
                                list.add(Pair.create(pair.first, pair.second));
                                try {
                                    catalog.getBrokerMgr().addBrokers(brokerName, list);
                                    LOG.info("add broker {}:{} with name {}", pair.first, pair.second, brokerName);
                                } catch (DdlException e) {
                                    LOG.warn("failed to add broker {}:{} with name {}",
                                             pair.first, pair.second, brokerName);
                                    continue;
                                }
                            }
                        }

                    } else {
                        // broker with this name does not exist in remote. drop all
                        try {
                            catalog.getBrokerMgr().dropAllBroker(brokerName);
                            LOG.info("drop all brokers with name: {}", brokerName);
                        } catch (DdlException e) {
                            LOG.warn("failed to drop all brokers with name: {}", brokerName, e);
                            continue;
                        }
                    }
                } // end for

                // 2. add new brokers
                for (Map.Entry<String, List<Pair<String, Integer>>> entry : remoteBrokerHosts.entrySet()) {
                    String remoteBrokerName = entry.getKey();
                    if (!localBrokers.containsKey(remoteBrokerName)) {
                        // add new brokers
                        try {
                            catalog.getBrokerMgr().addBrokers(remoteBrokerName, entry.getValue());
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

    private BrokerAddress getHostFromBrokerAddrs(List<BrokerAddress> addrList,
            String ip, Integer port) {
        for (BrokerAddress brokerAddress : addrList) {
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
     * We only handle one change at a time
     */
    private void inspectNodeChange(List<Pair<String, Integer>> remoteHosts,
            List<Pair<String, Integer>> localHosts,
            NodeType nodeType) {

        // 2.1 Find local node which need to be dropped.
        for (Pair<String, Integer> localHost : localHosts) {
            String localIp = localHost.first;
            Integer localPort = localHost.second;
            Pair<String, Integer> foundHost = getHostFromPairList(remoteHosts, localIp, localPort);
            if (foundHost == null) {
                // Double check if is it self
                if (isSelf(localIp, localPort)) {
                    // This is it self. Shut down now.
                    LOG.error("Self host {}:{} does not exist in remote hosts. Showdown.");
                    System.exit(-1);
                }
                
                // Check the detected downtime
                if (!counterMap.containsKey(localHost.toString())) {
                    // First detected downtime. Add to the map and ignore
                    LOG.warn("downtime of {} node: {} detected times: 1",
                             nodeType.name(), localHost);
                    counterMap.put(localHost.toString(), 1);
                    return;
                } else {
                    int times = counterMap.get(localHost.toString());
                    if (times < MAX_MISSING_TIME) {
                        LOG.warn("downtime of {} node: {} detected times: {}",
                                 nodeType.name(), localHost, times + 1);
                        counterMap.put(localHost.toString(), times + 1);
                        return;
                    } else {
                        // Reset the counter map and do the dropping operation
                        LOG.warn("downtime of {} node: {} detected times: {}. drop it",
                                 nodeType.name(), localHost, times + 1);
                        counterMap.remove(localHost.toString());
                    }
                }

                // Can not find local host from remote host list,
                // which means this node should be dropped.
                try {
                    switch (nodeType) {
                        case ELECTABLE:
                            catalog.dropFrontend(FrontendNodeType.FOLLOWER, localIp, localPort);
                            break;
                        case OBSERVER:
                            catalog.dropFrontend(FrontendNodeType.OBSERVER, localIp, localPort);
                            break;
                        case BACKEND:
                            Catalog.getCurrentSystemInfo().dropBackend(localIp, localPort);
                            break;
                        default:
                            break;
                    }
                } catch (DdlException e) {
                    LOG.error("Failed to drop {} node: {}:{}", nodeType, localIp, localPort, e);
                    return;
                }

                LOG.info("Finished to drop {} node: {}:{}", nodeType, localIp, localPort);
                return;
            }
        }

        // 2.2. Find remote node which need to be added.
        for (Pair<String, Integer> remoteHost : remoteHosts) {
            String remoteIp = remoteHost.first;
            Integer remotePort = remoteHost.second;
            Pair<String, Integer> foundHost = getHostFromPairList(localHosts, remoteIp, remotePort);
            if (foundHost == null) {
                // Can not find remote host in local hosts,
                // which means this remote host need to be added.
                try {
                    switch (nodeType) {
                        case ELECTABLE:
                            catalog.addFrontend(FrontendNodeType.FOLLOWER, remoteIp, remotePort);
                            break;
                        case OBSERVER:
                            catalog.addFrontend(FrontendNodeType.OBSERVER, remoteIp, remotePort);
                            break;
                        case BACKEND:
                            List<Pair<String, Integer>> newBackends = Lists.newArrayList();
                            newBackends.add(Pair.create(remoteIp, remotePort));
                            Catalog.getCurrentSystemInfo().addBackends(newBackends, false);
                            break;
                        default:
                            break;
                    }
                } catch (DdlException e) {
                    LOG.error("Failed to add {} node: {}:{}", nodeType, remoteIp, remotePort, e);
                    return;
                }

                LOG.info("Finished to add {} node: {}:{}", nodeType, remoteIp, remotePort);
                return;
            }
        }
    }

    // Get host port pair from pair list. Return null if not found
    private Pair<String, Integer> getHostFromPairList(List<Pair<String, Integer>> pairList,
            String ip, Integer port) {
        for (Pair<String, Integer> pair : pairList) {
            if (ip.equals(pair.first) && port.equals(pair.second)) {
                return pair;
            }
        }
        return null;
    }

    private List<Pair<String, Integer>> convertToHostPortPair(List<Frontend> frontends) {
        List<Pair<String, Integer>> hostPortPair = Lists.newArrayList();
        for (Frontend fe : frontends) {
            hostPortPair.add(Pair.create(fe.getHost(), fe.getPort()));
        }
        return hostPortPair;
    }

    private boolean isSelf(String ip, Integer port) {
        if (catalog.getMasterIp() == ip && Config.edit_log_port == port) {
            return true;
        }
        return false;
    }

    private static class PairComparator<T extends Pair<String, Integer>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            int res = o1.first.compareTo(o2.first);
            if (res == 0) {
                return o1.second.compareTo(o2.second);
            }
            return res;
        }
    }
}
