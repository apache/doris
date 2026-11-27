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

package org.apache.doris.ha;

import org.apache.doris.catalog.Env;
import org.apache.doris.journal.bdbje.BDBEnvironment;
import org.apache.doris.system.Frontend;

import com.google.common.collect.Lists;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class BDBHA implements HAProtocol {
    private static final Logger LOG = LogManager.getLogger(BDBHA.class);

    private final BDBEnvironment environment;
    private final String nodeName;
    private static final int RETRY_TIME = 3;

    // Unstable node is a follower node that is joining the cluster but have not
    // completed.
    // We should record this kind node and set the bdb electable group size to
    // (size_of_all_followers - size_of_unstable_nodes).
    // Because once the handshake is successful, the joined node is put into the
    // optional group,
    // but it may take a little time for this node to replicate the historical data.
    // This node will never respond to a new data replication until the historical
    // replication is completed,
    // and if the master cannot receive a quorum response, the write operation will
    // fail.
    private final Set<String> unReadyElectableNodes = new HashSet<>();

    public BDBHA(BDBEnvironment env, String nodeName) {
        this.environment = env;
        this.nodeName = nodeName;
    }

    @Override
    public boolean fencing() {
        Database epochDb = environment.getEpochDB();

        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                long count = epochDb.count();
                long myEpoch = count + 1;
                LOG.info("start fencing, epoch number is {}", myEpoch);
                Long key = myEpoch;
                DatabaseEntry theKey = new DatabaseEntry();
                TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
                idBinding.objectToEntry(key, theKey);
                DatabaseEntry theData = new DatabaseEntry(new byte[1]);
                OperationStatus status = epochDb.putNoOverwrite(null, theKey, theData);
                if (status == OperationStatus.SUCCESS) {
                    Env.getCurrentEnv().setEpoch(myEpoch);
                    return true;
                } else if (status == OperationStatus.KEYEXIST) {
                    return false;
                } else {
                    throw new Exception(status.toString());
                }
            } catch (Exception e) {
                LOG.warn("fencing failed. tried {} times", i, e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    LOG.warn("fencing sleep exception:", e1);
                }
            }
        }
        return false;
    }

    @Override
    public List<InetSocketAddress> getObserverNodes() {
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return ret;
        }

        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getSecondaryNodes()) {
                ret.add(replicationNode.getSocketAddress());
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getObserverNodes.", e);
            return Lists.newArrayList();
        } finally {
            replicationGroupAdmin.close();
        }
        return ret;
    }

    @Override
    public List<InetSocketAddress> getElectableNodes(boolean leaderIncluded) {
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return ret;
        }

        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getElectableNodes()) {
                if (leaderIncluded) {
                    ret.add(replicationNode.getSocketAddress());
                } else {
                    if (!replicationNode.getName()
                            .equals(replicationGroupAdmin.getMasterNodeName())) {
                        ret.add(replicationNode.getSocketAddress());
                    }
                }
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getElectableNodes.", e);
            return Lists.newArrayList();
        } finally {
            replicationGroupAdmin.close();
        }
        return ret;
    }

    /**
     * Hands mastership to the first of {@code replicas} that catches up with this master within the timeout,
     * and returns its node name. Commits on this master keep going while the replicas catch up, block once one of
     * them is nearly current, and fail after the handover, when this node is a replica.
     */
    public String transferMaster(Set<String> replicas, int timeoutSecond) {
        ReplicatedEnvironment replicatedEnvironment = environment.getReplicatedEnvironment();
        if (replicatedEnvironment == null) {
            throw new IllegalStateException("replicated environment is not open");
        }
        return replicatedEnvironment.transferMaster(replicas, timeoutSecond, TimeUnit.SECONDS);
    }

    /**
     * Waits up to timeoutMs until each of {@code followers} is a replica of {@code master}. Returns whether they
     * all are.
     */
    public boolean waitForReplicasOf(String master, Set<String> followers, long timeoutMs)
            throws InterruptedException {
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        while (true) {
            int joined = 0;
            try {
                ReplicationGroupAdmin admin = environment.getReplicationGroupAdmin();
                ReplicationGroup group = admin == null ? null : admin.getGroup();
                for (String name : followers) {
                    ReplicationNode node = group == null ? null : group.getMember(name);
                    if (node == null) {
                        continue;
                    }
                    NodeState state = admin.getNodeState(node, 1000);
                    if (state.getNodeState() == ReplicatedEnvironment.State.REPLICA
                            && master.equals(state.getMasterName())) {
                        joined++;
                    }
                }
            } catch (Exception e) {
                LOG.debug("failed to read the replication state of {}", followers, e);
            }
            if (joined == followers.size()) {
                return true;
            }
            if (System.currentTimeMillis() >= deadlineMs) {
                return false;
            }
            Thread.sleep(100);
        }
    }

    /**
     * Returns the node names of the alive followers other than {@code selfNodeName}: the nodes a master can
     * hand its mastership to.
     */
    public static Set<String> chooseMasterTransferTargets(Collection<Frontend> frontends, String selfNodeName) {
        Set<String> targets = new TreeSet<>();
        for (Frontend fe : frontends) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER && fe.isAlive()
                    && !fe.getNodeName().equals(selfNodeName)) {
                targets.add(fe.getNodeName());
            }
        }
        return targets;
    }

    /**
     * The followers a departing master hands its mastership to: those that answered a heartbeat just now, given as
     * node name to replayed journal id, and are within maxJournalLag of masterJournalId. Empty unless they form a
     * majority of all {@code electable} nodes, the departing master included.
     */
    public static Set<String> chooseHandoverTargets(int electable, Map<String, Long> replayedJournalIds,
            long masterJournalId, long maxJournalLag) {
        Set<String> targets = chooseReadyTransferTargets(replayedJournalIds, masterJournalId, maxJournalLag);
        if (!keepsQuorumWithout(electable, targets.size())) {
            LOG.warn("only {} of {} electable frontends besides this one are ready, no quorum without it,"
                    + " shut down as master", targets.size(), electable);
            return new TreeSet<>();
        }
        return targets;
    }

    /**
     * Whether the electable nodes other than the departing master, of which {@code answering} are reachable, still
     * form a majority of all {@code electable} nodes. Without one, the group can neither elect a master nor
     * acknowledge its journal commits.
     */
    public static boolean keepsQuorumWithout(int electable, int answering) {
        return answering >= electable / 2 + 1;
    }

    /**
     * Returns the followers ready to take mastership over: those that answered a heartbeat just now, given as
     * node name to replayed journal id, and whose replayed journal id is within maxJournalLag of masterJournalId.
     */
    public static Set<String> chooseReadyTransferTargets(Map<String, Long> replayedJournalIds, long masterJournalId,
            long maxJournalLag) {
        Set<String> targets = new TreeSet<>();
        for (Map.Entry<String, Long> entry : replayedJournalIds.entrySet()) {
            if (masterJournalId - entry.getValue() <= maxJournalLag) {
                targets.add(entry.getKey());
            }
        }
        return targets;
    }

    @Override
    public InetSocketAddress getLeader() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        try {
            String leaderName = replicationGroupAdmin.getMasterNodeName();
            ReplicationGroup rg = replicationGroupAdmin.getGroup();
            ReplicationNode rn = rg.getMember(leaderName);
            return rn.getSocketAddress();
        } finally {
            if (replicationGroupAdmin != null) {
                replicationGroupAdmin.close();
            }
        }
    }

    @Override
    public boolean removeElectableNode(String nodeName) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return false;
        }
        try {
            LOG.info("remove electable node: {}", nodeName);
            replicationGroupAdmin.removeMember(nodeName);
        } catch (MemberNotFoundException e) {
            LOG.warn("the electable node is not found {}", nodeName, e);
            return false;
        } catch (Exception e) {
            LOG.error("remove electable node {} meeting unkown exception:", nodeName, e);
            System.exit(-1);
        } finally {
            replicationGroupAdmin.close();
        }
        return true;
    }

    public boolean updateNodeAddress(String nodeName, String newHostName, int port) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return false;
        }
        try {
            LOG.info("update electable node {} with new host name: {}, port: {}", nodeName, newHostName, port);
            replicationGroupAdmin.updateAddress(nodeName, newHostName, port);
        } catch (MemberNotFoundException e) {
            LOG.error("the updating electable node is not found {}", nodeName, e);
            return false;
        } catch (MasterStateException e) {
            LOG.error("the updating electable node is master {}", nodeName, e);
            return false;
        } finally {
            replicationGroupAdmin.close();
        }
        return true;
    }

    public void removeConflictNodeIfExist(String host, int port) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return;
        }

        try {
            List<String> conflictNodes = Lists.newArrayList();
            Set<ReplicationNode> replicationNodes = replicationGroupAdmin.getGroup().getElectableNodes();
            for (ReplicationNode replicationNode : replicationNodes) {
                if (replicationNode.getHostName().equals(host) && replicationNode.getPort() == port) {
                    conflictNodes.add(replicationNode.getName());
                }
            }

            for (String conflictNode : conflictNodes) {
                removeElectableNode(conflictNode);
            }
        } finally {
            replicationGroupAdmin.close();
        }
    }

    public synchronized void addUnReadyElectableNode(String nodeName, int totalFollowerCount) {
        unReadyElectableNodes.add(nodeName);
        ReplicatedEnvironment replicatedEnvironment = environment.getReplicatedEnvironment();
        if (replicatedEnvironment != null) {
            int override = totalFollowerCount - unReadyElectableNodes.size();
            LOG.info("set electable group size override to {}, total follower count: {}, add unready node: {}",
                    override, totalFollowerCount, nodeName);
            replicatedEnvironment.setRepMutableConfig(new ReplicationMutableConfig()
                    .setElectableGroupSizeOverride(override));
        }
    }

    public synchronized void removeUnReadyElectableNode(String nodeName, int totalFollowerCount) {
        unReadyElectableNodes.remove(nodeName);
        ReplicatedEnvironment replicatedEnvironment = environment.getReplicatedEnvironment();
        if (replicatedEnvironment != null) {
            if (unReadyElectableNodes.isEmpty()) {
                // Setting ElectableGroupSizeOverride to 0 means remove this config,
                // and bdb will use the normal electable group size.
                LOG.info("remove electable group size override, total follower count: {}, remove unready node: {}",
                        totalFollowerCount, nodeName);
                replicatedEnvironment.setRepMutableConfig(
                        new ReplicationMutableConfig().setElectableGroupSizeOverride(0));
            } else {
                int override = totalFollowerCount - unReadyElectableNodes.size();
                LOG.info("set electable group size override to {}, total follower count: {}, remove unready node: {}",
                        override, totalFollowerCount, nodeName);
                replicatedEnvironment.setRepMutableConfig(new ReplicationMutableConfig()
                        .setElectableGroupSizeOverride(override));
            }
        }
    }

    public void removeDroppedMember(ConcurrentLinkedQueue<String> removedFrontends) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return;
        }
        try {
            Set<ReplicationNode> replicationNodes = replicationGroupAdmin.getGroup().getElectableNodes();
            LOG.debug("removedFrontends:{}", removedFrontends);
            for (ReplicationNode replicationNode : replicationNodes) {
                LOG.debug("node:{}", replicationNode.toString());
                if (removedFrontends.contains(replicationNode.getName())) {
                    try {
                        replicationGroupAdmin.removeMember(replicationNode.getName());
                    } catch (MemberNotFoundException e) {
                        LOG.warn("the electable node is not found {}", replicationNode.getName());
                    } catch (Exception e) {
                        LOG.error("remove electable node {} meeting unknown exception:", replicationNode.getName(), e);
                        System.exit(-1);
                    }
                }
            }
        } finally {
            replicationGroupAdmin.close();
        }
    }
}
