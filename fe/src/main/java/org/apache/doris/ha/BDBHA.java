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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.journal.bdbje.BDBEnvironment;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BDBHA implements HAProtocol {
    private static final Logger LOG = LogManager.getLogger(BDBHA.class);
    
    private BDBEnvironment environment;
    private String nodeName;
    private static final int RETRY_TIME = 3;
    
    public BDBHA(BDBEnvironment env, String nodeName) {
        this.environment = env;
        this.nodeName = nodeName;
    }

    @Override
    public long getEpochNumber() {
        return 0;
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
                    Catalog.getCurrentCatalog().setEpoch(myEpoch);
                    return true;
                } else if (status == OperationStatus.KEYEXIST) {
                    return false;
                } else {
                    throw new Exception(status.toString());
                }
            } catch (Exception e) {
                LOG.error("fencing failed. tried {} times", i, e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return false;
    }

    @Override
    public List<InetSocketAddress> getObserverNodes() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return null;
        }
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getSecondaryNodes()) {
                ret.add(replicationNode.getSocketAddress());
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getObserverNodes.", e);
            return Lists.newArrayList();
        }
        return ret;
    }

    @Override
    public List<InetSocketAddress> getElectableNodes(boolean leaderIncluded) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return null;
        }
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getElectableNodes()) {
                if (leaderIncluded) {
                    ret.add(replicationNode.getSocketAddress());
                } else {
                    if (!replicationNode.getName().equals(replicationGroupAdmin.getMasterNodeName())) {
                        ret.add(replicationNode.getSocketAddress());
                    }
                }
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getElectableNodes.", e);
            return Lists.newArrayList();
        }
        return ret;
    }

    @Override
    public InetSocketAddress getLeader() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        String leaderName = replicationGroupAdmin.getMasterNodeName();
        ReplicationGroup rg = replicationGroupAdmin.getGroup();
        ReplicationNode rn = rg.getMember(leaderName);
        return rn.getSocketAddress();
    }

    @Override
    public List<InetSocketAddress> getNoneLeaderNodes() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return null;
        }
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getSecondaryNodes()) {
                ret.add(replicationNode.getSocketAddress());
            }
            for (ReplicationNode replicationNode : replicationGroup.getElectableNodes()) {
                if (!replicationNode.getName().equals(replicationGroupAdmin.getMasterNodeName())) {
                    ret.add(replicationNode.getSocketAddress());
                }
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getNoneLeaderNodes.", e);
            return null;
        }
        return ret;
    }

    @Override
    public void transferToMaster() {
        
    }
    
    @Override
    public void transferToNonMaster() {
        
    }

    @Override
    public boolean isLeader() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        String leaderName = replicationGroupAdmin.getMasterNodeName();
        return leaderName.equals(nodeName);
    }

    @Override
    public boolean removeElectableNode(String nodeName) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return false;
        }
        try {
            replicationGroupAdmin.removeMember(nodeName);
        } catch (MemberNotFoundException e) {
            LOG.error("the deleting electable node is not found {}", nodeName, e);
            return false;
        } catch (MasterStateException e) {
            LOG.error("the deleting electable node is master {}", nodeName, e);
            return false;
        }
        return true;
    }

    // When new Follower FE is added to the cluster, it should also be added to the helper sockets in
    // ReplicationGroupAdmin, in order to fix the following case:
    // 1. A Observer starts with helper of master FE.
    // 2. Master FE is dead, new Master is elected.
    // 3. Observer's helper sockets only contains the info of the dead master FE.
    //    So when you try to get frontends' info from this Observer, it will throw the Exception:
    //    "Could not determine master from helpers at:[/dead master FE host:port]"
    public void addHelperSocket(String ip, Integer port) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        Set<InetSocketAddress> helperSockets = Sets.newHashSet(replicationGroupAdmin.getHelperSockets());
        InetSocketAddress newHelperSocket =  new InetSocketAddress(ip,port);
        if (!helperSockets.contains(newHelperSocket)) {
            helperSockets.add(newHelperSocket);
            environment.setNewReplicationGroupAdmin(helperSockets);
            LOG.info("add {}:{} to helper sockets", ip, port);
        }
    }
}
