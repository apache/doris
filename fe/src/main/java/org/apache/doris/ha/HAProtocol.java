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

import java.net.InetSocketAddress;
import java.util.List;

public interface HAProtocol {
    // get current epoch number
    public long getEpochNumber();
    
    // increase epoch number by one
    public boolean fencing();
    
    // get observer nodes in the current group
    public List<InetSocketAddress> getObserverNodes();
    
    // get replica nodes in the current group
    public List<InetSocketAddress> getElectableNodes(boolean leaderIncluded);
    
    // get the leader of current group
    public InetSocketAddress getLeader();
    
    // get all the nodes except leader in the current group
    public List<InetSocketAddress> getNoneLeaderNodes();
    
    // transfer from nonMaster(unknown, follower or init) to master
    public void transferToMaster();
    
    // transfer to non-master
    public void transferToNonMaster();
    
    // check if the current node is leader
    public boolean isLeader();
    
    // remove a node from the group
    public boolean removeElectableNode(String nodeName);
}
