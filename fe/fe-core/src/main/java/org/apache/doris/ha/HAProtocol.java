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
    // increase epoch number by one
    boolean fencing();

    // get observer nodes in the current group
    List<InetSocketAddress> getObserverNodes();

    // get replica nodes in the current group
    List<InetSocketAddress> getElectableNodes(boolean leaderIncluded);

    // get the leader of current group
    InetSocketAddress getLeader();

    // remove a node from the group
    boolean removeElectableNode(String nodeName);
}
