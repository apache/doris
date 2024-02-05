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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.node;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.memo.Group;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * HyperGraph Node.
 */
public class DPhyperNode extends AbstractNode {

    private final Group group;

    public DPhyperNode(int index, Group group, List<Edge> edges) {
        super(group.getLogicalExpression().getPlan(), index, edges);
        Preconditions.checkArgument(group != null,
                "DPhyper requires Group is not null");
        this.group = group;
    }

    public DPhyperNode(int index, Group group) {
        this(index, group, new ArrayList<>());
    }

    public DPhyperNode withGroup(Group group) {
        return new DPhyperNode(index, group, getEdges());
    }

    public Group getGroup() {
        return group;
    }

    public double getRowCount() {
        return group.getStatistics().getRowCount();
    }
}
