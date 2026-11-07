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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPaloNodesInfo;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MaterializationNodeTest {
    @Test
    void selectsBackendsFromExplicitComputeGroupWithoutSession() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        try {
            Backend alive = new Backend(1L, "127.0.0.1", 9050);
            alive.setAlive(true);
            alive.setBrpcPort(8060);
            Backend unavailable = new Backend(2L, "127.0.0.2", 9050);
            ComputeGroup computeGroup = Mockito.mock(ComputeGroup.class);
            Mockito.when(computeGroup.getBackendList()).thenReturn(ImmutableList.of(alive, unavailable));
            TupleDescriptor tuple = Mockito.mock(TupleDescriptor.class);
            Mockito.when(tuple.getId()).thenReturn(new TupleId(1));

            MaterializationNode node = new MaterializationNode(
                    new PlanNodeId(1), tuple, Mockito.mock(PlanNode.class), computeGroup);

            TPaloNodesInfo nodes = Deencapsulation.getField(node, "nodesInfo");
            Assertions.assertEquals(1, nodes.getNodesSize());
            Assertions.assertEquals(alive.getId(), nodes.getNodes().get(0).getId());
            Assertions.assertNull(ConnectContext.get());
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
