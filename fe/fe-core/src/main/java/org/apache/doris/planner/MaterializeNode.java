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

import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TPlanNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MaterializeNode extends PlanNode {
    private TPaloNodesInfo nodesInfo;
    private PlanNodeId id;
    private TupleDescriptor desc;

    private List<SlotRef> rowIds;

    private List<List<Column>> columns;

    private List<List<Integer>> locations;

    private List<Boolean> slotRowStoreFlags;

    private boolean isTopMaterializeNode;

    public MaterializeNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc.getId().asList(), "MaterializeNode", StatisticalType.DEFAULT);
        initNodeInfo();
    }

    public void initNodeInfo() {
        // get backend by tag
        Set<Tag> tagSet = new HashSet<>();
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            tagSet = context.getResourceTags();
        }
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .setRequireAliveBe()
                .addTags(tagSet)
                .build();
        nodesInfo = new TPaloNodesInfo();
        for (Backend backend : Env.getCurrentSystemInfo().getBackendsByPolicy(policy)) {
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }
}
