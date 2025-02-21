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
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TPlanNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * struct TMaterializationNode {
 *     // A Materialization materializes all tuple
 *     // 如果 child 的output 是[a, row_id1, b, row_id2]，
 *     // row_id1 生成 e f
 *     // row_id2 生成 c d
 *     // 那么 tuple_id =  [a, e, f, b, c, d]
 *     1: optional Types.TTupleId tuple_id
 *
 *     // Nodes in this cluster, used for second phase fetch, BE 节点信息
 *     2: optional Descriptors.TPaloNodesInfo nodes_info;
 *
 *     // Separate list of expr for fetch data
 *     // row_id 字段对应的slotRef 的列表: [row_id1, row_id2]
 *     3: optional list<Exprs.TExpr>> fetch_expr_lists
 *     // Fetch schema
 *     //[[e, f],[c, d]],
 *     4: optional list<list<Descriptors.TColumn>> column_descs_lists;
 *
 *     // 和column_descs_lists 对应，slot_locs_lists=[[1, 2], [4, 5]]，
 *     // 其中 1表示e在tuple_id 中的第1号slotDesc，f 对应tuple_id 中第2号 slotDesc
 *     5: optional list<list<int>> slot_locs_lists;
 *
 *     // Whether fetch row store
 *     // fe根据是否有行存 以及 延迟物化列和总列数的比例 延迟物化列数 判断是否使用行存优化
 *     6: optional list<bool> fetch_row_store;
 *
 *     7：bool do_gc = false; // 最靠近root的 MaterializeNode设置为true，其它M 设置为false
 * }
 */
public class MaterializeNode extends PlanNode {
    private TPaloNodesInfo nodesInfo;
    private PlanNodeId id;
    private TupleDescriptor outputTupleDescriptor;
    private TupleDescriptor materializeTupleDescriptor;

    private List<SlotRef> rowIds;

    private List<List<Column>> columns;

    private List<List<Integer>> locations;

    private List<Boolean> slotRowStoreFlags;

    private boolean isTopMaterializeNode;

    public MaterializeNode(PlanNodeId id, TupleDescriptor desc, PlanNode child) {
        super(id, desc.getId().asList(), "MaterializeNode", StatisticalType.DEFAULT);
        this.materializeTupleDescriptor = desc;
        this.outputTupleDescriptor = desc;
        initNodeInfo();
        this.children.add(child);
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

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        return "";
    }
}
