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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TMaterializationNode;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.List;

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
 *
 *     类似于 slot_locs_lists, 不过其中的数字表示在 table 中的第几列
 *     8. optional list<list<int>> table_idx_lists;
 *
 * }
 */
public class MaterializationNode extends PlanNode {
    private TPaloNodesInfo nodesInfo;
    private TupleDescriptor materializeTupleDescriptor;

    private List<Expr> rowIds;

    private List<List<Column>> lazyColumns;

    private List<List<Integer>> locations;
    private List<List<Integer>> idxs;

    private List<Boolean> rowStoreFlags;

    private boolean isTopMaterializeNode;

    public MaterializationNode(PlanNodeId id, TupleDescriptor desc, PlanNode child) {
        super(id, desc.getId().asList(), "MaterializeNode", StatisticalType.DEFAULT);
        this.materializeTupleDescriptor = desc;
        initNodeInfo();
        this.children.add(child);
    }

    public void initNodeInfo() {
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .setRequireAliveBe()
                .build();
        nodesInfo = new TPaloNodesInfo();
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            context = new ConnectContext();
        }
        ComputeGroup computeGroup = context.getComputeGroupSafely();
        for (Backend backend : policy.getCandidateBackends(computeGroup.getBackendList())) {
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix)
                .append("materialize tuple id:")
                .append(materializeTupleDescriptor.getId()).append("\n");

        if (!projectList.isEmpty()) {
            output.append(detailPrefix)
                    .append("output tuple id:").append(outputTupleDesc.getId()).append("\n");

            output.append(detailPrefix)
                    .append("projectList:").append(projectList.toString()).append("\n");
        }
        output.append(detailPrefix).append("column_descs_lists").append(lazyColumns).append("\n");
        output.append(detailPrefix).append("locations: ").append(locations).append("\n");
        output.append(detailPrefix).append("table_idxs: ").append(idxs).append("\n");
        output.append(detailPrefix).append("row_ids: ").append(rowIds).append("\n");
        output.append(detailPrefix).append("isTopMaterializeNode: ").append(isTopMaterializeNode).append("\n");
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MATERIALIZATION_NODE;
        msg.materialization_node = new TMaterializationNode();
        msg.materialization_node.setTupleId(tupleIds.get(0).asInt());
        msg.materialization_node.setIntermediateTupleId(materializeTupleDescriptor.getId().asInt());
        msg.materialization_node.setNodesInfo(nodesInfo);
        msg.materialization_node.setFetchExprLists(Expr.treesToThrift(rowIds));

        List<List<TColumn>> thriftCols = new ArrayList<>();
        for (List<Column> cols : lazyColumns) {
            List<TColumn> array = new ArrayList<>();
            for (Column col : cols) {
                array.add(col.toThrift());
            }
            thriftCols.add(array);
        }
        msg.materialization_node.setColumnDescsLists(thriftCols);

        msg.materialization_node.setSlotLocsLists(locations);
        msg.materialization_node.setColumnIdxsLists(idxs);
        msg.materialization_node.setFetchRowStores(rowStoreFlags);
        msg.materialization_node.setGcIdMap(isTopMaterializeNode);
    }

    public void setRowIds(List<Expr> rowIds) {
        this.rowIds = rowIds;
    }

    public void setLazyColumns(List<List<Column>> lazyColumns) {
        this.lazyColumns = lazyColumns;
    }

    public void setLocations(List<List<Integer>> locations) {
        this.locations = locations;
    }

    public void setIdxs(List<List<Integer>> idxs) {
        this.idxs = idxs;
    }

    public void setRowStoreFlags(List<Boolean> rowStoreFlags) {
        this.rowStoreFlags = rowStoreFlags;
    }

    public void setTopMaterializeNode(boolean topMaterializeNode) {
        isTopMaterializeNode = topMaterializeNode;
    }

    public TupleDescriptor getMaterializeTupleDescriptor() {
        return materializeTupleDescriptor;
    }

    public List<Expr> getRowIds() {
        return rowIds;
    }

    public List<List<Column>> getLazyColumns() {
        return lazyColumns;
    }

    public List<List<Integer>> getLocations() {
        return locations;
    }

    public List<Boolean> getRowStoreFlags() {
        return rowStoreFlags;
    }

    public boolean isTopMaterializeNode() {
        return isTopMaterializeNode;
    }
}
