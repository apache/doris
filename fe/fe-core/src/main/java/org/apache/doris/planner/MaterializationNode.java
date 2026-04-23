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
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
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
    /**
     * Example to illustrate the fields below:
     *
     *   SQL:  SELECT pk, col_date FROM t ORDER BY pk LIMIT 10
     *         (col_date is chosen for lazy materialization)
     *
     *   The child plan produces:  [pk(eager), row_id_t]
     *   row_id_t is a "global row-id" slot that encodes where the real col_date lives on disk.
     *
     *   materializeTupleDescriptor (intermediate tuple, e.g. tuple 5):
     *     index 0 → pk          (eager slot, already in the child block)
     *     index 1 → col_date    (lazy slot, to be fetched remotely via row_id)
     *
     *   outputTupleDesc (final output tuple, e.g. tuple 6):
     *     contains the columns actually returned to the parent node after
     *     applying projectList on top of materializeTupleDescriptor.
     *     In simple cases it is the same set of columns.
     *     Example: projectList = [pk#0, col_date#1]  →  outputTupleDesc = {pk, col_date}
     *
     *   Relationship:
     *     materializeTupleDescriptor  =  "working" tuple that holds eager + lazy columns,
     *                                    used to build the RPC fetch request and place the
     *                                    fetched values back into the block.
     *     outputTupleDesc             =  "public" tuple visible to the parent; produced by
     *                                    projecting materializeTupleDescriptor through projectList.
     *
     *   locations (= PhysicalLazyMaterialize.lazySlotLocations):
     *     For each relation, the index of each lazy slot inside materializeTupleDescriptor.
     *     In the example above: [[1]]
     *       → outer list index 0 = first (only) relation t
     *       → inner value 1      = col_date is at index 1 in materializeTupleDescriptor.slots()
     *     BE uses this to call  slots[location]->to_protobuf(...)  when building the fetch
     *     request so the remote side knows the schema of what to return.
     *
    *   columnIdxsLists (= PhysicalLazyMaterialize.lazyBaseColumnIndices):
     *     For each relation, the physical column index in the table for each lazy column.
     *     In the example above: [[3]]   (col_date is the 3rd column in t, 0-indexed)
     *     BE sends this index to the storage layer so it can locate the column on disk
     *     without needing the full schema.
     *
     *   If two columns b and c from table t were both lazy, and the column indices in the
     *   table are 2 and 5 respectively, and they occupy slots at index 1 and 2 in the tuple:
     *     locations = [[1, 2]],  columnIdxsLists = [[2, 5]]
     */
    private TPaloNodesInfo nodesInfo;
    private TupleDescriptor materializeTupleDescriptor;

    private List<Expr> rowIds;

    private List<List<Column>> lazyColumns;

    private List<List<Integer>> locations;
    private List<List<Integer>> columnIdxsLists;

    private List<Boolean> rowStoreFlags;

    private boolean isTopMaterializeNode;

    public MaterializationNode(PlanNodeId id, TupleDescriptor desc, PlanNode child) {
        super(id, desc.getId().asList(), "MaterializeNode");
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
        output.append(detailPrefix).append("column_idxs_lists: ").append(columnIdxsLists).append("\n");
        output.append(detailPrefix).append("row_ids: ").append(rowIds).append("\n");
        output.append(detailPrefix).append("isTopMaterializeNode: ").append(isTopMaterializeNode).append("\n");
        printNestedColumns(output, detailPrefix, outputTupleDesc);

        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MATERIALIZATION_NODE;
        msg.materialization_node = new TMaterializationNode();
        msg.materialization_node.setTupleId(tupleIds.get(0).asInt());
        msg.materialization_node.setIntermediateTupleId(materializeTupleDescriptor.getId().asInt());
        msg.materialization_node.setNodesInfo(nodesInfo);
        msg.materialization_node.setFetchExprLists(ExprToThriftVisitor.treesToThrift(rowIds));

        List<List<TColumn>> thriftCols = new ArrayList<>();
        for (List<Column> cols : lazyColumns) {
            List<TColumn> array = new ArrayList<>();
            for (Column col : cols) {
                array.add(ColumnToThrift.toThrift(col));
            }
            thriftCols.add(array);
        }
        msg.materialization_node.setColumnDescsLists(thriftCols);

        msg.materialization_node.setSlotLocsLists(locations);
        msg.materialization_node.setColumnIdxsLists(columnIdxsLists);
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

    public void setColumnIdxsLists(List<List<Integer>> columnIdxsLists) {
        this.columnIdxsLists = columnIdxsLists;
    }

    public void setRowStoreFlags(List<Boolean> rowStoreFlags) {
        this.rowStoreFlags = rowStoreFlags;
    }

    public void setTopMaterializeNode(boolean topMaterializeNode) {
        isTopMaterializeNode = topMaterializeNode;
    }

    @Override
    public boolean isSerialOperator() {
        return true;
    }
}
