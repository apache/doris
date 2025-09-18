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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExchangeNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExchangeNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Collections;

/**
 * Receiver side of a 1:n data stream. Logically, an ExchangeNode consumes the data
 * produced by its children. For each of the sending child nodes the actual data
 * transmission is performed by the DataStreamSink of the PlanFragment housing
 * that child node. Typically, an ExchangeNode only has a single sender child but,
 * e.g., for distributed union queries an ExchangeNode may have one sender child per
 * union operand.
 *
 * If a (optional) SortInfo field is set, the ExchangeNode will merge its
 * inputs on the parameters specified in the SortInfo object. It is assumed that the
 * inputs are also sorted individually on the same SortInfo parameter.
 */
public class ExchangeNode extends PlanNode {

    public static final String EXCHANGE_NODE = "EXCHANGE";
    public static final String MERGING_EXCHANGE_NODE = "MERGING-EXCHANGE";

    // The parameters based on which sorted input streams are merged by this
    // exchange node. Null if this exchange does not merge sorted streams
    private SortInfo mergeInfo;

    private boolean isRightChildOfBroadcastHashJoin = false;
    private TPartitionType partitionType;

    /**
     * use for Nereids only.
     */
    public ExchangeNode(PlanNodeId id, PlanNode inputNode) {
        super(id, inputNode, EXCHANGE_NODE, StatisticalType.EXCHANGE_NODE);
        offset = 0;
        limit = -1;
        this.conjuncts = Collections.emptyList();
        children.add(inputNode);
        computeTupleIds();
    }

    public TPartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(TPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    @Override
    public final void computeTupleIds() {
        PlanNode inputNode = getChild(0);
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
        updateTupleIds(outputTupleDesc);
    }

    public void updateTupleIds(TupleDescriptor outputTupleDesc) {
        if (outputTupleDesc != null) {
            tupleIds.clear();
            tupleIds.add(outputTupleDesc.getId());
            tblRefIds.add(outputTupleDesc.getId());
            nullableTupleIds.add(outputTupleDesc.getId());
        } else {
            clearTupleIds();
            tupleIds.addAll(getChild(0).getOutputTupleIds());
            tblRefIds.addAll(getChild(0).getTblRefIds());
            nullableTupleIds.addAll(getChild(0).getNullableTupleIds());
        }
    }

    /**
     * Set the parameters used to merge sorted input streams. This can be called
     * after init().
     */
    public void setMergeInfo(SortInfo info) {
        this.mergeInfo = info;
        this.planNodeName =  "V" + MERGING_EXCHANGE_NODE;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        // If this fragment has another scan node, this exchange node is serial or not should be decided by the scan
        // node.
        msg.setIsSerialOperator((isSerialOperator() || fragment.hasSerialScanNode())
                && fragment.useSerialSource(ConnectContext.get()));
        msg.node_type = TPlanNodeType.EXCHANGE_NODE;
        msg.exchange_node = new TExchangeNode();
        for (TupleId tid : tupleIds) {
            msg.exchange_node.addToInputRowTuples(tid.asInt());
        }
        if (mergeInfo != null) {
            msg.exchange_node.setSortInfo(mergeInfo.toThrift());
        }
        msg.exchange_node.setOffset(offset);
        msg.exchange_node.setPartitionType(partitionType);
    }

    @Override
    protected String debugString() {
        ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("offset", offset);
        return helper.toString();
    }

    @Override
    public int getNumInstances() {
        return numInstances;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "offset: " + offset + "\n";
    }

    public boolean isRightChildOfBroadcastHashJoin() {
        return isRightChildOfBroadcastHashJoin;
    }

    public void setRightChildOfBroadcastHashJoin(boolean value) {
        isRightChildOfBroadcastHashJoin = value;
    }

    /**
     * If table `t1` has unique key `k1` and value column `v1`.
     * Now use plan below to load data into `t1`:
     * ```
     * FRAGMENT 0:
     *  Merging Exchange (id = 1)
     *   NL Join (id = 2)
     *  DataStreamSender (id = 3, dst_id = 3) (OLAP_TABLE_SINK_HASH_PARTITIONED)
     *
     * FRAGMENT 1:
     *  Exchange (id = 3)
     *  OlapTableSink (id = 4) ```
     *
     * In this plan, `Exchange (id = 1)` needs to do merge sort using column `k1` and `v1` so parallelism
     * of FRAGMENT 0 must be 1 and data will be shuffled to FRAGMENT 1 which also has only 1 instance
     * because this loading job relies on the global ordering of column `k1` and `v1`.
     *
     * So FRAGMENT 0 should not use serial source.
     */
    @Override
    public boolean isSerialOperator() {
        return (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isUseSerialExchange()
                || partitionType == TPartitionType.UNPARTITIONED) && mergeInfo == null;
    }

    @Override
    public boolean hasSerialChildren() {
        return isSerialOperator();
    }

    @Override
    public boolean hasSerialScanChildren() {
        return false;
    }
}
