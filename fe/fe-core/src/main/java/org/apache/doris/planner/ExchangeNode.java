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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TExchangeNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOG = LogManager.getLogger(ExchangeNode.class);

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

    public void setPartitionType(TPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    /**
     * Create ExchangeNode that consumes output of inputNode.
     * An ExchangeNode doesn't have an input node as a child, which is why we
     * need to compute the cardinality here.
     */
    public ExchangeNode(PlanNodeId id, PlanNode inputNode, boolean copyConjuncts) {
        super(id, inputNode, EXCHANGE_NODE, StatisticalType.EXCHANGE_NODE);
        offset = 0;
        children.add(inputNode);
        if (!copyConjuncts) {
            this.conjuncts = Lists.newArrayList();
        }
        // Only apply the limit at the receiver if there are multiple senders.
        if (inputNode.getFragment().isPartitioned()) {
            limit = inputNode.limit;
        }
        if (!(inputNode instanceof ExchangeNode)) {
            offset = inputNode.offset;
        }
        computeTupleIds();
    }

    public boolean isFunctionalExchange() {
        return mergeInfo != null || limit != -1 || offset != 0;
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
            tupleIds.addAll(getChild(0).getTupleIds());
            tblRefIds.addAll(getChild(0).getTblRefIds());
            nullableTupleIds.addAll(getChild(0).getNullableTupleIds());
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        Preconditions.checkState(conjuncts.isEmpty());
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        computeStats(analyzer);
    }

    @Override
    protected void computeStats(Analyzer analyzer) throws UserException {
        Preconditions.checkState(children.size() == 1);
        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Exchange:" + id + ", cardinality: " + cardinality);
        }
    }

    public SortInfo getMergeInfo() {
        return mergeInfo;
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
}
