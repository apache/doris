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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Cross join between left child and right child.
 */
public class CrossJoinNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(CrossJoinNode.class);

    // Default per-host memory requirement used if no valid stats are available.
    // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
    private static final long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;
    private final TableRef innerRef;

    public CrossJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef) {
        super(id, "CROSS JOIN", StatisticalType.CROSS_JOIN_NODE);
        this.innerRef = innerRef;
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
        tblRefIds.addAll(outer.getTblRefIds());
        tblRefIds.addAll(inner.getTblRefIds());
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());
    }

    /**
     * Only for Nereids.
     */
    public CrossJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, List<TupleId> tupleIds) {
        super(id, "CROSS JOIN", StatisticalType.CROSS_JOIN_NODE);
        this.innerRef = null;
        this.tupleIds.addAll(tupleIds);
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());
    }

    public TableRef getInnerRef() {
        return innerRef;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        assignedConjuncts = analyzer.getAssignedConjuncts();
        computeStats(analyzer);
    }

    @Override
    public void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();
    }

    @Override
    protected void computeOldCardinality() {
        if (getChild(0).cardinality == -1 || getChild(1).cardinality == -1) {
            cardinality = -1;
        } else {
            cardinality = getChild(0).cardinality * getChild(1).cardinality;
            if (computeOldSelectivity() != -1) {
                cardinality = Math.round(((double) cardinality) * computeOldSelectivity());
            }
        }
        LOG.debug("stats CrossJoin: cardinality={}", Long.toString(cardinality));
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder().append(detailPrefix).append("cross join:").append("\n");
        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(String.format("cardinality=%s", cardinality)).append("\n");
            return output.toString();
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        } else {
            output.append(detailPrefix).append("predicates is NULL.");
        }
        output.append(detailPrefix).append(String.format("cardinality=%s", cardinality)).append("\n");
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
    }

}
