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
import org.apache.doris.analysis.Expr;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Node that applies conjuncts and a limit clause. Has exactly one child.
 */
public class SelectNode extends PlanNode {
    private final static Logger LOG = LogManager.getLogger(SelectNode.class);

    protected SelectNode(PlanNodeId id, PlanNode child) {
        super(id, child.getTupleIds(), "SELECT");
        addChild(child);
        this.nullableTupleIds = child.nullableTupleIds;
    }
    protected SelectNode(PlanNodeId id, PlanNode child, List<Expr> conjuncts) {
        super(id, child.getTupleIds(), "SELECT");
        addChild(child);
        this.tblRefIds = child.tblRefIds;
        this.nullableTupleIds = child.nullableTupleIds;
        this.conjuncts.addAll(conjuncts);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SELECT_NODE;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        analyzer.markConjunctsAssigned(conjuncts);
        computeStats(analyzer);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }
        cardinality = getChild(0).cardinality;
        applyConjunctsSelectivity();
        capCardinalityAtLimit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Select: cardinality={}", this.cardinality);
        }
    }

    @Override
    protected void computeOldCardinality() {
        long cardinality = getChild(0).cardinality;
        double selectivity = computeOldSelectivity();
        if (cardinality < 0 || selectivity < 0) {
            this.cardinality = -1;
        } else {
            this.cardinality = Math.round(cardinality * selectivity);
        }
        LOG.debug("stats Select: cardinality={}", this.cardinality);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        if (!conjuncts.isEmpty()) {
            output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }
}
