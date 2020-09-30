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
import org.apache.doris.common.UserException;
import org.apache.doris.analysis.Expr;

import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
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
      analyzer.markConjunctsAssigned(conjuncts);
      computeStats(analyzer);
      createDefaultSmap(analyzer);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (getChild(0).cardinality == -1) {
            cardinality = -1;
        } else {
            cardinality = Math.round(((double) getChild(0).cardinality) * computeSelectivity());
            Preconditions.checkState(cardinality >= 0);
        }
        LOG.info("stats Select: cardinality=" + Long.toString(cardinality));
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
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
