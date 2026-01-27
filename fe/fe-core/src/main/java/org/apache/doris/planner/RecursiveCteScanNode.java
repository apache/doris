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
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;

// Full scan of recursive cte temp table
public class RecursiveCteScanNode extends PlanNode {
    private final String recursiveCteName;

    public RecursiveCteScanNode(String recursiveCteName, PlanNodeId id, TupleDescriptor desc) {
        super(id, desc.getId().asList(), "RECURSIVE_CTE_SCAN");
        this.recursiveCteName = recursiveCteName;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("Recursive Cte: ").append(recursiveCteName).append("\n");
        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
        }
        return output.toString();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("recursiveCteName", recursiveCteName)
                .add("id", getId().asInt())
                .add("tid", getTupleIds().get(0)).toString();
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.REC_CTE_SCAN_NODE;
    }

    @Override
    public boolean isSerialOperator() {
        return true;
    }
}
