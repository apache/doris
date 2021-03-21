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

import org.apache.doris.analysis.AssertNumRowsElement;
import org.apache.doris.thrift.TAssertNumRowsNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

/**
 * Assert num rows node is used to determine whether the number of rows is less then desired num of rows.
 * The rows are the result of subqueryString.
 * If the number of rows is more than the desired num of rows, the query will be cancelled.
 * The cancelled reason will be reported by Backend and displayed back to the user.
 */
public class AssertNumRowsNode extends PlanNode {

    private long desiredNumOfRows;
    private String subqueryString;
    private AssertNumRowsElement.Assertion assertion;

    public AssertNumRowsNode(PlanNodeId id, PlanNode input, AssertNumRowsElement assertNumRowsElement) {
        super(id, "ASSERT NUMBER OF ROWS");
        this.desiredNumOfRows = assertNumRowsElement.getDesiredNumOfRows();
        this.subqueryString = assertNumRowsElement.getSubqueryString();
        this.assertion = assertNumRowsElement.getAssertion();
        this.children.add(input);
        this.tupleIds.addAll(input.getTupleIds());
        this.tblRefIds.addAll(input.getTblRefIds());
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (detailLevel == TExplainLevel.BRIEF) {
            return "";
        }
        StringBuilder output = new StringBuilder()
                .append(prefix + "assert number of rows: ")
                .append(assertion).append(" ").append(desiredNumOfRows).append("\n");
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ASSERT_NUM_ROWS_NODE;
        msg.assert_num_rows_node = new TAssertNumRowsNode();
        msg.assert_num_rows_node.setDesiredNumRows(desiredNumOfRows);
        msg.assert_num_rows_node.setSubqueryString(subqueryString);
        msg.assert_num_rows_node.setAssertion(assertion.toThrift());
    }
}
