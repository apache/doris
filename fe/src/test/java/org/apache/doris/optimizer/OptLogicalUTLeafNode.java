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

package org.apache.doris.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptLogical;
import org.apache.doris.optimizer.operator.OptOperatorType;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.BitSet;
import java.util.List;

public class OptLogicalUTLeafNode extends OptLogical {
    private int value;
    private List<OptColumnRef> outputColumns;

    public OptLogicalUTLeafNode() {
        super(OptOperatorType.OP_LOGICAL_UNIT_TEST_LEAF);
        this.value = OptUtils.getUTOperatorId();
        this.outputColumns = Lists.newArrayList();
    }

    public OptLogicalUTLeafNode(List<OptColumnRef> outputColumns) {
        super(OptOperatorType.OP_LOGICAL_UNIT_TEST_LEAF);
        this.value = OptUtils.getUTOperatorId();
        this.outputColumns = outputColumns;
    }

    public int getValue() { return value; }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptLogicalUTLeafNode rhs = (OptLogicalUTLeafNode) object;
        return value == rhs.value;
    }

    @Override
    public String getExplainString(String prefix) { return type.getName() + " (value=" + value + ")";
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_IMP_UT_LEAF.ordinal());
        return set;
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle expressionHandle, RequiredLogicalProperty property) {
        final Statistics statistics = new Statistics(property);
        for (OptColumnRef column : outputColumns) {
            statistics.addColumnCardinality(column.getId(), 100);
        }
        statistics.setRowCount(1000);
        return statistics;
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle, RequiredLogicalProperty property, int childIndex) {
        Preconditions.checkState(false, "Scan does't have children.");
        return null;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(outputColumns);
        return columns;
    }

}
