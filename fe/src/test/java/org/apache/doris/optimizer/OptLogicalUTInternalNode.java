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

import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptLogical;
import org.apache.doris.optimizer.operator.OptOperatorType;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsEstimator;

import java.util.BitSet;

// Only used for UT
public class OptLogicalUTInternalNode extends OptLogical {

    public OptLogicalUTInternalNode() {
        super(OptOperatorType.OP_LOGICAL_UNIT_TEST_INTERNAL);
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        return true;
    }

    @Override
    public String getExplainString(String prefix) { return type.getName(); }

    @Override
    public BitSet getCandidateRulesForExplore() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_EXP_UT_COMMUTATIVITY.ordinal());
        set.set(OptRuleType.RULE_EXP_UT_ASSOCIATIVITY.ordinal());
        return set;
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_IMP_UT_INTERNAL.ordinal());
        return set;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(exprHandle.getChildLogicalProperty(0).getOutputColumns());
        columns.include(exprHandle.getChildLogicalProperty(1).getOutputColumns());
        return columns;
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle expressionHandle, RequiredLogicalProperty property) {
        return StatisticsEstimator.estimateUTInternal(expressionHandle, property);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle, RequiredLogicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        final OptLogicalProperty childLogicalProperty = expressionHandle.getChildLogicalProperty(childIndex);
        columns.include(childLogicalProperty.getOutputColumns());
        return columns;
    }

}
