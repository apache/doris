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

package org.apache.doris.optimizer.operator;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.BitSet;

public class OptLogicalInnerJoin extends OptLogicalJoin {

    public OptLogicalInnerJoin() {
        super(OptOperatorType.OP_LOGICAL_INNER_JOIN);
    }

    @Override
    public BitSet getCandidateRulesForExplore() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_EXP_JOIN_COMMUTATIVITY.ordinal());
        set.set(OptRuleType.RULE_EXP_JOIN_ASSOCIATIVITY.ordinal());
        return set;
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_IMP_EQ_JOIN_TO_HASH_JOIN.ordinal());
        return set;
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle expressionHandle, RequiredLogicalProperty property) {
        Preconditions.checkArgument(expressionHandle.getChildrenStatistics().size() == 2);
        final Statistics outerChild = expressionHandle.getChildrenStatistics().get(0);
        final Statistics innerChild = expressionHandle.getChildrenStatistics().get(1);
        return new Statistics();
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle, RequiredLogicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        final OptItemProperty conjunctProperty = expressionHandle.getChildItemProperty(2);
        columns.include(conjunctProperty.getUsedColumns());
        columns.include(conjunctProperty.getDefinedColumns());
        columns.intersects(expressionHandle.getChildLogicalProperty(childIndex).getOutputColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        for (int i = 0; i < exprHandle.arity() - 1; ++i) {
            columns.include(exprHandle.getChildLogicalProperty(i).getOutputColumns());
        }
        return columns;
    }

    //------------------------------------------------------------------------
    // Used to get operator's derived property
    //------------------------------------------------------------------------
    @Override
    public OptMaxcard getMaxcard(OptExpressionHandle exprHandle) {
        return getMaxcard(exprHandle, 2, getDefaultMaxcard(exprHandle));
    }


}
