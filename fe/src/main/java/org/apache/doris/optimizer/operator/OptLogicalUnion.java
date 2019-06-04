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
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsEstimator;

import java.util.BitSet;
import java.util.List;

public class OptLogicalUnion extends OptLogical {
    private final boolean isUnionAll;
    private final List<OptColumnRef> groupBys;

    public OptLogicalUnion(List<OptColumnRef> groupBys, boolean isUnionAll) {
        super(OptOperatorType.OP_LOGICAL_UNION);
        this.isUnionAll = isUnionAll;
        this.groupBys = groupBys;
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet ruleSet = new BitSet();
        ruleSet.set(OptRuleType.RULE_IMP_UNION.ordinal());
        return ruleSet;
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle exprHandle, RequiredLogicalProperty property) {
        Preconditions.checkArgument(exprHandle.getChildrenStatistics().size() == 2);
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(groupBys);
        return StatisticsEstimator.estimateUnion(columns, exprHandle, property);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle,
            RequiredLogicalProperty property, int childIndex) {
        final OptLogicalProperty childProperty = expressionHandle.getChildLogicalProperty(childIndex);
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(groupBys);
        columns.intersects(childProperty.getOutputColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(groupBys);
        return columns;
    }

    public boolean isUnionAll() {
        return this.isUnionAll;
    }

    public List<OptColumnRef> getGroupBy() {
        return groupBys;
    }
}
