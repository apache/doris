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
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.BitSet;

public class OptLogicalAggregate extends OptLogical {
    // Original columns that GroupBy refer.
    private OptColumnRefSet groupBy;
    private AggregateInfo aggInfo;

    public OptLogicalAggregate() {
        super(OptOperatorType.OP_LOGICAL_AGGREGATE);
    }

    public OptLogicalAggregate(OptColumnRefSet groupBy, AggregateInfo aggInfo) {
        super(OptOperatorType.OP_LOGICAL_AGGREGATE);
        this.groupBy = groupBy;
        this.aggInfo = aggInfo;
    }

    @Override
    public BitSet getCandidateRulesForExplore() {
        return null;
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_IMP_AGG_TO_HASH_AGG.ordinal());
        return null;
    }

    public Statistics deriveStat(OptExpressionHandle expressionHandle, RequiredLogicalProperty property) {
        Preconditions.checkArgument(expressionHandle.getChildrenStatistics().size() == 1,
                "Aggregate has wrong number of children.");
        return estimateAgg(groupBy, property, expressionHandle.getChildrenStatistics().get(0));
    }

    @Override
    public OptColumnRefSet requiredStatForChild(OptExpressionHandle expressionHandle,
                                                RequiredLogicalProperty property, int childIndex) {
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        columns.include(groupBy);

        Preconditions.checkArgument(expressionHandle.getChildProperty(childIndex) instanceof OptLogicalProperty);
        final OptLogicalProperty logical = (OptLogicalProperty) expressionHandle.getChildProperty(childIndex);
        columns.intersects(logical.getOutputColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        return exprHandle.getLogicalProperty().getOutputColumns();
    }

    public boolean isDuplicate() {
        return aggInfo.isDistinctAgg();
    }

    public OptColumnRefSet getGroupBy() {
        return groupBy;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
    }
}
