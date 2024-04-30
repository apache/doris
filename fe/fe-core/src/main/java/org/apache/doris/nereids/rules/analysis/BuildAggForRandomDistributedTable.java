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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * build agg plan for querying random distributed table
 */
public class BuildAggForRandomDistributedTable implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Project(Scan)
                logicalProject(logicalOlapScan()).when(project -> isRandomDistributedTbl(project.child()))
                        .then(project -> preAggForRandomDistribution(project, project.child()))
                        .toRule(RuleType.BUILD_AGG_FOR_RANDOM_DISTRIBUTED_TABLE_PROJECT_SCAN),
                // agg(scan)
                logicalAggregate(logicalOlapScan()).when(agg -> isRandomDistributedTbl(agg.child()))
                        .whenNot(agg -> agg.getAggregateFunctions().stream().allMatch(this::aggTypeMatch))
                        .then(agg -> preAggForRandomDistribution(agg, agg.child()))
                        .toRule(RuleType.BUILD_AGG_FOR_RANDOM_DISTRIBUTED_TABLE_AGG_SCAN),
                // filter(scan)
                logicalFilter(logicalOlapScan()).when(filter -> isRandomDistributedTbl(filter.child()))
                        .then(filter -> preAggForRandomDistribution(filter, filter.child()))
                        .toRule(RuleType.BUILD_AGG_FOR_RANDOM_DISTRIBUTED_TABLE_FILTER_SCAN));
    }

    /**
     * check the olapTable of olapScan is randomDistributed table
     *
     * @param olapScan olap scan plan
     * @return true if olapTable is randomDistributed table
     */
    private boolean isRandomDistributedTbl(LogicalOlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        KeysType keysType = olapTable.getKeysType();
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
        return keysType == KeysType.AGG_KEYS
                && distributionInfo.getType() == DistributionInfo.DistributionInfoType.RANDOM;
    }

    /**
     * add LogicalAggregate above olapScan for preAgg
     *
     * @param logicalPlan parent plan of olapScan
     * @param olapScan olap scan plan, it may be LogicalProject, LogicalFilter, LogicalAggregate
     * @return rewritten plan
     */
    private Plan preAggForRandomDistribution(LogicalPlan logicalPlan, LogicalOlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        List<Slot> childOutputSlots = olapScan.computeOutput();
        List<Expression> groupByExpressions = new ArrayList<>();
        List<NamedExpression> outputExpressions = new ArrayList<>();
        List<Column> columns = olapTable.getBaseSchema();

        for (Column col : columns) {
            // use exist slot in the plan
            Slot slot = SlotReference.fromColumn(olapTable, col, col.getName(), olapScan.getQualifier());
            ExprId exprId = slot.getExprId();
            for (Slot childSlot : childOutputSlots) {
                if (childSlot instanceof SlotReference && ((SlotReference) childSlot).getName() == col.getName()) {
                    exprId = childSlot.getExprId();
                    slot = slot.withExprId(exprId);
                    break;
                }
            }
            if (col.isKey()) {
                groupByExpressions.add(slot);
                outputExpressions.add(slot);
            } else {
                AggregateType aggregateType = col.getAggregationType();
                AggregateFunction function;
                if (aggregateType == AggregateType.SUM) {
                    function = new Sum(slot);
                } else if (aggregateType == AggregateType.MAX) {
                    function = new Max(slot);
                } else if (aggregateType == AggregateType.MIN) {
                    function = new Min(slot);
                } else {
                    // do not rewrite
                    return logicalPlan;
                }
                Alias alias = new Alias(exprId, function, col.getName());
                outputExpressions.add(alias);
            }
        }
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(groupByExpressions, outputExpressions,
                olapScan);
        return logicalPlan.withChildren(aggregate);
    }

    /**
     * if the agg type of AggregateFunction is as same as the agg type of column, DO NOT need to rewrite
     *
     * @param function agg function to check
     * @return true if agg type match
     */
    private boolean aggTypeMatch(AggregateFunction function) {
        List<Expression> children = function.children();
        String functionName = function.getName();
        // do not rewrite for count distinct
        if (function.getName().equalsIgnoreCase("count")) {
            return function.isDistinct();
        }
        return children.stream().allMatch(child -> aggTypeMatch(functionName, child));
    }

    /**
     * check if the agg type of functionCall match the agg type of column
     *
     * @param functionName the functionName of functionCall
     * @param expression expr to check
     * @return true if agg type match
     */
    private boolean aggTypeMatch(String functionName, Expression expression) {
        if (expression.children().isEmpty()) {
            if (expression instanceof SlotReference && ((SlotReference) expression).getColumn().isPresent()) {
                Column col = ((SlotReference) expression).getColumn().get();
                if (col.isKey()) {
                    return functionName.equalsIgnoreCase("max") || functionName.equalsIgnoreCase("min");
                }
                if (col.isAggregated()) {
                    AggregateType aggType = col.getAggregationType();
                    // agg type not mach
                    return (aggType == AggregateType.SUM || aggType == AggregateType.MAX
                            || aggType == AggregateType.MIN) && functionName.equalsIgnoreCase(aggType.name());
                }
            }
            return false;
        }
        List<Expression> children = expression.children();
        return children.stream().allMatch(child -> aggTypeMatch(functionName, child));
    }
}
