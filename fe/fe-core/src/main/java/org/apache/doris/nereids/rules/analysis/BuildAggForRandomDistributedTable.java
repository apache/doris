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

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.QuantileUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * build agg plan for querying random distributed table
 */
public class BuildAggForRandomDistributedTable implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Project(Scan) -> project(agg(scan))
                logicalProject(logicalOlapScan())
                        .when(this::isQuery)
                        .when(project -> isRandomDistributedTbl(project.child()))
                        .then(project -> preAggForRandomDistribution(project, project.child()))
                        .toRule(RuleType.BUILD_AGG_FOR_RANDOM_DISTRIBUTED_TABLE_PROJECT_SCAN),
                // agg(scan) -> agg(agg(scan)), agg(agg) may optimized by MergeAggregate
                logicalAggregate(logicalOlapScan())
                        .when(this::isQuery)
                        .when(agg -> isRandomDistributedTbl(agg.child()))
                        .whenNot(agg -> {
                            Set<AggregateFunction> functions = agg.getAggregateFunctions();
                            List<Expression> groupByExprs = agg.getGroupByExpressions();
                            // check if need generate an inner agg plan or not
                            // should not rewrite twice if we had rewritten olapScan to aggregate(olapScan)
                            return functions.stream().allMatch(this::aggTypeMatch) && groupByExprs.stream()
                                    .allMatch(this::isKeyOrConstantExpr);
                        })
                        .then(agg -> preAggForRandomDistribution(agg, agg.child()))
                        .toRule(RuleType.BUILD_AGG_FOR_RANDOM_DISTRIBUTED_TABLE_AGG_SCAN),
                // filter(scan) -> filter(agg(scan))
                logicalFilter(logicalOlapScan())
                        .when(this::isQuery)
                        .when(filter -> isRandomDistributedTbl(filter.child()))
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
        return keysType == KeysType.AGG_KEYS && distributionInfo.getType() == DistributionInfoType.RANDOM;
    }

    private boolean isQuery(LogicalPlan plan) {
        return ConnectContext.get() != null
                && ConnectContext.get().getState() != null
                && ConnectContext.get().getState().isQuery();
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
            SlotReference slot = SlotReference.fromColumn(olapTable, col, col.getName(), olapScan.getQualifier());
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
                Expression function = generateAggFunction(slot, col);
                // DO NOT rewrite
                if (function == null) {
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
     * generate aggregation function according to the aggType of column
     *
     * @param slot slot of column
     * @return aggFunction generated
     */
    private Expression generateAggFunction(SlotReference slot, Column column) {
        AggregateType aggregateType = column.getAggregationType();
        switch (aggregateType) {
            case SUM:
                return new Sum(slot);
            case MAX:
                return new Max(slot);
            case MIN:
                return new Min(slot);
            case HLL_UNION:
                return new HllUnion(slot);
            case BITMAP_UNION:
                return new BitmapUnion(slot);
            case QUANTILE_UNION:
                return new QuantileUnion(slot);
            case GENERIC:
                Type type = column.getType();
                if (!type.isAggStateType()) {
                    return null;
                }
                AggStateType aggState = (AggStateType) type;
                // use AGGREGATE_FUNCTION_UNION to aggregate multiple agg_state into one
                String funcName = aggState.getFunctionName() + AggCombinerFunctionBuilder.UNION_SUFFIX;
                FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
                FunctionBuilder builder = functionRegistry.findFunctionBuilder(funcName, slot);
                return builder.build(funcName, ImmutableList.of(slot)).first;
            default:
                return null;
        }
    }

    /**
     * if the agg type of AggregateFunction is as same as the agg type of column, DO NOT need to rewrite
     *
     * @param function agg function to check
     * @return true if agg type match
     */
    private boolean aggTypeMatch(AggregateFunction function) {
        List<Expression> children = function.children();
        if (function.getName().equalsIgnoreCase("count")) {
            Count count = (Count) function;
            // do not rewrite for count distinct for key column
            if (count.isDistinct()) {
                return children.stream().allMatch(this::isKeyOrConstantExpr);
            }
            if (count.isStar()) {
                return false;
            }
        }
        return children.stream().allMatch(child -> aggTypeMatch(function, child));
    }

    /**
     * check if the agg type of functionCall match the agg type of column
     *
     * @param function the functionCall
     * @param expression expr to check
     * @return true if agg type match
     */
    private boolean aggTypeMatch(AggregateFunction function, Expression expression) {
        if (expression.children().isEmpty()) {
            if (expression instanceof SlotReference && ((SlotReference) expression).getColumn().isPresent()) {
                Column col = ((SlotReference) expression).getColumn().get();
                String functionName = function.getName();
                if (col.isKey()) {
                    return functionName.equalsIgnoreCase("max") || functionName.equalsIgnoreCase("min");
                }
                if (col.isAggregated()) {
                    AggregateType aggType = col.getAggregationType();
                    // agg type not mach
                    if (aggType == AggregateType.GENERIC) {
                        return col.getType().isAggStateType();
                    }
                    if (aggType == AggregateType.HLL_UNION) {
                        return function instanceof HllFunction;
                    }
                    if (aggType == AggregateType.BITMAP_UNION) {
                        return function instanceof BitmapFunction;
                    }
                    return functionName.equalsIgnoreCase(aggType.name());
                }
            }
            return false;
        }
        List<Expression> children = expression.children();
        return children.stream().allMatch(child -> aggTypeMatch(function, child));
    }

    /**
     * check if the columns in expr is key column or constant, if group by clause contains value column, need rewrite
     *
     * @param expr expr to check
     * @return true if all columns is key column or constant
     */
    private boolean isKeyOrConstantExpr(Expression expr) {
        if (expr instanceof SlotReference && ((SlotReference) expr).getColumn().isPresent()) {
            Column col = ((SlotReference) expr).getColumn().get();
            return col.isKey();
        } else if (expr.isConstant()) {
            return true;
        }
        List<Expression> children = expr.children();
        return children.stream().allMatch(this::isKeyOrConstantExpr);
    }
}
