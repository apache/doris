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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** AggregateStrategies */
@DependsRules({
    NormalizeAggregate.class,
    FoldConstantRuleOnFE.class
})
public class AggregateStrategies implements ImplementationRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.COUNT_ON_INDEX_WITHOUT_PROJECT.build(
                logicalAggregate(
                    logicalFilter(
                        logicalOlapScan().when(this::isDupOrMowKeyTable).when(this::isInvertedIndexEnabledOnTable)
                    )
                )
                .when(agg -> enablePushDownCountOnIndex())
                .when(agg -> agg.getGroupByExpressions().isEmpty())
                .when(agg -> {
                    Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                    if (funcs.isEmpty() || !funcs.stream().allMatch(f -> f instanceof Count && !f.isDistinct()
                            && (((Count) f).isCountStar() || f.child(0) instanceof Slot))) {
                        return false;
                    }
                    Set<Expression> conjuncts = agg.child().getConjuncts();
                    if (conjuncts.isEmpty()) {
                        return false;
                    }

                    Set<Slot> aggSlots = funcs.stream()
                            .flatMap(f -> f.getInputSlots().stream())
                            .collect(Collectors.toSet());
                    return aggSlots.isEmpty() || conjuncts.stream().allMatch(expr ->
                                checkSlotInOrExpression(expr, aggSlots) && checkIsNullExpr(expr, aggSlots));
                })
                .thenApply(ctx -> {
                    LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg = ctx.root;
                    LogicalFilter<LogicalOlapScan> filter = agg.child();
                    LogicalOlapScan olapScan = filter.child();
                    return pushdownCountOnIndex(agg, null, filter, olapScan, ctx.cascadesContext);
                })
            ),
            RuleType.COUNT_ON_INDEX.build(
                logicalAggregate(
                    logicalProject(
                        logicalFilter(
                            logicalOlapScan().when(this::isDupOrMowKeyTable).when(this::isInvertedIndexEnabledOnTable)
                        )
                    )
                )
                .when(agg -> enablePushDownCountOnIndex())
                .when(agg -> agg.getGroupByExpressions().isEmpty())
                .when(agg -> {
                    Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                    if (funcs.isEmpty() || !funcs.stream().allMatch(f -> f instanceof Count && !f.isDistinct()
                            && (((Count) f).isCountStar() || f.child(0) instanceof Slot))) {
                        return false;
                    }
                    Set<Expression> conjuncts = agg.child().child().getConjuncts();
                    if (conjuncts.isEmpty()) {
                        return false;
                    }

                    Set<Slot> aggSlots = funcs.stream()
                            .flatMap(f -> f.getInputSlots().stream())
                            .collect(Collectors.toSet());
                    return aggSlots.isEmpty() || conjuncts.stream().allMatch(expr ->
                                checkSlotInOrExpression(expr, aggSlots) && checkIsNullExpr(expr, aggSlots));
                })
                .thenApply(ctx -> {
                    LogicalAggregate<LogicalProject<LogicalFilter<LogicalOlapScan>>> agg = ctx.root;
                    LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                    LogicalFilter<LogicalOlapScan> filter = project.child();
                    LogicalOlapScan olapScan = filter.child();
                    return pushdownCountOnIndex(agg, project, filter, olapScan, ctx.cascadesContext);
                })
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_MINMAX_ON_UNIQUE_WITHOUT_PROJECT.build(
                logicalAggregate(
                        logicalFilter(
                                logicalOlapScan().when(this::isUniqueKeyTable))
                                .when(filter -> {
                                    if (filter.getConjuncts().size() != 1) {
                                        return false;
                                    }
                                    Expression childExpr = filter.getConjuncts().iterator().next().children().get(0);
                                    if (childExpr instanceof SlotReference) {
                                        Optional<Column> column = ((SlotReference) childExpr).getOriginalColumn();
                                        return column.map(Column::isDeleteSignColumn).orElse(false);
                                    }
                                    return false;
                                })
                        )
                        .when(agg -> enablePushDownMinMaxOnUnique())
                        .when(agg -> agg.getGroupByExpressions().isEmpty())
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> (f instanceof Min) || (f instanceof Max));
                        })
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg = ctx.root;
                            LogicalFilter<LogicalOlapScan> filter = agg.child();
                            LogicalOlapScan olapScan = filter.child();
                            return pushdownMinMaxOnUniqueTable(agg, null, filter, olapScan,
                                    ctx.cascadesContext);
                        })
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_MINMAX_ON_UNIQUE.build(
                    logicalAggregate(logicalProject(logicalFilter(logicalOlapScan().when(this::isUniqueKeyTable))
                            .when(filter -> {
                                if (filter.getConjuncts().size() != 1) {
                                    return false;
                                }
                                Expression childExpr = filter.getConjuncts().iterator().next()
                                        .children().get(0);
                                if (childExpr instanceof SlotReference) {
                                    Optional<Column> column = ((SlotReference) childExpr).getOriginalColumn();
                                    return column.map(Column::isDeleteSignColumn).orElse(false);
                                }
                                return false;
                            })))
                        .when(agg -> enablePushDownMinMaxOnUnique())
                        .when(agg -> agg.getGroupByExpressions().isEmpty())
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty()
                                    && funcs.stream().allMatch(f -> (f instanceof Min) || (f instanceof Max));
                        })
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalProject<LogicalFilter<LogicalOlapScan>>> agg = ctx.root;
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = agg.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan olapScan = filter.child();
                            return pushdownMinMaxOnUniqueTable(agg, project, filter, olapScan,
                                    ctx.cascadesContext);
                        })
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT.build(
                logicalAggregate(
                    logicalOlapScan()
                )
                .when(agg -> agg.isNormalized() && enablePushDownNoGroupAgg())
                .thenApply(ctx -> storageLayerAggregate(ctx.root, null, ctx.root.child(), ctx.cascadesContext))
            ),
            RuleType.STORAGE_LAYER_WITH_PROJECT_NO_SLOT_REF.build(
                    logicalProject(
                            logicalOlapScan()
                    )
                    .thenApply(ctx -> {
                        LogicalProject<LogicalOlapScan> project = ctx.root;
                        LogicalOlapScan olapScan = project.child();
                        return pushDownCountWithoutSlotRef(project, olapScan, ctx.cascadesContext);
                    })
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT.build(
                logicalAggregate(
                    logicalProject(
                        logicalOlapScan()
                    )
                )
                .when(agg -> agg.isNormalized() && enablePushDownNoGroupAgg())
                .thenApply(ctx -> {
                    LogicalAggregate<LogicalProject<LogicalOlapScan>> agg = ctx.root;
                    LogicalProject<LogicalOlapScan> project = agg.child();
                    LogicalOlapScan olapScan = project.child();
                    return storageLayerAggregate(agg, project, olapScan, ctx.cascadesContext);
                })
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_WITHOUT_PROJECT_FOR_FILE_SCAN.build(
                logicalAggregate(
                    logicalFileScan()
                )
                    .when(agg -> agg.isNormalized() && enablePushDownNoGroupAgg())
                    .thenApply(ctx -> storageLayerAggregate(ctx.root, null, ctx.root.child(), ctx.cascadesContext))
            ),
            RuleType.STORAGE_LAYER_AGGREGATE_WITH_PROJECT_FOR_FILE_SCAN.build(
                logicalAggregate(
                    logicalProject(
                        logicalFileScan()
                    )
                ).when(agg -> agg.isNormalized() && enablePushDownNoGroupAgg())
                    .thenApply(ctx -> {
                        LogicalAggregate<LogicalProject<LogicalFileScan>> agg = ctx.root;
                        LogicalProject<LogicalFileScan> project = agg.child();
                        LogicalFileScan fileScan = project.child();
                        return storageLayerAggregate(agg, project, fileScan, ctx.cascadesContext);
                    })
            )
        );
    }

    /*
     *  select 66 from baseall_dup; could use pushAggOp=COUNT to not scan real data.
     */
    private LogicalProject<? extends Plan> pushDownCountWithoutSlotRef(
            LogicalProject<? extends Plan> project,
            LogicalOlapScan logicalScan,
            CascadesContext cascadesContext) {
        final LogicalProject<? extends Plan> canNotPush = project;
        if (!enablePushDownNoGroupAgg()) {
            return canNotPush;
        }
        if (logicalScan != null) {
            KeysType keysType = logicalScan.getTable().getKeysType();
            if (keysType != KeysType.DUP_KEYS) {
                return canNotPush;
            }
        }
        for (Expression e : project.getProjects()) {
            if (e.anyMatch(SlotReference.class::isInstance)) {
                return canNotPush;
            }
        }
        PhysicalOlapScan physicalOlapScan
                = (PhysicalOlapScan) new LogicalOlapScanToPhysicalOlapScan()
                .build()
                .transform(logicalScan, cascadesContext)
                .get(0);
        return project.withChildren(ImmutableList.of(new PhysicalStorageLayerAggregate(
                physicalOlapScan, PushDownAggOp.COUNT)));
    }

    private boolean enablePushDownMinMaxOnUnique() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext != null && connectContext.getSessionVariable().isEnablePushDownMinMaxOnUnique();
    }

    private boolean isUniqueKeyTable(LogicalOlapScan logicalScan) {
        if (logicalScan != null) {
            KeysType keysType = logicalScan.getTable().getKeysType();
            return keysType == KeysType.UNIQUE_KEYS;
        }
        return false;
    }

    private boolean enablePushDownCountOnIndex() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext != null && connectContext.getSessionVariable().isEnablePushDownCountOnIndex();
    }

    private boolean checkSlotInOrExpression(Expression expr, Set<Slot> aggSlots) {
        if (expr instanceof Or) {
            Set<Slot> slots = expr.getInputSlots();
            if (!slots.stream().allMatch(aggSlots::contains)) {
                return false;
            }
        } else {
            for (Expression child : expr.children()) {
                if (!checkSlotInOrExpression(child, aggSlots)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkIsNullExpr(Expression expr, Set<Slot> aggSlots) {
        if (expr instanceof IsNull) {
            Set<Slot> slots = expr.getInputSlots();
            if (slots.stream().anyMatch(aggSlots::contains)) {
                return false;
            }
        } else {
            for (Expression child : expr.children()) {
                if (!checkIsNullExpr(child, aggSlots)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDupOrMowKeyTable(LogicalOlapScan logicalScan) {
        if (logicalScan != null) {
            KeysType keysType = logicalScan.getTable().getKeysType();
            return (keysType == KeysType.DUP_KEYS)
                    || (keysType == KeysType.UNIQUE_KEYS && logicalScan.getTable().getEnableUniqueKeyMergeOnWrite());
        }
        return false;
    }

    private boolean isInvertedIndexEnabledOnTable(LogicalOlapScan logicalScan) {
        if (logicalScan == null) {
            return false;
        }

        OlapTable olapTable = logicalScan.getTable();
        Map<Long, MaterializedIndexMeta> indexIdToMeta = olapTable.getIndexIdToMeta();

        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            for (Index index : indexMeta.getIndexes()) {
                IndexType indexType = index.getIndexType();
                switch (indexType) {
                    case INVERTED:
                    case BITMAP:
                        return true;
                    default: {
                    }
                }
            }
        }
        return false;
    }

    /**
     * sql: select count(*) from tbl where column match 'token'
     * <p>
     * before:
     * <p>
     *               LogicalAggregate(groupBy=[], output=[count(*)])
     *                                |
     *                     LogicalFilter(column match 'token')
     *                                |
     *                       LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *               LogicalAggregate(groupBy=[], output=[count(*)])
     *                                |
     *                    LogicalFilter(column match 'token')
     *                                |
     *        PhysicalStorageLayerAggregate(pushAggOp=COUNT_ON_INDEX, table=PhysicalOlapScan(table=tbl))
     *
     */
    private LogicalAggregate<? extends Plan> pushdownCountOnIndex(
            LogicalAggregate<? extends Plan> agg,
            @Nullable LogicalProject<? extends Plan> project,
            LogicalFilter<? extends Plan> filter,
            LogicalOlapScan olapScan,
            CascadesContext cascadesContext) {

        PhysicalOlapScan physicalOlapScan = (PhysicalOlapScan) new LogicalOlapScanToPhysicalOlapScan()
                .build()
                .transform(olapScan, cascadesContext)
                .get(0);

        List<Expression> argumentsOfAggregateFunction = normalizeArguments(agg.getAggregateFunctions(), project);

        if (!onlyContainsSlotOrLiteral(argumentsOfAggregateFunction)) {
            return agg;
        }

        return agg.withChildren(ImmutableList.of(
                project != null
                        ? project.withChildren(ImmutableList.of(
                        filter.withChildren(ImmutableList.of(
                                new PhysicalStorageLayerAggregate(
                                        physicalOlapScan, PushDownAggOp.COUNT_ON_MATCH)))))
                        : filter.withChildren(ImmutableList.of(
                                new PhysicalStorageLayerAggregate(
                                        physicalOlapScan, PushDownAggOp.COUNT_ON_MATCH)))
        ));
    }

    private List<Expression> normalizeArguments(Set<AggregateFunction> aggregateFunctions,
            @Nullable LogicalProject<? extends Plan> project) {
        List<Expression> arguments = aggregateFunctions.stream()
                .flatMap(aggregateFunction -> aggregateFunction.getArguments().stream())
                .collect(ImmutableList.toImmutableList());

        if (project != null) {
            arguments = Project.findProject(arguments, project.getProjects())
                    .stream()
                    .map(p -> p instanceof Alias ? p.child(0) : p)
                    .collect(ImmutableList.toImmutableList());
        }

        return arguments;
    }

    private boolean onlyContainsSlotOrLiteral(List<Expression> arguments) {
        return arguments.stream().allMatch(argument -> {
            if (argument instanceof SlotReference || argument instanceof Literal) {
                return true;
            }
            return false;
        });
    }

    //select /*+SET_VAR(enable_pushdown_minmax_on_unique=true) */min(user_id) from table_unique;
    //push pushAggOp=MINMAX to scan node
    private LogicalAggregate<? extends Plan> pushdownMinMaxOnUniqueTable(
            LogicalAggregate<? extends Plan> aggregate,
            @Nullable LogicalProject<? extends Plan> project,
            LogicalFilter<? extends Plan> filter,
            LogicalOlapScan olapScan,
            CascadesContext cascadesContext) {
        final LogicalAggregate<? extends Plan> canNotPush = aggregate;
        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        if (checkWhetherPushDownMinMax(aggregateFunctions, project, olapScan.getOutput())) {
            PhysicalOlapScan physicalOlapScan = (PhysicalOlapScan) new LogicalOlapScanToPhysicalOlapScan()
                    .build()
                    .transform(olapScan, cascadesContext)
                    .get(0);
            if (project != null) {
                return aggregate.withChildren(ImmutableList.of(
                        project.withChildren(ImmutableList.of(
                                filter.withChildren(ImmutableList.of(
                                        new PhysicalStorageLayerAggregate(
                                                physicalOlapScan,
                                                PushDownAggOp.MIN_MAX)))))));
            } else {
                return aggregate.withChildren(ImmutableList.of(
                        filter.withChildren(ImmutableList.of(
                                new PhysicalStorageLayerAggregate(
                                        physicalOlapScan,
                                        PushDownAggOp.MIN_MAX)))));
            }
        } else {
            return canNotPush;
        }
    }

    private boolean checkWhetherPushDownMinMax(Set<AggregateFunction> aggregateFunctions,
            @Nullable LogicalProject<? extends Plan> project, List<Slot> outPutSlots) {
        boolean onlyContainsSlotOrNumericCastSlot = aggregateFunctions.stream()
                .map(ExpressionTrait::getArguments)
                .flatMap(List::stream)
                .allMatch(argument -> argument instanceof SlotReference);
        if (!onlyContainsSlotOrNumericCastSlot) {
            return false;
        }
        List<Expression> argumentsOfAggregateFunction = aggregateFunctions.stream()
                .flatMap(aggregateFunction -> aggregateFunction.getArguments().stream())
                .collect(ImmutableList.toImmutableList());

        if (project != null) {
            argumentsOfAggregateFunction = Project.findProject(
                    argumentsOfAggregateFunction, project.getProjects())
                    .stream()
                    .map(p -> p instanceof Alias ? p.child(0) : p)
                    .collect(ImmutableList.toImmutableList());
        }
        onlyContainsSlotOrNumericCastSlot = argumentsOfAggregateFunction
                .stream()
                .allMatch(argument -> argument instanceof SlotReference);
        if (!onlyContainsSlotOrNumericCastSlot) {
            return false;
        }
        Set<SlotReference> aggUsedSlots = ExpressionUtils.collect(argumentsOfAggregateFunction,
                SlotReference.class::isInstance);
        List<SlotReference> usedSlotInTable = (List<SlotReference>) Project.findProject(aggUsedSlots, outPutSlots);
        for (SlotReference slot : usedSlotInTable) {
            Column column = slot.getOriginalColumn().get();
            PrimitiveType colType = column.getType().getPrimitiveType();
            if (colType.isComplexType() || colType.isHllType() || colType.isBitmapType()) {
                return false;
            }
        }
        return true;
    }

    /**
     * sql: select count(*) from tbl
     * <p>
     * before:
     * <p>
     *               LogicalAggregate(groupBy=[], output=[count(*)])
     *                                |
     *                       LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *               LogicalAggregate(groupBy=[], output=[count(*)])
     *                                |
     *        PhysicalStorageLayerAggregate(pushAggOp=COUNT, table=PhysicalOlapScan(table=tbl))
     *
     */
    private LogicalAggregate<? extends Plan> storageLayerAggregate(
            LogicalAggregate<? extends Plan> aggregate,
            @Nullable LogicalProject<? extends Plan> project,
            LogicalRelation logicalScan, CascadesContext cascadesContext) {
        final LogicalAggregate<? extends Plan> canNotPush = aggregate;

        if (!(logicalScan instanceof LogicalOlapScan) && !(logicalScan instanceof LogicalFileScan)) {
            return canNotPush;
        }

        if (logicalScan instanceof LogicalOlapScan) {
            KeysType keysType = ((LogicalOlapScan) logicalScan).getTable().getKeysType();
            if (keysType != KeysType.AGG_KEYS && keysType != KeysType.DUP_KEYS) {
                return canNotPush;
            }
        }
        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        if (!groupByExpressions.isEmpty() || !aggregate.getDistinctArguments().isEmpty()) {
            return canNotPush;
        }

        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        Set<Class<? extends AggregateFunction>> functionClasses = aggregateFunctions
                .stream()
                .map(AggregateFunction::getClass)
                .collect(Collectors.toSet());

        Map<Class<? extends AggregateFunction>, PushDownAggOp> supportedAgg = PushDownAggOp.supportedFunctions();
        if (!supportedAgg.keySet().containsAll(functionClasses)) {
            return canNotPush;
        }
        if (logicalScan instanceof LogicalOlapScan) {
            LogicalOlapScan logicalOlapScan = (LogicalOlapScan) logicalScan;
            KeysType keysType = logicalOlapScan.getTable().getKeysType();
            if (functionClasses.contains(Count.class) && keysType != KeysType.DUP_KEYS) {
                return canNotPush;
            }
            if (functionClasses.contains(Count.class) && logicalOlapScan.isDirectMvScan()) {
                return canNotPush;
            }
        }
        if (aggregateFunctions.stream().anyMatch(fun -> fun.arity() > 1)) {
            return canNotPush;
        }

        // TODO: refactor this to process slot reference or expression together
        boolean onlyContainsSlotOrNumericCastSlot = aggregateFunctions.stream()
                .map(ExpressionTrait::getArguments)
                .flatMap(List::stream)
                .allMatch(argument -> {
                    if (argument instanceof SlotReference) {
                        return true;
                    }
                    if (argument instanceof Cast) {
                        return argument.child(0) instanceof SlotReference
                                && argument.getDataType().isNumericType()
                                && argument.child(0).getDataType().isNumericType();
                    }
                    return false;
                });
        if (!onlyContainsSlotOrNumericCastSlot) {
            return canNotPush;
        }

        // we already normalize the arguments to slotReference
        List<Expression> argumentsOfAggregateFunction = aggregateFunctions.stream()
                .flatMap(aggregateFunction -> aggregateFunction.getArguments().stream())
                .collect(ImmutableList.toImmutableList());

        if (project != null) {
            argumentsOfAggregateFunction = Project.findProject(
                        argumentsOfAggregateFunction, project.getProjects())
                    .stream()
                    .map(p -> p instanceof Alias ? p.child(0) : p)
                    .collect(ImmutableList.toImmutableList());
        }

        onlyContainsSlotOrNumericCastSlot = argumentsOfAggregateFunction
                .stream()
                .allMatch(argument -> {
                    if (argument instanceof SlotReference) {
                        return true;
                    }
                    if (argument instanceof Cast) {
                        return argument.child(0) instanceof SlotReference
                                && argument.getDataType().isNumericType()
                                && argument.child(0).getDataType().isNumericType();
                    }
                    return false;
                });
        if (!onlyContainsSlotOrNumericCastSlot) {
            return canNotPush;
        }

        Set<PushDownAggOp> pushDownAggOps = functionClasses.stream()
                .map(supportedAgg::get)
                .collect(Collectors.toSet());

        PushDownAggOp mergeOp = pushDownAggOps.size() == 1
                ? pushDownAggOps.iterator().next()
                : PushDownAggOp.MIX;

        Set<SlotReference> aggUsedSlots =
                ExpressionUtils.collect(argumentsOfAggregateFunction, SlotReference.class::isInstance);

        List<SlotReference> usedSlotInTable = (List<SlotReference>) Project.findProject(aggUsedSlots,
                logicalScan.getOutput());

        for (SlotReference slot : usedSlotInTable) {
            Column column = slot.getOriginalColumn().get();
            if (column.isAggregated()) {
                return canNotPush;
            }
            // The zone map max length of CharFamily is 512, do not
            // over the length: https://github.com/apache/doris/pull/6293
            if (mergeOp == PushDownAggOp.MIN_MAX || mergeOp == PushDownAggOp.MIX) {
                PrimitiveType colType = column.getType().getPrimitiveType();
                if (colType.isComplexType() || colType.isHllType() || colType.isBitmapType()
                         || (colType == PrimitiveType.STRING && !enablePushDownStringMinMax())) {
                    return canNotPush;
                }
                if (colType.isCharFamily() && column.getType().getLength() > 512 && !enablePushDownStringMinMax()) {
                    return canNotPush;
                }
            }
            if (mergeOp == PushDownAggOp.COUNT || mergeOp == PushDownAggOp.MIX) {
                // NULL value behavior in `count` function is zero, so
                // we should not use row_count to speed up query. the col
                // must be not null
                if (column.isAllowNull()) {
                    return canNotPush;
                }
            }
        }

        if (logicalScan instanceof LogicalOlapScan) {
            PhysicalOlapScan physicalScan = (PhysicalOlapScan) new LogicalOlapScanToPhysicalOlapScan()
                    .build()
                    .transform(logicalScan, cascadesContext)
                    .get(0);

            if (project != null) {
                return aggregate.withChildren(ImmutableList.of(
                    project.withChildren(
                        ImmutableList.of(new PhysicalStorageLayerAggregate(physicalScan, mergeOp)))
                ));
            } else {
                return aggregate.withChildren(ImmutableList.of(
                    new PhysicalStorageLayerAggregate(physicalScan, mergeOp)
                ));
            }

        } else if (logicalScan instanceof LogicalFileScan) {
            Rule rule = (logicalScan instanceof LogicalHudiScan) ? new LogicalHudiScanToPhysicalHudiScan().build()
                    : new LogicalFileScanToPhysicalFileScan().build();
            PhysicalFileScan physicalScan = (PhysicalFileScan) rule.transform(logicalScan, cascadesContext)
                    .get(0);
            if (project != null) {
                return aggregate.withChildren(ImmutableList.of(
                    project.withChildren(
                        ImmutableList.of(new PhysicalStorageLayerAggregate(physicalScan, mergeOp)))
                ));
            } else {
                return aggregate.withChildren(ImmutableList.of(
                    new PhysicalStorageLayerAggregate(physicalScan, mergeOp)
                ));
            }

        } else {
            return canNotPush;
        }
    }

    private boolean enablePushDownStringMinMax() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext != null && connectContext.getSessionVariable().isEnablePushDownStringMinMax();
    }

    private boolean enablePushDownNoGroupAgg() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext == null || connectContext.getSessionVariable().enablePushDownNoGroupAgg();
    }
}
