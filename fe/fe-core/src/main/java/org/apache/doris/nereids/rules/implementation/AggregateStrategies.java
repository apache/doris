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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
        PatternDescriptor<LogicalAggregate<GroupPlan>> basePattern = logicalAggregate();

        return ImmutableList.of(
            RuleType.COUNT_ON_INDEX_WITHOUT_PROJECT.build(
                logicalAggregate(
                    logicalFilter(
                        logicalOlapScan().when(this::isDupOrMowKeyTable).when(this::isInvertedIndexEnabledOnTable)
                    ).when(filter -> !filter.getConjuncts().isEmpty()))
                    .when(agg -> enablePushDownCountOnIndex())
                    .when(agg -> agg.getGroupByExpressions().isEmpty())
                    .when(agg -> {
                        Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                        return !funcs.isEmpty() && funcs.stream()
                                .allMatch(f -> f instanceof Count && !f.isDistinct());
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
                        ).when(filter -> !filter.getConjuncts().isEmpty())))
                    .when(agg -> enablePushDownCountOnIndex())
                    .when(agg -> agg.getGroupByExpressions().isEmpty())
                    .when(agg -> {
                        Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                        return !funcs.isEmpty() && funcs.stream().allMatch(f -> f instanceof Count && !f.isDistinct());
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
                                        Optional<Column> column = ((SlotReference) childExpr).getColumn();
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
                    logicalAggregate(
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan().when(this::isUniqueKeyTable))
                                            .when(filter -> {
                                                if (filter.getConjuncts().size() != 1) {
                                                    return false;
                                                }
                                                Expression childExpr = filter.getConjuncts().iterator().next()
                                                        .children().get(0);
                                                if (childExpr instanceof SlotReference) {
                                                    Optional<Column> column = ((SlotReference) childExpr).getColumn();
                                                    return column.map(Column::isDeleteSignColumn).orElse(false);
                                                }
                                                return false;
                                            }))
                        )
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
            ),
            RuleType.ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().isEmpty())
                    .thenApplyMulti(ctx -> onePhaseAggregateWithoutDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().isEmpty())
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithoutDistinct(ctx.root, ctx.connectContext))
            ),
            // RuleType.TWO_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI.build(
            //     basePattern
            //         .when(this::containsCountDistinctMultiExpr)
            //         .thenApplyMulti(ctx -> twoPhaseAggregateWithCountDistinctMulti(ctx.root, ctx.cascadesContext))
            // ),
            RuleType.THREE_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI.build(
                basePattern
                    .when(this::containsCountDistinctMultiExpr)
                    .thenApplyMulti(ctx -> threePhaseAggregateWithCountDistinctMulti(ctx.root, ctx.cascadesContext))
            ),
            RuleType.ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1 && couldConvertToMulti(agg))
                    .thenApplyMulti(ctx -> onePhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1 && couldConvertToMulti(agg))
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            RuleType.TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() > 1
                            && !containsCountDistinctMultiExpr(agg)
                            && couldConvertToMulti(agg))
                    .thenApplyMulti(ctx -> twoPhaseAggregateWithMultiDistinct(ctx.root, ctx.connectContext))
            ),
            // RuleType.TWO_PHASE_AGGREGATE_WITH_DISTINCT.build(
            //     basePattern
            //         .when(agg -> agg.getDistinctArguments().size() == 1)
            //         .thenApplyMulti(ctx -> twoPhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            // ),
            RuleType.THREE_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                     .whenNot(agg -> agg.mustUseMultiDistinctAgg())
                    .thenApplyMulti(ctx -> threePhaseAggregateWithDistinct(ctx.root, ctx.connectContext))
            ),
            /*
             * sql:
             * select count(distinct name), sum(age) from student;
             * <p>
             * 4 phase plan
             * DISTINCT_GLOBAL(BUFFER_TO_RESULT, groupBy(),
             *          output[count(partial_count(name)), sum(partial_sum(partial_sum(age)))],
             *          GATHER)
             * +--DISTINCT_LOCAL(INPUT_TO_BUFFER, groupBy(),
             *          output(partial_count(name), partial_sum(partial_sum(age))),
             *          hash distribute by name)
             *    +--GLOBAL(BUFFER_TO_BUFFER, groupBy(name),
             *          output(name, partial_sum(age)),
             *          hash_distribute by name)
             *       +--LOCAL(INPUT_TO_BUFFER, groupBy(name), output(name, partial_sum(age)))
             *          +--scan(name, age)
             */
            RuleType.FOUR_PHASE_AGGREGATE_WITH_DISTINCT.build(
                basePattern
                    .when(agg -> agg.getDistinctArguments().size() == 1)
                    .when(agg -> agg.getGroupByExpressions().isEmpty())
                    .whenNot(agg -> agg.mustUseMultiDistinctAgg())
                    .thenApplyMulti(ctx -> {
                        Function<List<Expression>, RequireProperties> secondPhaseRequireDistinctHash =
                                groupByAndDistinct -> RequireProperties.of(
                                        PhysicalProperties.createHash(
                                                ctx.root.getDistinctArguments(), ShuffleType.REQUIRE
                                        )
                                );
                        Function<LogicalAggregate<? extends Plan>, RequireProperties> fourPhaseRequireGather =
                                agg -> RequireProperties.of(PhysicalProperties.GATHER);
                        return fourPhaseAggregateWithDistinct(
                                ctx.root, ctx.connectContext,
                                secondPhaseRequireDistinctHash, fourPhaseRequireGather
                        );
                    })
            ),
            /*
             * sql:
             * select age, count(distinct name) from student group by age;
             * <p>
             * 4 phase plan
             * DISTINCT_GLOBAL(BUFFER_TO_RESULT, groupBy(age),
             *          output[age, sum(partial_count(name))],
             *          hash distribute by name)
             * +--DISTINCT_LOCAL(INPUT_TO_BUFFER, groupBy(age),
             *          output(age, partial_count(name)),
             *          hash distribute by age, name)
             *    +--GLOBAL(BUFFER_TO_BUFFER, groupBy(age, name),
             *          output(age, name),
             *          hash_distribute by age, name)
             *       +--LOCAL(INPUT_TO_BUFFER, groupBy(age, name), output(age, name))
             *          +--scan(age, name)
             */
            RuleType.FOUR_PHASE_AGGREGATE_WITH_DISTINCT_WITH_FULL_DISTRIBUTE.build(
                basePattern
                    .when(agg -> agg.everyDistinctArgumentNumIsOne() && !agg.getGroupByExpressions().isEmpty())
                    .when(agg ->
                        ImmutableSet.builder()
                            .addAll(agg.getGroupByExpressions())
                            .addAll(agg.getDistinctArguments())
                            .build().size() > agg.getGroupByExpressions().size()
                    )
                    .when(agg -> {
                        if (agg.getDistinctArguments().size() == 1) {
                            return true;
                        }
                        return couldConvertToMulti(agg);
                    })
                    .thenApplyMulti(ctx -> {
                        Function<List<Expression>, RequireProperties> secondPhaseRequireGroupByAndDistinctHash =
                                groupByAndDistinct -> RequireProperties.of(
                                        PhysicalProperties.createHash(groupByAndDistinct, ShuffleType.REQUIRE)
                                );

                        Function<LogicalAggregate<? extends Plan>, RequireProperties> fourPhaseRequireGroupByHash =
                                agg -> RequireProperties.of(
                                        PhysicalProperties.createHash(
                                                agg.getGroupByExpressions(), ShuffleType.REQUIRE
                                        )
                                );
                        return fourPhaseAggregateWithDistinct(
                                ctx.root, ctx.connectContext,
                                secondPhaseRequireGroupByAndDistinctHash, fourPhaseRequireGroupByHash
                        );
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

        return indexIdToMeta.values().stream()
                .anyMatch(indexMeta -> indexMeta.getIndexes().stream()
                        .anyMatch(index -> index.getIndexType() == IndexType.INVERTED
                                || index.getIndexType() == IndexType.BITMAP));
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
        PhysicalOlapScan physicalOlapScan
                = (PhysicalOlapScan) new LogicalOlapScanToPhysicalOlapScan()
                .build()
                .transform(olapScan, cascadesContext)
                .get(0);
        if (project != null) {
            return agg.withChildren(ImmutableList.of(
                    project.withChildren(ImmutableList.of(
                            filter.withChildren(ImmutableList.of(
                                    new PhysicalStorageLayerAggregate(
                                            physicalOlapScan,
                                            PushDownAggOp.COUNT_ON_MATCH)))))
            ));
        } else {
            return agg.withChildren(ImmutableList.of(
                            filter.withChildren(ImmutableList.of(
                                    new PhysicalStorageLayerAggregate(
                                            physicalOlapScan,
                                            PushDownAggOp.COUNT_ON_MATCH)))));
        }
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
            Column column = slot.getColumn().get();
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
            Column column = slot.getColumn().get();
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

    /**
     * sql: select count(*) from tbl group by id
     * <p>
     * before:
     * <p>
     *          LogicalAggregate(groupBy=[id], output=[count(*)])
     *                       |
     *               LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *             PhysicalHashAggregate(groupBy=[id], output=[count(*)])
     *                              |
     *                 PhysicalDistribute(distributionSpec=GATHER)
     *                             |
     *                     LogicalOlapScan(table=tbl)
     * <p>
     *  distribute node aggregate:
     * <p>
     *            PhysicalHashAggregate(groupBy=[id], output=[count(*)])
     *                                    |
     *           LogicalOlapScan(table=tbl, **already distribute by id**)
     *
     */
    private List<PhysicalHashAggregate<Plan>> onePhaseAggregateWithoutDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        AggregateParam inputToResultParam = AggregateParam.LOCAL_RESULT;
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        return new AggregateExpression((AggregateFunction) outputChild, inputToResultParam);
                    }
                    return outputChild;
                });
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), newOutput, Optional.empty(),
                inputToResultParam, false,
                logicalAgg.getLogicalProperties(),
                requireGather, logicalAgg.child());

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            // TODO: usually bad, disable it until we could do better cost computation.
            // return ImmutableList.of(gatherLocalAgg);
            return ImmutableList.of();
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<Plan> hashLocalAgg = gatherLocalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    // TODO: usually bad, disable it until we could do better cost computation.
                    //.add(gatherLocalAgg)
                    .add(hashLocalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id, name) from tbl group by name
     * <p>
     * before:
     * <p>
     *          LogicalAggregate(groupBy=[name], output=[count(distinct id, name)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id])
     *                                |
     *           PhysicalDistribute(distributionSpec=GATHER)
     *                               |
     *                     LogicalOlapScan(table=tbl)
     * <p>
     *  distribute node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id])
     *                                |
     *           PhysicalDistribute(distributionSpec=HASH(name))
     *                               |
     *        LogicalOlapScan(table=tbl, **already distribute by name**)
     *
     */
    private List<PhysicalHashAggregate<Plan>> twoPhaseAggregateWithCountDistinctMulti(
            LogicalAggregate<? extends Plan> logicalAgg, CascadesContext cascadesContext) {
        AggregateParam inputToBufferParam = AggregateParam.LOCAL_BUFFER;
        Collection<Expression> countDistinctArguments = logicalAgg.getDistinctArguments();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(ImmutableSet.<Expression>builder()
                .addAll(logicalAgg.getGroupByExpressions())
                .addAll(countDistinctArguments)
                .build());

        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr);
                }));

        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        List<NamedExpression> localOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) localAggGroupBy.stream()
                        .filter(g -> !(g instanceof Literal))
                        .collect(ImmutableList.toImmutableList()))
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localOutput, Optional.of(partitionExpressions),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                maybeUsingStreamAgg(cascadesContext.getConnectContext(), logicalAgg),
                logicalAgg.getLogicalProperties(), requireGather, logicalAgg.child()
        );

        List<Expression> distinctGroupBy = logicalAgg.getGroupByExpressions();

        LogicalAggregate<? extends Plan> countIfAgg = countDistinctMultiExprToCountIf(
                logicalAgg, cascadesContext).first;

        AggregateParam distinctInputToResultParam
                = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_RESULT);
        AggregateParam globalBufferToResultParam
                = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                countIfAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                        Alias alias = nonDistinctAggFunctionToAliasPhase1.get(aggregateFunction);
                        if (alias == null) {
                            return new AggregateExpression(aggregateFunction, distinctInputToResultParam);
                        } else {
                            return new AggregateExpression(aggregateFunction,
                                    globalBufferToResultParam, alias.toSlot());
                        }
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<Plan> gatherLocalGatherDistinctAgg = new PhysicalHashAggregate<>(
                distinctGroupBy, distinctOutput, Optional.of(partitionExpressions),
                distinctInputToResultParam, false,
                logicalAgg.getLogicalProperties(), requireGather, gatherLocalAgg
        );

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of(gatherLocalGatherDistinctAgg);
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<Plan> hashLocalHashGlobalAgg = gatherLocalGatherDistinctAgg
                    .withRequireTree(requireHash.withChildren(requireHash))
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    // TODO: usually bad, disable it until we could do better cost computation.
                    //.add(gatherLocalGatherDistinctAgg)
                    .add(hashLocalHashGlobalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id, name) from tbl group by name
     * <p>
     * before:
     * <p>
     *          LogicalAggregate(groupBy=[name], output=[count(distinct id, name)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                   |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                   |
     *                PhysicalDistribute(distributionSpec=GATHER)
     *                                   |
     *       PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                   |
     *                        LogicalOlapScan(table=tbl)
     * <p>
     *  distribute node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[count(if(id is null, null, name))])
     *                                   |
     *          PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                   |
     *                PhysicalDistribute(distributionSpec=HASH(name))
     *                                   |
     *       PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                   |
     *                        LogicalOlapScan(table=tbl)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> threePhaseAggregateWithCountDistinctMulti(
            LogicalAggregate<? extends Plan> logicalAgg, CascadesContext cascadesContext) {
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);

        Collection<Expression> countDistinctArguments = logicalAgg.getDistinctArguments();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(ImmutableSet.<Expression>builder()
                .addAll(logicalAgg.getGroupByExpressions())
                .addAll(countDistinctArguments)
                .build());

        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr);
                }));

        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        List<NamedExpression> localOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) localAggGroupBy.stream()
                        .filter(g -> !(g instanceof Literal))
                        .collect(ImmutableList.toImmutableList()))
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();
        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localOutput, Optional.of(partitionExpressions),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                maybeUsingStreamAgg(cascadesContext.getConnectContext(), logicalAgg),
                logicalAgg.getLogicalProperties(), requireAny, logicalAgg.child()
        );

        List<Expression> globalAggGroupBy = localAggGroupBy;

        boolean hasCountDistinctMulti = logicalAgg.getAggregateFunctions().stream()
                .filter(AggregateFunction::isDistinct)
                .filter(Count.class::isInstance)
                .anyMatch(c -> c.arity() > 1);
        AggregateParam bufferToBufferParam = new AggregateParam(
                AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER, !hasCountDistinctMulti);

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase2 =
                nonDistinctAggFunctionToAliasPhase1.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(Entry::getKey, kv -> {
                            AggregateFunction originFunction = kv.getKey();
                            Alias localOutputAlias = kv.getValue();
                            AggregateExpression globalAggExpr = new AggregateExpression(
                                    originFunction, bufferToBufferParam, localOutputAlias.toSlot());
                            return new Alias(globalAggExpr);
                        }));

        Set<SlotReference> slotInCountDistinct = ExpressionUtils.collect(
                ImmutableList.copyOf(countDistinctArguments), SlotReference.class::isInstance);
        List<NamedExpression> globalAggOutput = ImmutableList.copyOf(ImmutableSet.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(slotInCountDistinct)
                .addAll(nonDistinctAggFunctionToAliasPhase2.values())
                .build());

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                globalAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                requireGather, anyLocalAgg);

        LogicalAggregate<? extends Plan> countIfAgg = countDistinctMultiExprToCountIf(
                logicalAgg, cascadesContext).first;

        AggregateParam distinctInputToResultParam
                = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_RESULT, !hasCountDistinctMulti);
        AggregateParam globalBufferToResultParam
                = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                countIfAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                        Alias alias = nonDistinctAggFunctionToAliasPhase2.get(aggregateFunction);
                        if (alias == null) {
                            return new AggregateExpression(aggregateFunction, distinctInputToResultParam);
                        } else {
                            return new AggregateExpression(aggregateFunction,
                                    globalBufferToResultParam, alias.toSlot());
                        }
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<Plan> anyLocalGatherGlobalGatherAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), distinctOutput, Optional.empty(),
                distinctInputToResultParam, false,
                logicalAgg.getLogicalProperties(), requireGather, anyLocalGatherGlobalAgg
        );

        // RequireProperties requireDistinctHash = RequireProperties.of(
        //         PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
        // PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalGatherDistinctAgg
        //         = anyLocalGatherGlobalGatherAgg.withChildren(ImmutableList.of(
        //                 anyLocalGatherGlobalAgg
        //                         .withRequire(requireDistinctHash)
        //                         .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
        //         ));

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalGatherAgg)
                    //.add(anyLocalHashGlobalGatherDistinctAgg)
                    .build();
        } else {
            RequireProperties requireGroupByHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<PhysicalHashAggregate<Plan>> anyLocalHashGlobalHashDistinctAgg
                    = anyLocalGatherGlobalGatherAgg.withRequirePropertiesAndChild(requireGroupByHash,
                            anyLocalGatherGlobalAgg
                                    .withRequire(requireGroupByHash)
                                    .withPartitionExpressions(logicalAgg.getGroupByExpressions())
                    )
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // .add(anyLocalGatherGlobalGatherAgg)
                    // .add(anyLocalHashGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalHashDistinctAgg)
                    .build();
        }
    }

    /**
     * sql: select name, count(value) from tbl group by name
     * <p>
     * before:
     * <p>
     *          LogicalAggregate(groupBy=[name], output=[name, count(value)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=BUFFER_TO_RESULT)
     *                                |
     *               PhysicalDistribute(distributionSpec=GATHER)
     *                                |
     *          PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=INPUT_TO_BUFFER)
     *                                |
     *                     LogicalOlapScan(table=tbl)
     * <p>
     *  distribute node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=BUFFER_TO_RESULT)
     *                                |
     *               PhysicalDistribute(distributionSpec=HASH(name))
     *                                |
     *          PhysicalHashAggregate(groupBy=[name], output=[name, count(value)], mode=INPUT_TO_BUFFER)
     *                                |
     *                     LogicalOlapScan(table=tbl)
     *
     */
    private List<PhysicalHashAggregate<Plan>> twoPhaseAggregateWithoutDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> inputToBufferAliases = logicalAgg.getAggregateFunctions()
                .stream()
                .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                    AggregateExpression inputToBuffer = new AggregateExpression(function, inputToBufferParam);
                    return new Alias(inputToBuffer);
                }));

        List<Expression> localAggGroupBy = logicalAgg.getGroupByExpressions();
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                // we already normalized the group by expressions to List<Slot> by the NormalizeAggregate rule
                .addAll((List) localAggGroupBy)
                .addAll(inputToBufferAliases.values())
                .build();

        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<? extends Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, localAggOutput, Optional.of(partitionExpressions),
                inputToBufferParam, maybeUsingStreamAgg(connectContext, logicalAgg),
                logicalAgg.getLogicalProperties(), requireAny,
                logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalAggOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (!(outputChild instanceof AggregateFunction)) {
                        return outputChild;
                    }
                    Alias inputToBufferAlias = inputToBufferAliases.get(outputChild);
                    if (inputToBufferAlias == null) {
                        return outputChild;
                    }
                    AggregateFunction function = (AggregateFunction) outputChild;
                    return new AggregateExpression(function, bufferToResultParam, inputToBufferAlias.toSlot());
                });

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToResultParam, false, anyLocalAgg.getLogicalProperties(),
                requireGather, anyLocalAgg);

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of(anyLocalGatherGlobalAgg);
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));

            PhysicalHashAggregate<Plan> anyLocalHashGlobalAgg = anyLocalGatherGlobalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<Plan>>builder()
                    // TODO: usually bad, disable it until we could do better cost computation.
                    // .add(anyLocalGatherGlobalAgg)
                    .add(anyLocalHashGlobalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from tbl group by name
     * <p>
     * before:
     * <p>
     *               LogicalAggregate(groupBy=[name], output=[name, count(distinct id)])
     *                                         |
     *                              LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id))], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                     PhysicalDistribute(distributionSpec=GATHER)
     *                                          |
     *                               LogicalOlapScan(table=tbl)
     * <p>
     * distribute node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id))], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                LogicalOlapScan(table=tbl, **if distribute by name**)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> twoPhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Set<NamedExpression> distinctArguments = aggregateFunctions.stream()
                .filter(AggregateFunction::isDistinct)
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBy = ImmutableSet.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = AggregateParam.LOCAL_BUFFER;

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr);
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBy)
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);

        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        PhysicalHashAggregate<Plan> gatherLocalAgg = new PhysicalHashAggregate<>(ImmutableList.copyOf(localAggGroupBy),
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                /*
                 * should not use streaming, there has some bug in be will compute wrong result,
                 * see aggregate_strategies.groovy
                 */
                false, Optional.empty(), logicalAgg.getLogicalProperties(),
                requireGather, logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) outputChild;
                        if (aggregateFunction.isDistinct()) {
                            Set<Expression> aggChild = Sets.newLinkedHashSet(aggregateFunction.children());
                            Preconditions.checkArgument(aggChild.size() == 1
                                            || aggregateFunction.getDistinctArguments().size() == 1,
                                    "cannot process more than one child in aggregate distinct function: "
                                            + aggregateFunction);
                            AggregateFunction nonDistinct = aggregateFunction
                                    .withDistinctAndChildren(false, ImmutableList.copyOf(aggChild));
                            return new AggregateExpression(nonDistinct, AggregateParam.LOCAL_RESULT);
                        } else {
                            Alias alias = nonDistinctAggFunctionToAliasPhase1.get(outputChild);
                            return new AggregateExpression(aggregateFunction, bufferToResultParam, alias.toSlot());
                        }
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<Plan> gatherLocalGatherGlobalAgg
                = new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), globalOutput,
                Optional.empty(), bufferToResultParam, false,
                logicalAgg.getLogicalProperties(), requireGather, gatherLocalAgg);

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            RequireProperties requireDistinctHash = RequireProperties.of(PhysicalProperties.createHash(
                    distinctArguments, ShuffleType.REQUIRE));
            PhysicalHashAggregate<? extends Plan> hashLocalGatherGlobalAgg = gatherLocalGatherGlobalAgg
                    .withChildren(ImmutableList.of(gatherLocalAgg
                            .withRequire(requireDistinctHash)
                            .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
                    ));
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    //.add(gatherLocalGatherGlobalAgg)
                    .add(hashLocalGatherGlobalAgg)
                    .build();
        } else {
            RequireProperties requireGroupByHash = RequireProperties.of(PhysicalProperties.createHash(
                    logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<PhysicalHashAggregate<Plan>> hashLocalHashGlobalAgg = gatherLocalGatherGlobalAgg
                    .withRequirePropertiesAndChild(requireGroupByHash, gatherLocalAgg
                            .withRequire(requireGroupByHash)
                            .withPartitionExpressions(logicalAgg.getGroupByExpressions())
                    )
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // .add(gatherLocalGatherGlobalAgg)
                    .add(hashLocalHashGlobalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from tbl group by name
     * <p>
     * before:
     * <p>
     *               LogicalAggregate(groupBy=[name], output=[name, count(distinct id)])
     *                                         |
     *                              LogicalOlapScan(table=tbl)
     * <p>
     * after:
     *  single node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id))], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                          |
     *                     PhysicalDistribute(distributionSpec=GATHER)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                               LogicalOlapScan(table=tbl)
     * <p>
     *  distribute node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, count(distinct(id))], mode=BUFFER_TO_RESULT)
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=BUFFER_TO_BUFFER)
     *                                          |
     *                     PhysicalDistribute(distributionSpec=HASH(name))
     *                                          |
     *     PhysicalHashAggregate(groupBy=[name, id], output=[name, id], mode=INPUT_TO_BUFFER)
     *                                          |
     *                               LogicalOlapScan(table=tbl)
     *
     */
    // TODO: support one phase aggregate(group by columns + distinct columns) + two phase distinct aggregate
    private List<PhysicalHashAggregate<? extends Plan>> threePhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        boolean couldBanned = couldConvertToMulti(logicalAgg);

        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Set<NamedExpression> distinctArguments = aggregateFunctions.stream()
                .filter(AggregateFunction::isDistinct)
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBySet = ImmutableSet.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER, couldBanned);

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr);
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(localAggGroupBySet);
        boolean isGroupByEmptySelectEmpty = localAggGroupBy.isEmpty() && localAggOutput.isEmpty();

        // be not recommend generate an aggregate node with empty group by and empty output,
        // so add a null int slot to group by slot and output
        if (isGroupByEmptySelectEmpty) {
            localAggGroupBy = ImmutableList.of(new NullLiteral(TinyIntType.INSTANCE));
            localAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }

        boolean maybeUsingStreamAgg = maybeUsingStreamAgg(connectContext, localAggGroupBy);
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(localAggGroupBy,
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                maybeUsingStreamAgg, Optional.empty(), logicalAgg.getLogicalProperties(),
                requireAny, logicalAgg.child());

        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER, couldBanned);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase2 =
                nonDistinctAggFunctionToAliasPhase1.entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(Entry::getKey, kv -> {
                        AggregateFunction originFunction = kv.getKey();
                        Alias localOutput = kv.getValue();
                        AggregateExpression globalAggExpr = new AggregateExpression(
                                originFunction, bufferToBufferParam, localOutput.toSlot());
                        return new Alias(globalAggExpr);
                    }));

        List<NamedExpression> globalAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase2.values())
                .build();

        // be not recommend generate an aggregate node with empty group by and empty output,
        // so add a null int slot to group by slot and output
        if (isGroupByEmptySelectEmpty) {
            globalAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, globalAggOutput, Optional.of(partitionExpressions),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                requireGather, anyLocalAgg);

        AggregateParam bufferToResultParam = new AggregateParam(
                AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_RESULT, couldBanned);
        List<NamedExpression> distinctOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) expr;
                        if (aggregateFunction.isDistinct()) {
                            Set<Expression> aggChild = Sets.newLinkedHashSet(aggregateFunction.children());
                            Preconditions.checkArgument(aggChild.size() == 1
                                            || aggregateFunction.getDistinctArguments().size() == 1,
                                    "cannot process more than one child in aggregate distinct function: "
                                            + aggregateFunction);
                            AggregateFunction nonDistinct = aggregateFunction
                                    .withDistinctAndChildren(false, ImmutableList.copyOf(aggChild));
                            return new AggregateExpression(nonDistinct, bufferToResultParam, aggregateFunction);
                        } else {
                            Alias alias = nonDistinctAggFunctionToAliasPhase2.get(expr);
                            return new AggregateExpression(aggregateFunction,
                                    new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_RESULT),
                                    alias.toSlot());
                        }
                    }
                    return expr;
                });

        PhysicalHashAggregate<Plan> anyLocalGatherGlobalGatherDistinctAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), distinctOutput, Optional.empty(),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                requireGather, anyLocalGatherGlobalAgg);

        RequireProperties requireDistinctHash = RequireProperties.of(
                PhysicalProperties.createHash(logicalAgg.getDistinctArguments(), ShuffleType.REQUIRE));
        PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalGatherDistinctAgg
                = anyLocalGatherGlobalGatherDistinctAgg
                    .withChildren(ImmutableList.of(anyLocalGatherGlobalAgg
                            .withRequire(requireDistinctHash)
                            .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
                    ));

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // TODO: this plan pattern is not good usually, we remove it temporary.
                    // .add(anyLocalGatherGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalGatherDistinctAgg)
                    .build();
        } else {
            RequireProperties requireGroupByHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalHashDistinctAgg
                    = anyLocalGatherGlobalGatherDistinctAgg
                    .withRequirePropertiesAndChild(requireGroupByHash, anyLocalGatherGlobalAgg
                            .withRequire(requireGroupByHash)
                            .withPartitionExpressions(logicalAgg.getGroupByExpressions())
                    )
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // TODO: this plan pattern is not good usually, we remove it temporary.
                    //.add(anyLocalGatherGlobalGatherDistinctAgg)
                    //.add(anyLocalHashGlobalGatherDistinctAgg)
                    .add(anyLocalHashGlobalHashDistinctAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from (...) group by name
     * <p>
     * before:
     * <p>
     *          LogicalAggregate(groupBy=[name], output=[count(distinct id)])
     *                       |
     *                    any plan
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *             PhysicalHashAggregate(groupBy=[name], output=[multi_distinct_count(id)])
     *                                    |
     *                 PhysicalDistribute(distributionSpec=GATHER)
     *                                    |
     *                                any plan
     * <p>
     *  distribute node aggregate:
     * <p>
     *            PhysicalHashAggregate(groupBy=[name], output=[multi_distinct_count(id)])
     *                                    |
     *                     any plan(**already distribute by name**)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> onePhaseAggregateWithMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        AggregateParam inputToResultParam = AggregateParam.LOCAL_RESULT;
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        AggregateFunction function = tryConvertToMultiDistinct((AggregateFunction) outputChild);
                        return new AggregateExpression(function, inputToResultParam);
                    }
                    return outputChild;
                });

        RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);
        PhysicalHashAggregate<? extends Plan> gatherLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), newOutput, inputToResultParam,
                maybeUsingStreamAgg(connectContext, logicalAgg),
                logicalAgg.getLogicalProperties(), requireGather, logicalAgg.child());
        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            // TODO: usually bad, disable it until we could do better cost computation.
            // return ImmutableList.of(gatherLocalAgg);
            return ImmutableList.of();
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<? extends Plan> hashLocalAgg = gatherLocalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // TODO: usually bad, disable it until we could do better cost computation.
                    // .add(gatherLocalAgg)
                    .add(hashLocalAgg)
                    .build();
        }
    }

    /**
     * sql: select count(distinct id) from tbl group by name
     * <p>
     * before:
     * <p>
     *          LogicalAggregate(groupBy=[name], output=[name, count(distinct id)])
     *                               |
     *                       LogicalOlapScan(table=tbl)
     * <p>
     * after:
     * <p>
     *  single node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=BUFFER_TO_RESULT)
     *                                                 |
     *                                PhysicalDistribute(distributionSpec=GATHER)
     *                                                 |
     *     PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=INPUT_TO_BUFFER)
     *                                                 |
     *                                       LogicalOlapScan(table=tbl)
     * <p>
     *  distribute node aggregate:
     * <p>
     *     PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=BUFFER_TO_RESULT)
     *                                                |
     *                               PhysicalDistribute(distributionSpec=HASH(name))
     *                                                |
     *      PhysicalHashAggregate(groupBy=[name], output=[name, multi_count_distinct(value)], mode=INPUT_TO_BUFFER)
     *                                                |
     *                                      LogicalOlapScan(table=tbl)
     *
     */
    private List<PhysicalHashAggregate<? extends Plan>> twoPhaseAggregateWithMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> aggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                    AggregateFunction multiDistinct = tryConvertToMultiDistinct(function);
                    AggregateExpression localAggExpr = new AggregateExpression(multiDistinct, inputToBufferParam);
                    return new Alias(localAggExpr);
                }));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                // already normalize group by expression to List<SlotReference>
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(aggFunctionToAliasPhase1.values())
                .build();

        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
        PhysicalHashAggregate<? extends Plan> anyLocalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), localAggOutput,
                inputToBufferParam, maybeUsingStreamAgg(connectContext, logicalAgg),
                logicalAgg.getLogicalProperties(), requireAny, logicalAgg.child());

        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        Alias alias = aggFunctionToAliasPhase1.get(outputChild);
                        AggregateExpression localAggExpr = (AggregateExpression) alias.child();
                        return new AggregateExpression(localAggExpr.getFunction(),
                                bufferToResultParam, alias.toSlot());
                    } else {
                        return outputChild;
                    }
                });

        PhysicalHashAggregate<? extends Plan> anyLocalGatherGlobalAgg = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), globalOutput, Optional.empty(),
                bufferToResultParam, false, logicalAgg.getLogicalProperties(),
                RequireProperties.of(PhysicalProperties.GATHER), anyLocalAgg);

        if (logicalAgg.getGroupByExpressions().isEmpty()) {
            // Collection<Expression> distinctArguments = logicalAgg.getDistinctArguments();
            // RequireProperties requireDistinctHash = RequireProperties.of(PhysicalProperties.createHash(
            //         distinctArguments, ShuffleType.REQUIRE));
            // PhysicalHashAggregate<? extends Plan> hashLocalGatherGlobalAgg = anyLocalGatherGlobalAgg
            //         .withChildren(ImmutableList.of(anyLocalAgg
            //                 .withRequire(requireDistinctHash)
            //                 .withPartitionExpressions(ImmutableList.copyOf(logicalAgg.getDistinctArguments()))
            //         ));
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    .add(anyLocalGatherGlobalAgg)
                    .build();
        } else {
            RequireProperties requireHash = RequireProperties.of(
                    PhysicalProperties.createHash(logicalAgg.getGroupByExpressions(), ShuffleType.REQUIRE));
            PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalAgg = anyLocalGatherGlobalAgg
                    .withRequire(requireHash)
                    .withPartitionExpressions(logicalAgg.getGroupByExpressions());
            return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                    // TODO: usually bad, disable it until we could do better cost computation.
                    // .add(anyLocalGatherGlobalAgg)
                    .add(anyLocalHashGlobalAgg)
                    .build();
        }
    }

    private boolean maybeUsingStreamAgg(
            ConnectContext connectContext, LogicalAggregate<? extends Plan> logicalAggregate) {
        return !connectContext.getSessionVariable().disableStreamPreaggregations
                && !logicalAggregate.getGroupByExpressions().isEmpty();
    }

    private boolean maybeUsingStreamAgg(
            ConnectContext connectContext, List<? extends Expression> groupByExpressions) {
        return !connectContext.getSessionVariable().disableStreamPreaggregations
                && !groupByExpressions.isEmpty();
    }

    private List<Expression> getHashAggregatePartitionExpressions(
            LogicalAggregate<? extends Plan> logicalAggregate) {
        return logicalAggregate.getGroupByExpressions().isEmpty()
                ? ImmutableList.copyOf(logicalAggregate.getDistinctArguments())
                : logicalAggregate.getGroupByExpressions();
    }

    private AggregateFunction tryConvertToMultiDistinct(AggregateFunction function) {
        if (function instanceof Count && function.isDistinct()) {
            return new MultiDistinctCount(function.getArgument(0),
                    function.getArguments().subList(1, function.arity()).toArray(new Expression[0]));
        } else if (function instanceof Sum && function.isDistinct()) {
            return ((Sum) function).convertToMultiDistinct();
        } else if (function instanceof GroupConcat && function.isDistinct()) {
            return ((GroupConcat) function).convertToMultiDistinct();
        }
        return function;
    }

    /**
     * countDistinctMultiExprToCountIf.
     * <p>
     * NOTE: this function will break the normalized output, e.g. from `count(distinct slot1, slot2)` to
     *       `count(if(slot1 is null, null, slot2))`. So if you invoke this method, and separate the
     *       phase of aggregate, please normalize to slot and create a bottom project like NormalizeAggregate.
     */
    private Pair<LogicalAggregate<? extends Plan>, List<Count>> countDistinctMultiExprToCountIf(
                LogicalAggregate<? extends Plan> aggregate, CascadesContext cascadesContext) {
        ImmutableList.Builder<Count> countIfList = ImmutableList.builder();
        List<NamedExpression> newOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof Count) {
                        Count count = (Count) outputChild;
                        if (count.isDistinct() && count.arity() > 1) {
                            Set<Expression> arguments = ImmutableSet.copyOf(count.getArguments());
                            Expression countExpr = count.getArgument(arguments.size() - 1);
                            for (int i = arguments.size() - 2; i >= 0; --i) {
                                Expression argument = count.getArgument(i);
                                If ifNull = new If(new IsNull(argument), NullLiteral.INSTANCE, countExpr);
                                countExpr = assignNullType(ifNull, cascadesContext);
                            }
                            Count countIf = new Count(countExpr);
                            countIfList.add(countIf);
                            return countIf;
                        }
                    }
                    return outputChild;
                });
        return Pair.of(aggregate.withAggOutput(newOutput), countIfList.build());
    }

    private boolean containsCountDistinctMultiExpr(LogicalAggregate<? extends Plan> aggregate) {
        return ExpressionUtils.deapAnyMatch(aggregate.getOutputExpressions(), expr ->
                expr instanceof Count && ((Count) expr).isDistinct() && expr.arity() > 1);
    }

    // don't invoke the ExpressionNormalization, because the expression maybe simplified and get rid of some slots
    private If assignNullType(If ifExpr, CascadesContext cascadesContext) {
        If ifWithCoercion = (If) TypeCoercionUtils.processBoundFunction(ifExpr);
        Expression trueValue = ifWithCoercion.getArgument(1);
        if (trueValue instanceof Cast && trueValue.child(0) instanceof NullLiteral) {
            List<Expression> newArgs = Lists.newArrayList(ifWithCoercion.getArguments());
            // backend don't support null type, so we should set the type
            newArgs.set(1, new NullLiteral(((Cast) trueValue).getDataType()));
            return ifWithCoercion.withChildren(newArgs);
        }
        return ifWithCoercion;
    }

    private boolean enablePushDownNoGroupAgg() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext == null || connectContext.getSessionVariable().enablePushDownNoGroupAgg();
    }

    private List<PhysicalHashAggregate<? extends Plan>> fourPhaseAggregateWithDistinct(
            LogicalAggregate<? extends Plan> logicalAgg, ConnectContext connectContext,
            Function<List<Expression>, RequireProperties> secondPhaseRequireSupplier,
            Function<LogicalAggregate<? extends Plan>, RequireProperties> fourPhaseRequireSupplier) {
        boolean couldBanned = couldConvertToMulti(logicalAgg);

        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        Set<NamedExpression> distinctArguments = aggregateFunctions.stream()
                .filter(AggregateFunction::isDistinct)
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());

        Set<NamedExpression> localAggGroupBySet = ImmutableSet.<NamedExpression>builder()
                .addAll((List<NamedExpression>) (List) logicalAgg.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();

        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER, couldBanned);

        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase1 = aggregateFunctions.stream()
                .filter(aggregateFunction -> !aggregateFunction.isDistinct())
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                    return new Alias(localAggExpr);
                }, (oldValue, newValue) -> newValue));

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase1.values())
                .build();

        List<Expression> localAggGroupBy = ImmutableList.copyOf(localAggGroupBySet);
        boolean maybeUsingStreamAgg = maybeUsingStreamAgg(connectContext, localAggGroupBy);
        List<Expression> partitionExpressions = getHashAggregatePartitionExpressions(logicalAgg);
        RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);

        boolean isGroupByEmptySelectEmpty = localAggGroupBy.isEmpty() && localAggOutput.isEmpty();

        // be not recommend generate an aggregate node with empty group by and empty output,
        // so add a null int slot to group by slot and output
        if (isGroupByEmptySelectEmpty) {
            localAggGroupBy = ImmutableList.of(new NullLiteral(TinyIntType.INSTANCE));
            localAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }

        PhysicalHashAggregate<Plan> anyLocalAgg = new PhysicalHashAggregate<>(localAggGroupBy,
                localAggOutput, Optional.of(partitionExpressions), inputToBufferParam,
                maybeUsingStreamAgg, Optional.empty(), logicalAgg.getLogicalProperties(),
                requireAny, logicalAgg.child());

        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER, couldBanned);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase2 =
                nonDistinctAggFunctionToAliasPhase1.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(Entry::getKey, kv -> {
                            AggregateFunction originFunction = kv.getKey();
                            Alias localOutput = kv.getValue();
                            AggregateExpression globalAggExpr = new AggregateExpression(
                                    originFunction, bufferToBufferParam, localOutput.toSlot());
                            return new Alias(globalAggExpr);
                        }));

        List<NamedExpression> globalAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(nonDistinctAggFunctionToAliasPhase2.values())
                .build();

        // be not recommend generate an aggregate node with empty group by and empty output,
        // so add a null int slot to group by slot and output
        if (isGroupByEmptySelectEmpty) {
            globalAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }

        RequireProperties secondPhaseRequire = secondPhaseRequireSupplier.apply(localAggGroupBy);

        //phase 2
        PhysicalHashAggregate<? extends Plan> anyLocalHashGlobalAgg = new PhysicalHashAggregate<>(
                localAggGroupBy, globalAggOutput, Optional.of(ImmutableList.copyOf(logicalAgg.getDistinctArguments())),
                bufferToBufferParam, false, logicalAgg.getLogicalProperties(),
                secondPhaseRequire, anyLocalAgg);

        boolean shouldDistinctAfterPhase2 = distinctArguments.size() > 1;

        // phase 3
        AggregateParam distinctLocalParam = new AggregateParam(
                AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER, couldBanned);
        Map<AggregateFunction, Alias> nonDistinctAggFunctionToAliasPhase3 = new HashMap<>();
        List<NamedExpression> localDistinctOutput = Lists.newArrayList();
        for (int i = 0; i < logicalAgg.getOutputExpressions().size(); i++) {
            NamedExpression outputExpr = logicalAgg.getOutputExpressions().get(i);
            List<AggregateFunction> needUpdateSlot = Lists.newArrayList();
            NamedExpression outputExprPhase3 = (NamedExpression) outputExpr
                    .rewriteDownShortCircuit(expr -> {
                        if (expr instanceof AggregateFunction) {
                            AggregateFunction aggregateFunction = (AggregateFunction) expr;
                            if (aggregateFunction.isDistinct()) {
                                Set<Expression> aggChild = Sets.newLinkedHashSet(aggregateFunction.children());
                                Preconditions.checkArgument(aggChild.size() == 1
                                                || aggregateFunction.getDistinctArguments().size() == 1,
                                        "cannot process more than one child in aggregate distinct function: "
                                                + aggregateFunction);

                                AggregateFunction newDistinct;
                                if (shouldDistinctAfterPhase2) {
                                    // we use aggregate function to process distinct,
                                    // so need to change to multi distinct function
                                    newDistinct = tryConvertToMultiDistinct(
                                            aggregateFunction.withDistinctAndChildren(
                                                    true, ImmutableList.copyOf(aggChild))
                                    );
                                } else {
                                    // we use group by to process distinct,
                                    // so no distinct param in the aggregate function
                                    newDistinct = aggregateFunction.withDistinctAndChildren(
                                            false, ImmutableList.copyOf(aggChild));
                                }

                                AggregateExpression newDistinctAggExpr = new AggregateExpression(
                                        newDistinct, distinctLocalParam, newDistinct);
                                return newDistinctAggExpr;
                            } else {
                                needUpdateSlot.add(aggregateFunction);
                                Alias alias = nonDistinctAggFunctionToAliasPhase2.get(expr);
                                return new AggregateExpression(aggregateFunction,
                                        new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_BUFFER),
                                        alias.toSlot());
                            }
                        }
                        return expr;
                    });
            for (AggregateFunction originFunction : needUpdateSlot) {
                nonDistinctAggFunctionToAliasPhase3.put(originFunction, (Alias) outputExprPhase3);
            }
            localDistinctOutput.add(outputExprPhase3);

        }
        PhysicalHashAggregate<? extends Plan> distinctLocal = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), localDistinctOutput, Optional.empty(),
                distinctLocalParam, false, logicalAgg.getLogicalProperties(),
                secondPhaseRequire, anyLocalHashGlobalAgg);

        //phase 4
        AggregateParam distinctGlobalParam = new AggregateParam(
                AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT, couldBanned);
        List<NamedExpression> globalDistinctOutput = Lists.newArrayList();
        for (int i = 0; i < logicalAgg.getOutputExpressions().size(); i++) {
            NamedExpression outputExpr = logicalAgg.getOutputExpressions().get(i);
            NamedExpression outputExprPhase4 = (NamedExpression) outputExpr.rewriteDownShortCircuit(expr -> {
                if (expr instanceof AggregateFunction) {
                    AggregateFunction aggregateFunction = (AggregateFunction) expr;
                    if (aggregateFunction.isDistinct()) {
                        Set<Expression> aggChild = Sets.newLinkedHashSet(aggregateFunction.children());
                        Preconditions.checkArgument(aggChild.size() == 1
                                        || aggregateFunction.getDistinctArguments().size() == 1,
                                "cannot process more than one child in aggregate distinct function: "
                                        + aggregateFunction);
                        AggregateFunction newDistinct;
                        if (shouldDistinctAfterPhase2) {
                            newDistinct = tryConvertToMultiDistinct(
                                    aggregateFunction.withDistinctAndChildren(
                                            true, ImmutableList.copyOf(aggChild))
                            );
                        } else {
                            newDistinct = aggregateFunction
                                    .withDistinctAndChildren(false, ImmutableList.copyOf(aggChild));
                        }
                        int idx = logicalAgg.getOutputExpressions().indexOf(outputExpr);
                        Alias localDistinctAlias = (Alias) (localDistinctOutput.get(idx));
                        return new AggregateExpression(newDistinct,
                                distinctGlobalParam, localDistinctAlias.toSlot());
                    } else {
                        Alias alias = nonDistinctAggFunctionToAliasPhase3.get(expr);
                        return new AggregateExpression(aggregateFunction,
                                new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_RESULT),
                                alias.toSlot());
                    }
                }
                return expr;
            });
            globalDistinctOutput.add(outputExprPhase4);
        }

        RequireProperties fourPhaseRequire = fourPhaseRequireSupplier.apply(logicalAgg);
        PhysicalHashAggregate<? extends Plan> distinctGlobal = new PhysicalHashAggregate<>(
                logicalAgg.getGroupByExpressions(), globalDistinctOutput, Optional.empty(),
                distinctGlobalParam, false, logicalAgg.getLogicalProperties(),
                fourPhaseRequire, distinctLocal);

        return ImmutableList.<PhysicalHashAggregate<? extends Plan>>builder()
                .add(distinctGlobal)
                .build();
    }

    private boolean couldConvertToMulti(LogicalAggregate<? extends Plan> aggregate) {
        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        for (AggregateFunction func : aggregateFunctions) {
            if (!func.isDistinct()) {
                continue;
            }
            if (!(func instanceof Count || func instanceof Sum || func instanceof GroupConcat)) {
                return false;
            }
            if (func.arity() <= 1) {
                continue;
            }
            for (int i = 1; i < func.arity(); i++) {
                // think about group_concat(distinct col_1, ',')
                if (!(func.child(i) instanceof OrderExpression) && !func.child(i).getInputSlots().isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }
}
