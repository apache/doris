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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * rewrite simple top n query to defer materialize slot not use for sort or predicate
 */
public class DeferMaterializeTopNResult implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(
                                logicalTopN(
                                        logicalOlapScan()
                                                .when(s -> s.getTable().getEnableLightSchemaChange())
                                                .when(s -> s.getTable().isDupKeysOrMergeOnWrite())
                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> t.getOrderKeys().stream()
                                                .map(OrderKey::getExpr)
                                                .allMatch(Expression::isColumnFromTable))
                        ).then(r -> deferMaterialize(r, r.child(),
                                Optional.empty(), Optional.empty(), r.child().child()))
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                                        .when(s -> s.getTable().getEnableLightSchemaChange())
                                                        .when(s -> s.getTable().isDupKeysOrMergeOnWrite())
                                        )
                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> {
                                            for (OrderKey orderKey : t.getOrderKeys()) {
                                                if (!orderKey.getExpr().isColumnFromTable()) {
                                                    return false;
                                                }
                                                if (!(orderKey.getExpr() instanceof SlotReference)) {
                                                    return false;
                                                }
                                                SlotReference slotRef = (SlotReference) orderKey.getExpr();
                                                // do not support alias in project now
                                                if (!t.child().getProjects().contains(slotRef)) {
                                                    return false;
                                                }
                                            }
                                            return true;
                                        })
                        ).then(r -> {
                            LogicalProject<LogicalOlapScan> project = r.child().child();
                            return deferMaterialize(r, r.child(), Optional.of(project),
                                    Optional.empty(), project.child());
                        })
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(
                                logicalTopN(
                                        logicalFilter(
                                                logicalOlapScan()
                                                        .when(s -> s.getTable().getEnableLightSchemaChange())
                                                        .when(s -> s.getTable().isDupKeysOrMergeOnWrite())
                                        )
                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> t.getOrderKeys().stream()
                                                .map(OrderKey::getExpr)
                                                .allMatch(Expression::isColumnFromTable))
                        ).then(r -> {
                            LogicalFilter<LogicalOlapScan> filter = r.child().child();
                            return deferMaterialize(r, r.child(), Optional.empty(),
                                    Optional.of(filter), filter.child());
                        })
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(
                                logicalTopN(
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                                .when(s -> s.getTable().getEnableLightSchemaChange())
                                                                .when(s -> s.getTable().isDupKeysOrMergeOnWrite())
                                                )
                                        )
                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> {
                                            for (OrderKey orderKey : t.getOrderKeys()) {
                                                if (!orderKey.getExpr().isColumnFromTable()) {
                                                    return false;
                                                }
                                                if (!(orderKey.getExpr() instanceof SlotReference)) {
                                                    return false;
                                                }
                                                SlotReference slotRef = (SlotReference) orderKey.getExpr();
                                                // do not support alias in project now
                                                if (!t.child().getProjects().contains(slotRef)) {
                                                    return false;
                                                }
                                            }
                                            return true;
                                        })
                        ).then(r -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = r.child().child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            return deferMaterialize(r, r.child(), Optional.of(project),
                                    Optional.of(filter), filter.child());
                        })
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                                        .when(s -> s.getTable().getEnableLightSchemaChange())
                                                        .when(s -> s.getTable().isDupKeysOrMergeOnWrite())

                                        )
                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> {
                                            for (OrderKey orderKey : t.getOrderKeys()) {
                                                if (!orderKey.getExpr().isColumnFromTable()) {
                                                    return false;
                                                }
                                                if (!(orderKey.getExpr() instanceof SlotReference)) {
                                                    return false;
                                                }
                                                SlotReference slotRef = (SlotReference) orderKey.getExpr();
                                                // do not support alias in project now
                                                if (!t.child().getProjects().contains(slotRef)) {
                                                    return false;
                                                }
                                            }
                                            return true;
                                        })
                        ).when(project -> project.canMergeProjections(project.child().child()))).then(r -> {
                            LogicalProject<?> upperProject = r.child();
                            LogicalProject<LogicalOlapScan> bottomProject = r.child().child().child();
                            List<NamedExpression> projections = upperProject.mergeProjections(bottomProject);
                            LogicalProject<?> project = upperProject.withProjects(projections);
                            return deferMaterialize(r, r.child().child(), Optional.of(project),
                                    Optional.empty(), bottomProject.child());
                        })
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(logicalProject(
                                logicalTopN(
                                        logicalOlapScan()
                                                .when(s -> s.getTable().getEnableLightSchemaChange())
                                                .when(s -> s.getTable().isDupKeysOrMergeOnWrite())

                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> {
                                            for (OrderKey orderKey : t.getOrderKeys()) {
                                                if (!orderKey.getExpr().isColumnFromTable()) {
                                                    return false;
                                                }
                                                if (!(orderKey.getExpr() instanceof SlotReference)) {
                                                    return false;
                                                }
                                            }
                                            return true;
                                        })
                        )).then(r -> deferMaterialize(r, r.child().child(), Optional.of(r.child()),
                                Optional.empty(), r.child().child().child()))
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(logicalProject(
                                logicalTopN(
                                        logicalProject(logicalFilter(
                                                        logicalOlapScan()
                                                                .when(s -> s.getTable().getEnableLightSchemaChange())
                                                                .when(s -> s.getTable().isDupKeysOrMergeOnWrite())
                                                )
                                        )
                                ).when(t -> t.getLimit() < getTopNOptLimitThreshold())
                                        .whenNot(t -> t.getOrderKeys().isEmpty())
                                        .when(t -> {
                                            for (OrderKey orderKey : t.getOrderKeys()) {
                                                if (!orderKey.getExpr().isColumnFromTable()) {
                                                    return false;
                                                }
                                                if (!(orderKey.getExpr() instanceof SlotReference)) {
                                                    return false;
                                                }
                                                SlotReference slotRef = (SlotReference) orderKey.getExpr();
                                                // do not support alias in project now
                                                if (!t.child().getProjects().contains(slotRef)) {
                                                    return false;
                                                }
                                            }
                                            return true;
                                        })
                        ).when(project -> project.canMergeProjections(project.child().child()))).then(r -> {
                            LogicalProject<?> upperProject = r.child();
                            LogicalProject<LogicalFilter<LogicalOlapScan>> bottomProject = r.child().child().child();
                            List<NamedExpression> projections = upperProject.mergeProjections(bottomProject);
                            LogicalProject<?> project = upperProject.withProjects(projections);
                            LogicalFilter<LogicalOlapScan> filter = bottomProject.child();
                            return deferMaterialize(r, r.child().child(), Optional.of(project),
                                    Optional.of(filter), filter.child());
                        })
                )
        );
    }

    private Plan deferMaterialize(LogicalResultSink<? extends Plan> logicalResultSink,
            LogicalTopN<? extends Plan> logicalTopN, Optional<LogicalProject<? extends Plan>> logicalProject,
            Optional<LogicalFilter<? extends Plan>> logicalFilter, LogicalOlapScan logicalOlapScan) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().enableTopnLazyMaterialization) {
            return null;
        }
        Column rowId = new Column(Column.ROWID_COL, Type.STRING, false, null, false, "", "rowid column");
        SlotReference columnId = SlotReference.fromColumn(
                logicalOlapScan.getTable(), rowId, logicalOlapScan.getQualifier());
        Set<Slot> orderKeys = Sets.newHashSet();
        Set<ExprId> deferredMaterializedExprIds = Sets.newHashSet(logicalOlapScan.getOutputExprIdSet());
        logicalFilter.ifPresent(filter -> filter.getConjuncts()
                .forEach(e -> deferredMaterializedExprIds.removeAll(e.getInputSlotExprIds())));
        logicalTopN.getOrderKeys().stream()
                .map(OrderKey::getExpr)
                .map(Slot.class::cast)
                .peek(orderKeys::add)
                .map(NamedExpression::getExprId)
                .filter(Objects::nonNull)
                .forEach(deferredMaterializedExprIds::remove);
        if (logicalProject.isPresent()) {
            deferredMaterializedExprIds.retainAll(logicalProject.get().getInputSlots().stream()
                    .map(NamedExpression::getExprId).collect(Collectors.toSet()));
        }
        if (deferredMaterializedExprIds.isEmpty()) {
            // nothing to deferred materialize
            return null;
        }
        LogicalDeferMaterializeOlapScan deferOlapScan = new LogicalDeferMaterializeOlapScan(
                logicalOlapScan, deferredMaterializedExprIds, columnId);
        Plan root = logicalFilter.map(f -> f.withChildren(deferOlapScan)).orElse(deferOlapScan);
        Set<Slot> inputSlots = Sets.newHashSet();
        logicalFilter.ifPresent(filter -> inputSlots.addAll(filter.getInputSlots()));
        if (logicalProject.isPresent()) {
            ImmutableList.Builder<NamedExpression> requiredSlots = ImmutableList.builder();
            inputSlots.addAll(logicalProject.get().getInputSlots());
            for (Slot output : root.getOutput()) {
                if (inputSlots.contains(output) || orderKeys.contains(output)) {
                    requiredSlots.add(output);
                }
            }
            requiredSlots.add(columnId);
            root = new LogicalProject<>(requiredSlots.build(), root);
        }
        root = new LogicalDeferMaterializeTopN<>((LogicalTopN<? extends Plan>) logicalTopN.withChildren(root),
                deferredMaterializedExprIds, columnId);
        if (logicalProject.isPresent()) {
            // generate projections with the order exactly same as result output's
            Map<Slot, NamedExpression> projectsMap = Maps.newHashMap();
            logicalProject.get().getProjects().forEach(p -> projectsMap.put(p.toSlot(), p));
            List<NamedExpression> outputProjects = logicalResultSink.getOutput().stream()
                    .map(projectsMap::get)
                    .collect(ImmutableList.toImmutableList());
            root = logicalProject.get().withProjectsAndChild(outputProjects, root);
        }
        root = logicalResultSink.withChildren(root);
        return new LogicalDeferMaterializeResultSink<>((LogicalResultSink<? extends Plan>) root,
                logicalOlapScan.getTable(), logicalOlapScan.getSelectedIndexId());
    }

    private long getTopNOptLimitThreshold() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            if (!ConnectContext.get().getSessionVariable().enableTwoPhaseReadOpt) {
                return -1;
            }
            return ConnectContext.get().getSessionVariable().topnOptLimitThreshold;
        }
        return -1;
    }
}
