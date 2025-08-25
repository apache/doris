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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.expression.QueryColumnCollector.CollectorContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to collect query column.
 */
public class QueryColumnCollector extends DefaultPlanRewriter<CollectorContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext connectContext = ConnectContext.get();
        if ((connectContext != null && connectContext.getState().isInternal())
                || !StatisticsUtil.enableAutoAnalyze()) {
            return plan;
        }
        CollectorContext context = new CollectorContext();
        plan.accept(this, context);
        if (StatisticsUtil.enableAutoAnalyze()) {
            context.midPriority.removeAll(context.highPriority);
            AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
            analysisManager.updateHighPriorityColumn(context.highPriority);
            analysisManager.updateMidPriorityColumn(context.midPriority);
        }
        return plan;
    }

    /**
     * Context.
     */
    public static class CollectorContext {
        public Map<Slot/*project output column*/, NamedExpression/*Actual project expr*/> projects
                = new LinkedHashMap<>();

        public Set<Slot> highPriority = new LinkedHashSet<>();

        public Set<Slot> midPriority = new LinkedHashSet<>();
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, CollectorContext context) {
        project.child().accept(this, context);
        List<NamedExpression> projects = project.getOutputs();
        List<Slot> slots = project.computeOutput();
        for (int i = 0; i < slots.size(); i++) {
            context.projects.put(slots.get(i), projects.get(i));
        }
        if (project.child() instanceof LogicalCatalogRelation
                || project.child() instanceof LogicalFilter
                && ((LogicalFilter<?>) project.child()).child() instanceof LogicalCatalogRelation) {

            RoaringBitmap bitSet = new RoaringBitmap();
            for (NamedExpression output : project.getOutputs()) {
                output.foreach(e -> {
                    if (e instanceof Slot) {
                        bitSet.add(((Slot) e).getExprId().asInt());
                    }
                });
            }
            LogicalCatalogRelation scan = project.child() instanceof LogicalCatalogRelation
                    ? (LogicalCatalogRelation) project.child()
                    : (LogicalCatalogRelation) project.child().child(0);
            List<Slot> outputOfScan = scan.getOutput();
            for (Slot slot : outputOfScan) {
                if (!bitSet.contains(slot.getExprId().asInt())) {
                    context.midPriority.remove(slot);
                }
            }
        }
        return project;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, CollectorContext context) {
        join.child(0).accept(this, context);
        join.child(1).accept(this, context);
        context.highPriority.addAll(
                (join.isMarkJoin() ? join.getLeftConditionSlot() : join.getConditionSlot())
                .stream().flatMap(s -> backtrace(s, context).stream())
                .collect(Collectors.toSet())
        );
        return join;
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, CollectorContext context) {
        aggregate.child(0).accept(this, context);
        context.highPriority.addAll(aggregate.getGroupByExpressions()
                .stream()
                .flatMap(e -> e.<SlotReference>collect(SlotReference.class::isInstance).stream())
                .flatMap(s -> backtrace(s, context).stream())
                .collect(Collectors.toSet()));
        return aggregate;
    }

    @Override
    public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, CollectorContext context) {
        having.child(0).accept(this, context);
        context.highPriority.addAll(
                having.getExpressions().stream()
                .flatMap(e -> e.<SlotReference>collect(SlotReference.class::isInstance).stream())
                .flatMap(s -> backtrace(s, context).stream())
                .collect(Collectors.toSet()));
        return having;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, CollectorContext context) {
        filter.child(0).accept(this, context);

        for (Slot inputSlot : filter.getInputSlots()) {
            context.highPriority.addAll(backtrace(inputSlot, context));
        }
        return filter;
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, CollectorContext context) {
        window.child(0).accept(this, context);
        context.highPriority.addAll(window
                .getWindowExpressions()
                .stream()
                .flatMap(e -> e.<SlotReference>collect(SlotReference.class::isInstance).stream())
                .flatMap(s -> backtrace(s, context).stream())
                .collect(Collectors.toSet()));
        return window;
    }

    private Set<Slot> backtrace(Slot slot, CollectorContext context) {
        return backtrace(slot, new LinkedHashSet<>(), context);
    }

    private Set<Slot> backtrace(Slot slot, Set<Slot> path, CollectorContext context) {
        if (path.contains(slot)) {
            return Collections.emptySet();
        }
        path.add(slot);
        if (slot instanceof SlotReference) {
            SlotReference slotReference = (SlotReference) slot;
            Optional<Column> col = slotReference.getOriginalColumn();
            Optional<TableIf> table = slotReference.getOriginalTable();
            if (col.isPresent() && table.isPresent()) {
                return Collections.singleton(slot);
            }
        }
        NamedExpression namedExpression = context.projects.get(slot);
        if (namedExpression == null) {
            return Collections.emptySet();
        }
        Set<SlotReference> slotReferences = namedExpression.collect(SlotReference.class::isInstance);
        Set<Slot> refCol = new LinkedHashSet<>();
        for (SlotReference slotReference : slotReferences) {
            refCol.addAll(backtrace(slotReference, path, context));
        }
        return refCol;
    }
}
