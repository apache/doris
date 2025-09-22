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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * stats derive for rbo
 */
public class StatsDerive extends PlanVisitor<Statistics, StatsDerive.DeriveContext> implements CustomRewriter {

    // when deepDerive is true, even nodes already have stats, we still derive them.
    private final boolean deepDerive;

    /**
    * context
    */
    public static class DeriveContext {
        StatsCalculator calculator = new StatsCalculator(null);
    }

    public StatsDerive(boolean deepDerive) {
        super();
        this.deepDerive = deepDerive;
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        plan.accept(this, new DeriveContext());
        return plan;
    }

    @Override
    public Statistics visit(Plan plan, DeriveContext context) {
        Statistics result = ((AbstractPlan) plan).getStats();
        if (result == null || deepDerive) {
            for (int i = plan.children().size() - 1; i >= 0; i--) {
                result = plan.children().get(i).accept(this, context);
            }
            ((AbstractPlan) plan).setStatistics(result);
        }
        return result;
    }

    @Override
    public Statistics visitLogicalProject(LogicalProject<? extends Plan> project, DeriveContext context) {
        Statistics stats = project.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = project.child().accept(this, context);
            stats = context.calculator.computeProject(project, childStats);
            project.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalSink(LogicalSink<? extends Plan> logicalSink, DeriveContext context) {
        Statistics childStats = logicalSink.child().accept(this, context);
        logicalSink.setStatistics(childStats);
        return childStats;
    }

    @Override
    public Statistics visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, DeriveContext context) {
        Statistics stats = context.calculator.computeEmptyRelation(emptyRelation);
        emptyRelation.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalLimit(LogicalLimit<? extends Plan> limit, DeriveContext context) {
        Statistics childStats = limit.child().accept(this, context);
        Statistics stats = context.calculator.computeLimit(limit, childStats);
        limit.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, DeriveContext context) {
        Statistics stats = context.calculator.computeOneRowRelation(oneRowRelation.getProjects());
        oneRowRelation.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, DeriveContext context) {
        Statistics stats = aggregate.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = aggregate.child().accept(this, context);
            stats = context.calculator.computeAggregate(aggregate, childStats);
            aggregate.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, DeriveContext context) {
        Statistics childStats = repeat.child().accept(this, context);
        Statistics stats = context.calculator.computeRepeat(repeat, childStats);
        repeat.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalFilter(LogicalFilter<? extends Plan> filter, DeriveContext context) {
        Statistics stats = filter.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = filter.child().accept(this, context);
            stats = context.calculator.computeFilter(filter, childStats);
            filter.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalOlapScan(LogicalOlapScan olapScan, DeriveContext context) {
        Statistics stats = olapScan.getStats();
        if (stats == null || deepDerive) {
            stats = context.calculator.computeOlapScan(olapScan);
            olapScan.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalDeferMaterializeOlapScan(LogicalDeferMaterializeOlapScan olapScan,
            DeriveContext context) {
        Statistics stats = context.calculator.computeOlapScan(olapScan);
        olapScan.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalCatalogRelation(LogicalCatalogRelation relation, DeriveContext context) {
        Statistics stats = relation.getStats();
        if (stats == null || deepDerive) {
            stats = context.calculator.computeCatalogRelation(relation);
            relation.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, DeriveContext context) {
        Statistics stats = tvfRelation.getFunction().computeStats(tvfRelation.getOutput());
        tvfRelation.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalSort(LogicalSort<? extends Plan> sort, DeriveContext context) {
        Statistics stats = sort.getStats();
        if (stats == null || deepDerive) {
            stats = sort.child().accept(this, context);
            sort.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalTopN(LogicalTopN<? extends Plan> topN, DeriveContext context) {
        Statistics stats = topN.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = topN.child().accept(this, context);
            stats = context.calculator.computeTopN(topN, childStats);
            topN.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN,
            DeriveContext context) {
        Statistics stats = topN.getStats();
        if (stats == null && deepDerive) {
            Statistics childStats = topN.child().accept(this, context);
            stats = context.calculator.computeTopN(topN, childStats);
            topN.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN,
            DeriveContext context) {
        Statistics stats = partitionTopN.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = partitionTopN.child().accept(this, context);
            stats = context.calculator.computePartitionTopN(partitionTopN, childStats);
            partitionTopN.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, DeriveContext context) {
        Statistics joinStats = join.getStats();
        if (joinStats == null || deepDerive) {
            Statistics leftStats = join.left().accept(this, context);
            Statistics rightStats = join.right().accept(this, context);
            joinStats = context.calculator.computeJoin(join, leftStats,
                    rightStats);
            joinStats = new StatisticsBuilder(joinStats).setWidthInJoinCluster(
                    leftStats.getWidthInJoinCluster() + rightStats.getWidthInJoinCluster()).build();
            join.setStatistics(joinStats);

        }
        return joinStats;
    }

    @Override
    public Statistics visitLogicalAssertNumRows(
            LogicalAssertNumRows<? extends Plan> assertNumRows, DeriveContext context) {
        Statistics childStats = assertNumRows.child().accept(this, context);
        Statistics stats = context.calculator.computeAssertNumRows(assertNumRows.getAssertNumRowsElement(),
                childStats);
        assertNumRows.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalUnion(
            LogicalUnion union, DeriveContext context) {
        Statistics stats = union.getStats();
        if (stats == null || deepDerive) {
            List<Statistics> childrenStats = new ArrayList<>();
            for (Plan child : union.children()) {
                Statistics childStats = child.accept(this, context);
                childrenStats.add(childStats);
            }
            stats = context.calculator.computeUnion(union, childrenStats);
            union.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalExcept(
            LogicalExcept except, DeriveContext context) {
        Statistics stats = except.getStats();
        if (stats == null || deepDerive) {
            for (Plan child : except.children()) {
                Statistics childStats = child.accept(this, context);
                if (stats == null) {
                    stats = childStats;
                }
            }
            stats = context.calculator.computeExcept(except, stats);
            except.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalIntersect(
            LogicalIntersect intersect, DeriveContext context) {
        Statistics stats = intersect.getStats();
        if (stats == null || deepDerive) {
            List<Statistics> childrenStats = new ArrayList<>();
            for (Plan child : intersect.children()) {
                Statistics childStats = child.accept(this, context);
                childrenStats.add(childStats);
            }

            stats = context.calculator.computeIntersect(intersect, childrenStats);
            intersect.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, DeriveContext context) {
        Statistics stats = generate.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = generate.child().accept(this, context);
            stats = context.calculator.computeGenerate(generate, childStats);
            generate.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalWindow(LogicalWindow<? extends Plan> window, DeriveContext context) {
        Statistics stats = window.getStats();
        if (stats == null || deepDerive) {
            Statistics childStats = window.child().accept(this, context);
            stats = context.calculator.computeWindow(window, childStats);
            window.setStatistics(stats);
        }
        return stats;
    }

    @Override
    public Statistics visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, DeriveContext context) {
        Statistics prodStats = cteProducer.child().accept(this, context);
        StatisticsBuilder builder = new StatisticsBuilder(prodStats);
        builder.setWidthInJoinCluster(1);
        Statistics stats = builder.build();
        cteProducer.setStatistics(stats);
        ConnectContext.get().getStatementContext().setProducerStats(cteProducer.getCteId(), stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, DeriveContext context) {
        CTEId cteId = cteConsumer.getCteId();
        Statistics prodStats = ConnectContext.get().getStatementContext().getProducerStatsByCteId(cteId);
        Preconditions.checkArgument(prodStats != null, String.format("Stats for CTE: %s not found", cteId));
        Statistics consumerStats = new Statistics(prodStats.getRowCount(), 1, new HashMap<>());
        for (Slot slot : cteConsumer.getOutput()) {
            Slot prodSlot = cteConsumer.getProducerSlot(slot);
            ColumnStatistic colStats = prodStats.columnStatistics().get(prodSlot);
            if (colStats == null) {
                continue;
            }
            consumerStats.addColumnStats(slot, colStats);
        }
        cteConsumer.setStatistics(consumerStats);
        return consumerStats;
    }

    @Override
    public Statistics visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            DeriveContext context) {
        Statistics childStats = null;
        for (Plan child : cteAnchor.children()) {
            childStats = child.accept(this, context);
        }
        Preconditions.checkArgument(childStats != null, "cteAnchor's childStats is null");
        // use consumer stats
        cteAnchor.setStatistics(childStats);
        return childStats;
    }

    /**
     * used for ut
     */
    @Override
    public Statistics visitLogicalRelation(LogicalRelation relation, DeriveContext context) {
        StatisticsBuilder builder = new StatisticsBuilder();
        builder.setRowCount(1);
        relation.getOutput().forEach(slot -> builder.putColumnStatistics(slot, ColumnStatistic.UNKNOWN));
        Statistics stats = builder.build();
        relation.setStatistics(stats);
        return stats;
    }
}





