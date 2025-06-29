package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

public class StatsDerive extends PlanVisitor<Statistics, Void> implements CustomRewriter {


    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        plan.accept(this, null);
        return plan;
    }

    @Override
    public Statistics visit(Plan plan, Void context) {
        Statistics result = null;
        for (int i = plan.children().size() - 1; i >=0; i--) {
            result = plan.children().get(i).accept(this, null);
        }
        return result;
    }

    @Override
    public Statistics visitLogicalOlapScan(LogicalOlapScan scan, Void context) {
        Statistics stats = StatsCalculator.INSTANCE.computeOlapScan(scan);
        scan.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        Statistics childStats = project.child().accept(this, null);
        Statistics stats = StatsCalculator.INSTANCE.computeProject(project, childStats);
        project.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalSink(LogicalSink<? extends Plan> logicalSink, Void context) {
        Statistics childStats = logicalSink.child().accept(this, null);
        logicalSink.setStatistics(childStats);
        return childStats;
    }

    @Override
    public Statistics visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, Void context) {
        Statistics stats = StatsCalculator.INSTANCE.computeEmptyRelation(emptyRelation);
        emptyRelation.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalLimit(LogicalLimit<? extends Plan> limit, Void context) {
        Statistics childStats = limit.child().accept(this, null);
        Statistics stats = StatsCalculator.INSTANCE.computeLimit(limit, childStats);
        limit.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Void context) {
        Statistics stats = StatsCalculator.INSTANCE.computeOneRowRelation(oneRowRelation.getProjects());
        oneRowRelation.setStatistics(stats);
        return stats;
    }

    @Override
    public Statistics visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        Statistics childStats = aggregate.child().accept(this, null);
        Statistics stats = StatsCalculator.INSTANCE.computeAggregate(aggregate, childStats);
        aggregate.setStatistics(stats);
        return stats;
    }

    }
