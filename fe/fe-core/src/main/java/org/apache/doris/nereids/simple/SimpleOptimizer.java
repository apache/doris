package org.apache.doris.nereids.simple;

import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Optional;

public class SimpleOptimizer extends DefaultPlanRewriter<Void> {

    @Override
    public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, Void context) {
        return new PhysicalResultSink<>(
                sink.getOutputExprs(),
                Optional.empty(),
                sink.getLogicalProperties(),
                sink.child().accept(this, context));
    }

    @Override
    public Plan visitLogicalLimit(LogicalLimit<? extends Plan> limit, Void context) {
        Plan child = limit.child().accept(this, context);
        if (limit.getPhase() == LimitPhase.GLOBAL) {
            child = new PhysicalDistribute<>(DistributionSpecGather.INSTANCE, child);
        }

        return new PhysicalLimit<>(
                limit.getLimit(),
                limit.getOffset(),
                limit.getPhase(),
                limit.getLogicalProperties(),
                child
        );
    }

    @Override
    public Plan visitLogicalTopN(LogicalTopN<? extends Plan> logicalTopN, Void context) {
        Plan child = logicalTopN.child().accept(this, context);

        PhysicalTopN<Plan> localSort = new PhysicalTopN<>(logicalTopN.getOrderKeys(),
                logicalTopN.getLimit() + logicalTopN.getOffset(), 0, SortPhase.LOCAL_SORT,
                logicalTopN.getLogicalProperties(), child);

        return new PhysicalTopN<>(logicalTopN.getOrderKeys(), logicalTopN.getLimit(),
                logicalTopN.getOffset(), SortPhase.MERGE_SORT, logicalTopN.getLogicalProperties(), localSort
        );
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return new PhysicalProject<>(
                project.getProjects(),
                project.getLogicalProperties(),
                project.child().accept(this, context)
        );
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        return new PhysicalFilter<>(
                filter.getConjuncts(),
                filter.getLogicalProperties(),
                filter.child().accept(this, context)
        );
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        return new PhysicalOlapScan(
                olapScan.getRelationId(),
                olapScan.getTable(),
                olapScan.getQualifier(),
                olapScan.getSelectedIndexId(),
                olapScan.getSelectedTabletIds(),
                olapScan.getSelectedPartitionIds(),
                LogicalOlapScanToPhysicalOlapScan.convertDistribution(olapScan),
                olapScan.getPreAggStatus(),
                olapScan.getOutputByIndex(olapScan.getTable().getBaseIndexId()),
                Optional.empty(),
                olapScan.getLogicalProperties(),
                olapScan.getTableSample());
    }

    @Override
    public Plan visitLogicalEmptyRelation(LogicalEmptyRelation relation, Void context) {
        return new PhysicalEmptyRelation(relation.getRelationId(),
                relation.getProjects(), relation.getLogicalProperties());
    }
}
