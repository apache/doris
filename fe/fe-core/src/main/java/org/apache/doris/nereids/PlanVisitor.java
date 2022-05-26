package org.apache.doris.nereids;

import org.apache.doris.nereids.trees.plans.Plan;

public abstract class PlanVisitor<C, R> {
    abstract R visit(Plan plan, C context);

    public R visitPhysicalAggregationPlan(Plan plan, C context) {
        return null;
    }

    public R visitPhysicalOlapScanPlan(Plan plan, C context) {return null;}

}
