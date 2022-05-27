package org.apache.doris.nereids;

import org.apache.doris.nereids.operators.plans.physical.PhysicalOperator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

@SuppressWarnings("rawtypes")
public abstract class PlanOperatorVisitor<R, C> {
    public abstract R visit(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan, C context);

    public R visitPhysicalAggregationPlan(
                                          PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                          C context) {
        return null;
    }

    public R visitPhysicalOlapScanPlan(
                                       PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                       C context) {return null;}

    public R visitPhysicalSortPlan(
                                   PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                   C context) {return null;}

    public R visitPhysicalHashJoinPlan(
                                       PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                       C context) {return null;}

    public R visitPhysicalProjectPlan(
                                      PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                      C context) {return null;}

    public R visitPhysicalFilter(
                                 PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                 C context) { return null;}

    public R visitPhysicalExchange(
                                 PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan,
                                 C context) { return null;}

}
