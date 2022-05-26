package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.operators.plans.physical.PhysicalOperator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanFragment;

@SuppressWarnings("rawtypes")
public class PhysicalPlanTranslator {

    public PlanFragment translatePlan(PhysicalPlan<? extends PhysicalPlan, ? extends PhysicalOperator> physicalPlan) {

    }


}
