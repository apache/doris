package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

public class PhysicalHashJoin
    extends PhysicalBinaryOperator
    <PhysicalHashJoin, PhysicalPlan, PhysicalPlan> {

    private JoinType joinType;

    // TODO: temp define, use Predicate when it's ready
    private ComparisonPredicate predicate;

    public PhysicalHashJoin() {
        super(OperatorType.PHYSICAL_HASH_JOIN);
    }


}
