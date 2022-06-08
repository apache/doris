package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

public class PhysicalHashJoin
    extends PhysicalBinaryOperator
    <PhysicalHashJoin, PhysicalPlan, PhysicalPlan> {

    private JoinType joinType;

    private Expression predicate;

    public PhysicalHashJoin(OperatorType type, JoinType joinType, ComparisonPredicate predicate) {
        super(type);
        this.joinType = joinType;
        this.predicate = predicate;
    }

    public PhysicalHashJoin() {
        super(OperatorType.PHYSICAL_HASH_JOIN);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Expression getPredicate() {
        return predicate;
    }
}
