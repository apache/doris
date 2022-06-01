package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.List;

public class PhysicalHashJoin
    extends PhysicalBinaryOperator
    <PhysicalHashJoin, PhysicalPlan, PhysicalPlan> {

    private JoinType joinType;

    // TODO: temp define, use Predicate when it's ready
    private ComparisonPredicate predicate;

    private List<Expression>  eqConjuncts;

    private List<Expression> otherConjuncts;

    public PhysicalHashJoin(OperatorType type, JoinType joinType, ComparisonPredicate predicate) {
        super(type);
        this.joinType = joinType;
        this.predicate = predicate;
        this.eqConjuncts =
    }

    public PhysicalHashJoin() {
        super(OperatorType.PHYSICAL_HASH_JOIN);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public boolean isCrossJoin() {
        return joinType.equals(JoinType.CROSS_JOIN) || eqConjuncts.isEmpty();
    }

}
