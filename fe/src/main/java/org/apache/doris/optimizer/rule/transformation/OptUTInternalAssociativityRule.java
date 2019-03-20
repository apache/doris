package org.apache.doris.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptLogicalUTInternalNode;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class OptUTInternalAssociativityRule extends ExplorationRule {

    public static OptUTInternalAssociativityRule INSTANCE = new OptUTInternalAssociativityRule();

    private OptUTInternalAssociativityRule() {
        super(OptRuleType.RULE_EXP_UT_ASSOCIATIVITY,
                OptExpression.create(new OptLogicalUTInternalNode(),
                        OptExpression.create(new OptLogicalUTInternalNode(),
                                OptExpression.create(new OptPatternLeaf()),
                                OptExpression.create(new OptPatternLeaf())),
                        OptExpression.create(new OptPatternLeaf())));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptExpression leftChildJoin = expr.getInput(0);
        final OptExpression rightChild = expr.getInput(1);
        Preconditions.checkNotNull(leftChildJoin);
        Preconditions.checkNotNull(rightChild);
        final OptExpression leftChildJoinLeftChild = leftChildJoin.getInput(0);
        final OptExpression leftChildJoinRightChild = leftChildJoin.getInput(1);
        Preconditions.checkNotNull(leftChildJoinLeftChild);
        Preconditions.checkNotNull(leftChildJoinRightChild);

        //TODO predicates.....
        final OptExpression newLeftChildJoin = OptExpression.create(
                new OptLogicalUTInternalNode(),
                leftChildJoinLeftChild,
                rightChild);
        final OptExpression newTopJoin = OptExpression.create(
                new OptLogicalUTInternalNode(),
                newLeftChildJoin,
                leftChildJoinRightChild);
        newExprs.add(newTopJoin);
    }
}
