package org.apache.doris.optimizer.rule.implementation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.*;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class DuplicateAggToHashAggRule extends ImplemetationRule {

    public static DuplicateAggToHashAggRule INSTANCE = new DuplicateAggToHashAggRule();

    private DuplicateAggToHashAggRule() {
        super(OptRuleType.RULE_IMP_AGG_TO_HASH_AGG,
                OptExpression.create(
                        new OptLogicalAggregate(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternMultiTree())
                ));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        final OptLogicalAggregate operator = (OptLogicalAggregate) expr.getOp();
        if (!operator.isDuplicate()) {
            return;
        }

        final OptExpression child = expr.getInput(0);
        Preconditions.checkNotNull(child, "Aggregate must have child.");

        final OptPhysicalHashAggregate intermediate = new OptPhysicalHashAggregate(operator.getGroupBy(),
                OptPhysical.HashAggStage.Intermediate);
        final OptExpression aggregateExpr = OptExpression.create(
                intermediate,
                expr.getInputs().get(0));

        final List<OptExpression> mergeChildren = Lists.newArrayList();
        mergeChildren.add(aggregateExpr);
        for (int i = 1; i < expr.getInputs().size(); i++) {
            mergeChildren.add(expr.getInput(i));
        }

        final OptPhysicalHashAggregate merge = new OptPhysicalHashAggregate(operator.getGroupBy(),
                OptPhysical.HashAggStage.Merge);
        final OptExpression mergeExpr = OptExpression.create(
                merge,
                mergeChildren);

        newExprs.add(mergeExpr);
    }
}
