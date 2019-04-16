package org.apache.doris.optimizer.rule.implementation;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptPhysicalProperty;
import org.apache.doris.optimizer.operator.*;
import org.apache.doris.optimizer.rule.OptRuleType;

import java.util.List;

public class UnionRule extends ImplemetationRule {
    public static UnionRule INSTANCE = new UnionRule();

    public UnionRule() {
        super(OptRuleType.RULE_IMP_UNION,
                OptExpression.create(
                        new OptPhysicalUnionAll(),
                        OptExpression.create(new OptPatternLeaf()),
                        OptExpression.create(new OptPatternLeaf())
                ));
    }

    @Override
    public void transform(OptExpression expr, List<OptExpression> newExprs) {
        Preconditions.checkArgument(expr.getInputs().size() == 2,
                "Union can only have tow children.");
        final OptLogicalUnion operator = (OptLogicalUnion) expr.getOp();
        if (operator.isUnionAll()) {
            final OptPhysicalUnionAll union = new OptPhysicalUnionAll();
            final OptExpression unionExpr = OptExpression.create(union, expr.getInputs());
            newExprs.add(unionExpr);
        } else {
            final OptPhysicalProperty outerProperty = (OptPhysicalProperty) expr.getProperty();
            final OptPhysicalProperty innerProperty = (OptPhysicalProperty) expr.getProperty();
            final OptPhysicalHashAggregate aggregate = new OptPhysicalHashAggregate(
                    operator.getGroupBy(), OptOperator.HashAggStage.Agg);
            final OptExpression aggregateExpr = OptExpression.create(aggregate, expr.getInputs());
            if (outerProperty.getDistributionSpec().isSingleSatisfySingle(innerProperty.getDistributionSpec())) {
                final OptPhysicalUnionAll union = new OptPhysicalUnionAll();
                final OptExpression unionExpr = OptExpression.create(union, aggregateExpr);
                newExprs.add(unionExpr);
            } else {
                final OptPhysicalHashAggregate mergeAggregate = new OptPhysicalHashAggregate(
                        operator.getGroupBy(), OptOperator.HashAggStage.Merge);
                final OptExpression mergeAggregateExpr = OptExpression.create(mergeAggregate, aggregateExpr);
                final OptPhysicalUnionAll union = new OptPhysicalUnionAll();
                final OptExpression unionExpr = OptExpression.create(union, mergeAggregateExpr);
                newExprs.add(unionExpr);
            }
        }
    }
}
