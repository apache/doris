package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsBigInt;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.amazonaws.services.logs.model.TagLogGroupRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CompressedMaterializeGroupBy extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.NORMALIZE_AGGREGATE.build(
                logicalAggregate().then(
                        aggregate -> compressedMaterialize(aggregate)
                )
        );
    }

    private boolean canCompress(Expression expression) {
        DataType type = expression.getDataType();
        if (type instanceof CharacterType) {
            CharacterType ct = (CharacterType) type;
            if (ct.getLen() < 7) {
                return true;
            }
        }
        return false;
    }

    /*
    example:
    [support] select sum(v) from t group by substring(k, 1,2)
    [not support] select substring(k, 1,2), sum(v) from t group by substring(k, 1,2)
    [support] select k, sum(v) from t group by k
    [not support] select substring(k, 1,2), sum(v) from t group by k
    [support]  select A as B from T group by A
    */
    private Set<Expression> getEncodableGroupByExpressions(LogicalAggregate<Plan> aggregate) {
        Set<Expression> encodableGroupbyExpressions = Sets.newHashSet();
        Set<Slot> slotShouldNotEncode = Sets.newHashSet();
        for (NamedExpression ne : aggregate.getOutputExpressions()) {
            if (ne instanceof Alias) {
                Expression child = ((Alias) ne).child();
                //support: select A as B from T group by A
                if (!(child instanceof SlotReference)) {
                    slotShouldNotEncode.addAll(child.getInputSlots());
                }
            }
        }
        for (Expression gb : aggregate.getGroupByExpressions()) {
            if (canCompress(gb)) {
                boolean encodable = true;
                for (Slot gbs : gb.getInputSlots()) {
                    if (slotShouldNotEncode.contains(gbs)) {
                        encodable = false;
                        break;
                    }
                }
                if (encodable) {
                    encodableGroupbyExpressions.add(gb);
                }
            }
        }
        return encodableGroupbyExpressions;
    }

    private LogicalAggregate<Plan> compressedMaterialize(LogicalAggregate<Plan> aggregate) {
        List<Alias> encodedExpressions = Lists.newArrayList();
        Set<Expression> encodableGroupByExpressions = getEncodableGroupByExpressions(aggregate);
        if (!encodableGroupByExpressions.isEmpty()) {
            List<Expression> newGroupByExpressions = Lists.newArrayList();
            for (Expression gp : aggregate.getGroupByExpressions()) {
                if (encodableGroupByExpressions.contains(gp)) {
                    Alias alias = new Alias(new EncodeAsBigInt(gp));
                    newGroupByExpressions.add(alias);
                    encodedExpressions.add(alias);
                } else {
                    newGroupByExpressions.add(gp);
                }
            }
            List<NamedExpression> newOutput = Lists.newArrayList();
            for (NamedExpression ne : aggregate.getOutputExpressions()) {
                if (ne instanceof SlotReference && encodableGroupByExpressions.contains(ne)) {
                    newOutput.add(new Alias(ne.getExprId(), new AnyValue(ne), ne.getName()));
                } else if (ne instanceof Alias && encodableGroupByExpressions.contains(((Alias) ne).child())) {
                    Expression child = ((Alias) ne).child();
                    Preconditions.checkArgument(child instanceof SlotReference,
                            "encode %s failed, not a slot", child);
                    newOutput.add(new Alias(((SlotReference) child).getExprId(), new AnyValue(child),
                            "any_value(" + child + ")"));
                } else {
                    newOutput.add(ne);
                }
            }
            newOutput.addAll(encodedExpressions);
            aggregate = aggregate.withGroupByAndOutput(newGroupByExpressions, newOutput);
        }
        return aggregate;
    }
}
