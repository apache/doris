package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Any;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsInt;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * select A, sum(B) from T group by A
 * =>
 * select any_value(A) from T group by encode_as_int(A)
  */

public class CompressedMaterialization extends PlanPostProcessor{
    @Override
    public PhysicalHashAggregate visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate,
            CascadesContext context) {
        List<Expression> newGroupByExpressions = Lists.newArrayList();
        List<Expression> encodedExpressions = Lists.newArrayList();
        Map<Expression, Alias> encodeMap = Maps.newHashMap();
        for (Expression gp : aggregate.getGroupByExpressions()) {
            if (gp instanceof SlotReference && canCompress(gp)) {
                Alias alias = new Alias(new EncodeAsInt(gp), ((SlotReference) gp).getName());
                newGroupByExpressions.add(alias);
                encodedExpressions.add(gp);
                encodeMap.put(gp, alias);
            } else {
                newGroupByExpressions.add(gp);
            }
        }
        if (!encodedExpressions.isEmpty()) {
            aggregate = aggregate.withGroupByExpressions(newGroupByExpressions);
            boolean hasNewOutput = false;
            List<NamedExpression> newOutput = Lists.newArrayList();
            List<NamedExpression> output = aggregate.getOutputExpressions();
            for (NamedExpression ne : output) {
                if (ne instanceof SlotReference && encodedExpressions.contains(ne)) {
                        newOutput.add(new Alias(ne.getExprId(), new AnyValue(ne), ne.getName()));
                        newOutput.add(encodeMap.get(ne));
                        hasNewOutput = true;
                } else {
                    newOutput.add(ne);
                }
            }
            if (hasNewOutput) {
                aggregate = aggregate.withAggOutput(newOutput);
            }
        }
        return aggregate;
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
}
