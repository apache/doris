package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Map.Entry;

public class SkipSimpleExprs {
    public static boolean isSimpleExpr(Expression expression) {
        ExprFeature exprFeature = computeExprFeature(expression);
        for (Entry<Integer, Integer> kv : exprFeature.slotCount.entrySet()) {
            Integer slotId = kv.getKey();
            Integer count = kv.getValue();
            if (count > 1) {
                Integer isNullCount = exprFeature.slotIsNullCount.get(slotId);
                if (isNullCount == null || isNullCount > 1) {
                    return false;
                }
            }
        }
        return true;
    }

    public static ExprFeature computeExprFeature(Expression expr) {
        Map<Integer, Integer> slotCount = Maps.newHashMap();
        Map<Integer, Integer> slotIsNullCount = Maps.newHashMap();
        computeExprFeature(expr, slotCount, slotIsNullCount);
        return new ExprFeature(slotCount, slotIsNullCount);
    }

    private static void computeExprFeature(
            Expression e, Map<Integer, Integer> slotCount, Map<Integer, Integer> slotIsNullCount) {
        if (e instanceof Slot) {
            int slotId = ((Slot) e).getExprId().asInt();
            Integer count = slotCount.get(slotId);
            slotCount.put(slotId, count == null ? 1 : count + 1);
        } else if (e instanceof IsNull && e.child(0) instanceof Slot) {
            int slotId = ((Slot) e.child(0)).getExprId().asInt();
            Integer count = slotIsNullCount.get(slotId);
            slotIsNullCount.put(slotId, count == null ? 1 : count + 1);
        } else {
            for (Expression child : e.children()) {
                computeExprFeature(child, slotCount, slotIsNullCount);
            }
        }
    }

    private static class ExprFeature {
        private Map<Integer, Integer> slotCount;
        private Map<Integer, Integer> slotIsNullCount;

        public ExprFeature(Map<Integer, Integer> slotCount, Map<Integer, Integer> slotIsNullCount) {
            this.slotCount = slotCount;
            this.slotIsNullCount = slotIsNullCount;
        }
    }
}
