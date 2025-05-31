// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Map.Entry;

/** SkipSimpleExprs */
public class SkipSimpleExprs {
    /** isSimpleExpr */
    public static boolean isSimpleExpr(Expression expression) {
        if (expression.containsType(UnboundSlot.class, IsNull.class)) {
            return false;
        }
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

    /** computeExprFeature */
    public static ExprFeature computeExprFeature(Expression expr) {
        Map<Integer, Integer> slotCount = Maps.newHashMap();
        Map<Integer, Integer> slotIsNullCount = Maps.newHashMap();
        computeExprFeature(expr, slotCount, slotIsNullCount);
        return new ExprFeature(slotCount, slotIsNullCount);
    }

    private static void computeExprFeature(
            Expression e, Map<Integer, Integer> slotCount, Map<Integer, Integer> slotIsNullCount) {
        if (e instanceof Slot) {
            if (!(e instanceof UnboundSlot)) {
                int slotId = ((Slot) e).getExprId().asInt();
                Integer count = slotCount.get(slotId);
                slotCount.put(slotId, count == null ? 1 : count + 1);
            }
        } else if (e instanceof IsNull && e.child(0) instanceof Slot) {
            if (!(e.child(0) instanceof UnboundSlot)) {
                int slotId = ((Slot) e.child(0)).getExprId().asInt();
                Integer count = slotIsNullCount.get(slotId);
                slotIsNullCount.put(slotId, count == null ? 1 : count + 1);
            }
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
