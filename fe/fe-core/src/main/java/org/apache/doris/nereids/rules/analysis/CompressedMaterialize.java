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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsBigInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsLargeInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsSmallInt;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * convert string to int in order to improve performance for aggregation and sorting.
 *
 * 1. AGG
 * select A from T group by A
 * =>
 * select DecodeAsVarchar(encode_as_int(A)) from T group by encode_as_int(A)
 *
 * 2. Sort
 * select * from T order by A
 * =>
 * select * from T order by encode_as_int(A)
 */
public class CompressedMaterialize implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.COMPRESSED_MATERIALIZE_AGG.build(
                        logicalAggregate().when(a -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().enableCompressMaterialize)
                        .then(this::compressedMaterializeAggregate)),
                RuleType.COMPRESSED_MATERIALIZE_SORT.build(
                        logicalSort().when(a -> ConnectContext.get() != null
                                && ConnectContext.get().getSessionVariable().enableCompressMaterialize)
                                .then(this::compressMaterializeSort)
                )
        );
    }

    private LogicalSort<Plan> compressMaterializeSort(LogicalSort<Plan> sort) {
        List<OrderKey> newOrderKeys = Lists.newArrayList();
        boolean changed = false;
        for (OrderKey orderKey : sort.getOrderKeys()) {
            Expression expr = orderKey.getExpr();
            Optional<Expression> encode = getEncodeExpression(expr);
            if (encode.isPresent()) {
                newOrderKeys.add(new OrderKey(encode.get(),
                        orderKey.isAsc(),
                        orderKey.isNullFirst()));
                changed = true;
            } else {
                newOrderKeys.add(orderKey);
            }
        }
        return changed ? sort.withOrderKeys(newOrderKeys) : sort;
    }

    private Optional<Expression> getEncodeExpression(Expression expression) {
        DataType type = expression.getDataType();
        Expression encodeExpr = null;
        if (type instanceof CharacterType) {
            CharacterType ct = (CharacterType) type;
            if (ct.getLen() > 0) {
                // skip column from variant, like 'L.var["L_SHIPMODE"] AS TEXT'
                if (ct.getLen() < 2) {
                    encodeExpr = new EncodeAsSmallInt(expression);
                } else if (ct.getLen() < 4) {
                    encodeExpr = new EncodeAsInt(expression);
                } else if (ct.getLen() < 7) {
                    encodeExpr = new EncodeAsBigInt(expression);
                } else if (ct.getLen() < 15) {
                    encodeExpr = new EncodeAsLargeInt(expression);
                }
            }
        }
        return Optional.ofNullable(encodeExpr);
    }

    /*
    example:
    [support] select sum(v) from t group by substring(k, 1,2)
    [not support] select substring(k, 1,2), sum(v) from t group by substring(k, 1,2)
    [support] select k, sum(v) from t group by k
    [not support] select substring(k, 1,2), sum(v) from t group by k
    [support]  select A as B from T group by A
    */
    private Map<Expression, Expression> getEncodeGroupByExpressions(LogicalAggregate<Plan> aggregate) {
        Map<Expression, Expression> encodeGroupbyExpressions = Maps.newHashMap();
        for (Expression gb : aggregate.getGroupByExpressions()) {
            Optional<Expression> encodeExpr = getEncodeExpression(gb);
            encodeExpr.ifPresent(expression -> encodeGroupbyExpressions.put(gb, expression));
        }
        return encodeGroupbyExpressions;
    }

    private LogicalAggregate<Plan> compressedMaterializeAggregate(LogicalAggregate<Plan> aggregate) {
        Map<Expression, Expression> encodeGroupByExpressions = getEncodeGroupByExpressions(aggregate);
        if (!encodeGroupByExpressions.isEmpty()) {
            List<Expression> newGroupByExpressions = Lists.newArrayList();
            for (Expression gp : aggregate.getGroupByExpressions()) {
                newGroupByExpressions.add(encodeGroupByExpressions.getOrDefault(gp, gp));
            }
            List<NamedExpression> newOutputs = Lists.newArrayList();
            Map<Expression, Expression> decodeMap = new HashMap<>();
            for (Expression gp : encodeGroupByExpressions.keySet()) {
                decodeMap.put(gp, new DecodeAsVarchar(encodeGroupByExpressions.get(gp)));
            }
            for (NamedExpression out : aggregate.getOutputExpressions()) {
                Expression replaced = ExpressionUtils.replace(out, decodeMap);
                if (out != replaced) {
                    if (out instanceof SlotReference) {
                        newOutputs.add(new Alias(out.getExprId(), replaced, out.getName()));
                    } else if (out instanceof Alias) {
                        newOutputs.add(((Alias) out).withChildren(replaced.children()));
                    } else {
                        // should not reach here
                        Preconditions.checkArgument(false, "output abnormal: " + aggregate);
                    }
                } else {
                    newOutputs.add(out);
                }
            }
            aggregate = aggregate.withGroupByAndOutput(newGroupByExpressions, newOutputs);
        }
        return aggregate;
    }
}
