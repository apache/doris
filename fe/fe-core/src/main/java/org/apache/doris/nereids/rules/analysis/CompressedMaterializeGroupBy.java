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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsBigInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsLargeInt;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * select A from T group by A
 * =>
 * select any_value(A) from T group by encode_as_int(A)
 */
public class CompressedMaterializeGroupBy extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.NORMALIZE_AGGREGATE.build(
                logicalAggregate().then(
                        aggregate -> compressedMaterialize(aggregate)
                )
        );
    }

    private Expression getEncodeExpression(Expression expression) {
        DataType type = expression.getDataType();
        Expression encodeExpr = null;
        if (type instanceof CharacterType) {
            CharacterType ct = (CharacterType) type;
            if (ct.getLen() < 4) {
                encodeExpr = new EncodeAsInt(expression);
            } else if (ct.getLen() < 7) {
                encodeExpr = new EncodeAsBigInt(expression);
            } else if (ct.getLen() < 15) {
                encodeExpr = new EncodeAsLargeInt(expression);
            }
        }
        return encodeExpr;
    }

    /*
    example:
    [support] select sum(v) from t group by substring(k, 1,2)
    [not support] select substring(k, 1,2), sum(v) from t group by substring(k, 1,2)
    [support] select k, sum(v) from t group by k
    [not support] select substring(k, 1,2), sum(v) from t group by k
    [support]  select A as B from T group by A
    */
    private Map<Expression, Expression> getEncodableGroupByExpressions(LogicalAggregate<Plan> aggregate) {
        Map<Expression, Expression> encodableGroupbyExpressions = Maps.newHashMap();
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
            Expression encodeExpr = getEncodeExpression(gb);
            if (encodeExpr != null) {
                boolean encodable = true;
                for (Slot gbs : gb.getInputSlots()) {
                    if (slotShouldNotEncode.contains(gbs)) {
                        encodable = false;
                        break;
                    }
                }
                if (encodable) {
                    encodableGroupbyExpressions.put(gb, encodeExpr);
                }
            }
        }
        return encodableGroupbyExpressions;
    }

    private LogicalAggregate<Plan> compressedMaterialize(LogicalAggregate<Plan> aggregate) {
        List<Alias> encodedExpressions = Lists.newArrayList();
        Map<Expression, Expression> encodableGroupByExpressions = getEncodableGroupByExpressions(aggregate);
        if (!encodableGroupByExpressions.isEmpty()) {
            List<Expression> newGroupByExpressions = Lists.newArrayList();
            for (Expression gp : aggregate.getGroupByExpressions()) {
                if (encodableGroupByExpressions.containsKey(gp)) {
                    Alias alias = new Alias(encodableGroupByExpressions.get(gp));
                    newGroupByExpressions.add(alias);
                    encodedExpressions.add(alias);
                } else {
                    newGroupByExpressions.add(gp);
                }
            }
            List<NamedExpression> newOutput = Lists.newArrayList();
            for (NamedExpression ne : aggregate.getOutputExpressions()) {
                // output A => output Any_value(A)
                if (ne instanceof SlotReference && encodableGroupByExpressions.containsKey(ne)) {
                    newOutput.add(new Alias(ne.getExprId(), new AnyValue(ne), ne.getName()));
                } else if (ne instanceof Alias && encodableGroupByExpressions.containsKey(((Alias) ne).child())) {
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
