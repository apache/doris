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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Follow legacy planner cast agg_state combinator's children if we need cast it to another agg_state type when insert
 */
public class ConvertAggStateCast implements ExpressionPatternRuleFactory {

    public static ConvertAggStateCast INSTANCE = new ConvertAggStateCast();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class).then(ConvertAggStateCast::convert)
                        .toRule(ExpressionRuleType.CONVERT_AGG_STATE_CAST)
        );
    }

    /** convert */
    public static Cast convert(Cast cast) {
        Expression child = cast.child();
        DataType originalType = child.getDataType();
        DataType targetType = cast.getDataType();
        if (originalType instanceof AggStateType && targetType instanceof AggStateType) {
            // TODO remove it after we refactor mv rewriter to avoid generate Alias in expression
            while (child instanceof Alias) {
                child = ((Alias) child).child();
            }
            if (!originalType.equals(targetType)
                    && ((AggStateType) originalType).hasSameAggregateSignature((AggStateType) targetType)
                    && (child instanceof If || child instanceof CaseWhen || child instanceof Coalesce)) {
                // Move a codec-only cast to value branches. For example, keep the
                // condition and outer NULL in IF(cond, COMBINE(v), NULL), but let its
                // producer use the sink codec. A stored-state branch keeps its cast
                // and must still pass the normal compatibility check.
                ImmutableList.Builder<Expression> children = ImmutableList.builder();
                for (int i = 0; i < child.arity(); i++) {
                    Expression branch = child.child(i);
                    if ((child instanceof If && i == 0)
                            || (child instanceof CaseWhen && i == 0
                                    && ((CaseWhen) child).getValue().isPresent())) {
                        children.add(branch);
                    } else if (branch instanceof WhenClause) {
                        WhenClause when = (WhenClause) branch;
                        children.add(new WhenClause(when.getOperand(), castBranch(when.getResult(), targetType)));
                    } else {
                        children.add(castBranch(branch, targetType));
                    }
                }
                Expression conditional = child;
                // IF and COALESCE can cache their version-20 return signature. Recompute
                // it from the version-19 branches, without changing their NULL properties.
                Expression rebuilt = MoreFieldsThread.keepFunctionSignature(false,
                        () -> conditional.withChildren(children.build()));
                return cast.withChildren(ImmutableList.of(rebuilt));
            }
            if (child instanceof CombineCombinator) {
                AggStateType source = (AggStateType) originalType;
                AggStateType target = (AggStateType) targetType;
                // COMBINE casts may change only the codec. Keep rejecting changes such
                // as AVG(INT) to AVG(BIGINT); they require different aggregate inputs.
                if (!source.equals(target) && source.hasSameAggregateSignature(target)) {
                    return cast.withChildren(ImmutableList.of(((CombineCombinator) child)
                            .withBeExecVersion(target.getBeExecVersion())));
                }
                return cast;
            }
            if (child instanceof StateCombinator) {
                AggStateType target = (AggStateType) targetType;
                ImmutableList.Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(child.arity());
                for (int i = 0; i < child.arity(); i++) {
                    Expression newChild = TypeCoercionUtils.castIfNotSameTypeStrict(
                            child.child(i), target.getSubTypes().get(i));
                    if (newChild.nullable() != target.getSubTypeNullables().get(i)) {
                        if (newChild.nullable()) {
                            newChild = new NonNullable(newChild);
                        } else {
                            newChild = new Nullable(newChild);
                        }
                    }
                    newChildren.add(newChild);
                }
                // Rebuild the producer with the target codec. Nullable AGG_STATE uses
                // String columns. A generic CAST does not convert their state bytes.
                StateCombinator state = (StateCombinator) child;
                child = state.withChildrenAndBeExecVersion(newChildren.build(), target.getBeExecVersion());
                return cast.withChildren(ImmutableList.of(child));
            }
        }
        return cast;
    }

    private static Expression castBranch(Expression branch, DataType targetType) {
        // SQL NULL is not an empty aggregate state. Keep it NULL with the target type.
        return branch instanceof NullLiteral ? new NullLiteral(targetType) : convert(new Cast(branch, targetType));
    }
}
