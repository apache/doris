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

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite DISTINCT intersect/except to spillable aggregate and join when spill is enabled.
 */
public class RewriteSetOperationToJoinWhenSpill implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalIntersect()
                        .thenApply(ctx -> rewrite(ctx.root, JoinType.LEFT_SEMI_JOIN, ctx.connectContext))
                        .toRule(RuleType.REWRITE_INTERSECT_TO_JOIN_WHEN_SPILL),
                logicalExcept()
                        .thenApply(ctx -> rewrite(ctx.root, JoinType.LEFT_ANTI_JOIN, ctx.connectContext))
                        .toRule(RuleType.REWRITE_EXCEPT_TO_JOIN_WHEN_SPILL)
        );
    }

    private static Plan rewrite(LogicalSetOperation setOperation, JoinType joinType, ConnectContext connectContext) {
        if (!shouldRewrite(setOperation, connectContext)) {
            return null;
        }

        Plan result = distinct(setOperation.child(0), setOperation.getRegularChildOutput(0));
        for (int i = 1; i < setOperation.arity(); i++) {
            Plan right = distinct(setOperation.child(i), setOperation.getRegularChildOutput(i));
            result = new LogicalJoin<>(joinType, buildNullSafeEqual(result.getOutput(), right.getOutput()),
                    ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.SHUFFLE_RIGHT),
                    result, right, null);
        }

        return new LogicalProject<>(buildOutputs(setOperation.getOutputs(), result.getOutput()), result);
    }

    private static boolean shouldRewrite(LogicalSetOperation setOperation, ConnectContext connectContext) {
        if (connectContext == null || !(connectContext.getSessionVariable().enableSpill
                || connectContext.getSessionVariable().enableForceSpill)) {
            return false;
        }
        if (setOperation.getQualifier() != Qualifier.DISTINCT || setOperation.arity() < 2) {
            return false;
        }
        if (setOperation.getRegularChildrenOutputs().size() != setOperation.arity()) {
            return false;
        }
        return setOperation.getOutputs().stream()
                .allMatch(output -> supportSetOperationToJoinRewrite(output.getDataType()));
    }

    private static boolean supportSetOperationToJoinRewrite(DataType dataType) {
        // Rewriting INTERSECT/EXCEPT uses aggregate grouping plus null-safe equal hash join keys.
        // Keep the original set operation unless the output type can be used by both operators.
        return dataType.isPrimitive()
                && !dataType.isJsonType()
                && !dataType.isObjectOrVariantType()
                && !dataType.isVarBinaryType();
    }

    private static LogicalAggregate<Plan> distinct(Plan child, List<SlotReference> childOutput) {
        return new LogicalAggregate<>(childOutput.stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList()), true, child);
    }

    private static List<Expression> buildNullSafeEqual(List<Slot> leftOutput, List<Slot> rightOutput) {
        Preconditions.checkArgument(leftOutput.size() == rightOutput.size(),
                "left output size should be equal to right output size");
        ImmutableList.Builder<Expression> hashConjuncts = ImmutableList.builderWithExpectedSize(leftOutput.size());
        for (int i = 0; i < leftOutput.size(); i++) {
            hashConjuncts.add(new NullSafeEqual(leftOutput.get(i), rightOutput.get(i)));
        }
        return hashConjuncts.build();
    }

    private static List<NamedExpression> buildOutputs(List<NamedExpression> setOutputs, List<Slot> joinOutputs) {
        Preconditions.checkArgument(setOutputs.size() == joinOutputs.size(),
                "set operation output size should be equal to rewritten join output size");
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(setOutputs.size());
        for (int i = 0; i < setOutputs.size(); i++) {
            NamedExpression setOutput = setOutputs.get(i);
            Slot joinOutput = joinOutputs.get(i);
            Preconditions.checkArgument(setOutput.getDataType().equals(joinOutput.getDataType()),
                    "set operation output type should be equal to rewritten join output type");
            Preconditions.checkArgument(setOutput.nullable() || !joinOutput.nullable(),
                    "rewritten join output should not be nullable when set operation output is not nullable");
            Expression child = !joinOutput.nullable() && setOutput.nullable() ? new Nullable(joinOutput) : joinOutput;
            projects.add(new Alias(setOutput.getExprId(), child, setOutput.getName()));
        }
        return projects.build();
    }
}
