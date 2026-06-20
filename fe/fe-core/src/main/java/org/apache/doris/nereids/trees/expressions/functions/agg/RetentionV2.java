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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'retention_v2'.
 * Optimized version of retention using a uint32_t bitmask state on the BE side.
 * The serialization format is incompatible with retention (v1), so this function
 * should only be used when all BE nodes have been upgraded.
 */
public class RetentionV2 extends NullableAggregateFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(BooleanType.INSTANCE)).varArgs(BooleanType.INSTANCE)
    );

    /**
     * constructor with 1 or more arguments.
     */
    public RetentionV2(Expression arg, Expression... varArgs) {
        this(false, arg, varArgs);
    }

    /**
     * constructor with 1 or more arguments.
     */
    public RetentionV2(boolean distinct, Expression arg, Expression... varArgs) {
        this(distinct, false, arg, varArgs);
    }

    public RetentionV2(boolean distinct, boolean alwaysNullable, Expression arg,
            Expression... varArgs) {
        super("retention_v2", distinct, alwaysNullable, ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /** constructor for withChildren and reuse signature */
    private RetentionV2(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        String functionName = getName();
        if (this.children.isEmpty()) {
            throw new AnalysisException(
                    "The " + functionName + " function must have at least one param");
        }

        // TODO: enforce the 32-condition limit (matching RetentionStateV2::MAX_EVENTS=32 on the BE
        //       side) so that passing 33+ conditions is rejected with a clear error instead of
        //       causing undefined behavior (bit-shift >= 32) in BE. The same issue exists in
        //       Retention (v1) which also has no upper-bound check.
        for (int i = 0; i < children.size(); i++) {
            if (!getArgumentType(i).isBooleanType()) {
                throw new AnalysisException(
                        "All params of " + functionName + " function must be boolean");
            }
        }
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public RetentionV2 withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new RetentionV2(getFunctionParams(distinct, children));
    }

    @Override
    public RetentionV2 withAlwaysNullable(boolean alwaysNullable) {
        return new RetentionV2(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRetentionV2(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
