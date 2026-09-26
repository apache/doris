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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * window function: Ntile()
 */
public class Ntile extends WindowFunction implements LeafExpression, AlwaysNotNullable, ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE)
    );

    public Ntile(Expression buckets) {
        super("ntile", buckets);
    }

    /** constructor for withChildren and reuse signature */
    private Ntile(WindowFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Ntile withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Ntile(getFunctionParams(children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        Expression buckets = getArgument(0);
        DataType type = buckets.getDataType();
        if (!type.isIntegralType()) {
            throw new AnalysisException("The bucket of NTILE must be a integer: " + this.toSql());
        }
        if (type.isLargeIntType()) {
            // NTILE always returns BIGINT, and backend computes the bucket index with an int64 value,
            // so a LARGEINT bucket can not be handled.
            throw new AnalysisException("The bucket of NTILE must be an integer within the range of BIGINT, "
                    + "but got " + type.toSql() + ": " + this.toSql());
        }
        if (!buckets.isConstant()) {
            throw new AnalysisException(
                "The bucket of NTILE must be a constant value: " + this.toSql());
        }
        // The bucket may be a constant expression such as `1 + 1`, which is folded to a literal only by the
        // rewrite phase after this check runs. Reject early a bucket that FE already evaluates to a non-positive
        // or NULL value. A bucket FE can not evaluate, e.g. `3 % 2`, may still be folded by BE when
        // enable_fold_constant_by_be is set, so it is left to checkLegalityAfterRewrite.
        Expression evaluated = FoldConstantRuleOnFE.evaluateWithoutContext(buckets);
        if (evaluated instanceof Literal) {
            checkPositiveBucket(evaluated);
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        // The backend reads the bucket from the argument column and registers ntile only for a non-nullable
        // integer argument, so the bucket must have been folded to a literal before the plan is translated.
        // That does not happen when constant folding is skipped, e.g. with debug_skip_fold_constant=true, or
        // when the configured folding can not evaluate the bucket.
        checkPositiveBucket(getArgument(0));
    }

    private void checkPositiveBucket(Expression bucket) {
        if (!(bucket instanceof IntegerLikeLiteral) || ((IntegerLikeLiteral) bucket).getLongValue() <= 0) {
            throw new AnalysisException(
                "The bucket parameter of NTILE must be a constant positive integer: " + this.toSql());
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNtile(this, context);
    }
}
