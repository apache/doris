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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'rand_spark'.
 * Spark-compatible version of random/rand.
 * Key differences from standard random:
 * - Supports rand(null) - treats NULL seed as 0
 * - Only supports [0, 1) range (no custom range support)
 * - Accepts constant expressions (e.g., 10 + 5), not just literals
 * - Rejects column references (validated at BE execution time)
 * - Variadic with 0 or 1 argument
 * - Always returns DoubleType, never nullable
 */
public class RandSpark extends UniqueFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE)
    );

    /**
     * constructor with 0 argument.
     */
    public RandSpark() {
        this(StatementScopeIdGenerator.newExprId(), false);
    }

    /**
     * constructor with 1 argument.
     */
    public RandSpark(Expression arg) {
        this(StatementScopeIdGenerator.newExprId(), false, arg);
    }

    public RandSpark(ExprId uniqueId, boolean ignoreUniqueId) {
        super("rand_spark", uniqueId, ignoreUniqueId);
    }

    public RandSpark(ExprId uniqueId, boolean ignoreUniqueId, Expression arg) {
        super("rand_spark", uniqueId, ignoreUniqueId, arg);
        // Unlike Random, we accept constant expressions and NULL values
        // Column references are rejected at BE execution time via is_col_constant() check
    }

    private RandSpark(ExprId uniqueId, boolean ignoreUniqueId, List<Expression> children) {
        super("rand_spark", uniqueId, ignoreUniqueId, children);
    }

    /** constructor for withChildren and reuse signature */
    private RandSpark(UniqueFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * custom compute nullable.
     */
    @Override
    public boolean nullable() {
        // rand_spark always returns a value, even with null seed
        return false;
    }

    /**
     * withChildren.
     */
    @Override
    public RandSpark withChildren(List<Expression> children) {
        if (children.size() > 1) {
            throw new AnalysisException("rand_spark function only accepts 0 or 1 argument");
        }
        return new RandSpark(getFunctionParams(children));
    }

    @Override
    public RandSpark withIgnoreUniqueId(boolean ignoreUniqueId) {
        return new RandSpark(uniqueId, ignoreUniqueId, children);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRandSpark(this, context);
    }
}
