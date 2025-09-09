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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'uniform'. This function generates uniform random numbers.
 * Signature: UNIFORM(min, max, gen)
 * - min, max: literal values defining the range [min, max]
 * - gen: expression used as seed for random generation
 * - If min/max are both integers, returns integer; otherwise returns double
 */
public class Uniform extends ScalarFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE,
                    BigIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, DoubleType.INSTANCE,
                    BigIntType.INSTANCE));

    /**
     * constructor with 3 arguments.
     */
    public Uniform(Expression min, Expression max, Expression gen) {
        super("uniform", min, max, gen);
    }

    /** constructor for withChildren and reuse signature */
    private Uniform(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!child(0).isLiteral()) {
            throw new AnalysisException("The first parameter (min) of uniform function must be literal");
        }
        if (!child(1).isLiteral()) {
            throw new AnalysisException("The second parameter (max) of uniform function must be literal");
        }
        // if do folding on BE, will before checkLegalityAfterRewrite, so we need it here too.
        checkLegalityAfterRewrite();
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (child(2).isLiteral()) {
            throw new AnalysisException("The third parameter (gen) of uniform function must not be literal");
        }
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        if (child(0).getDataType().isIntegralType() && child(1).getDataType().isIntegralType()) {
            // both integer, prefer integer return type
            return SIGNATURES.get(0);
        } else {
            // otherwise, prefer double return type
            return SIGNATURES.get(1);
        }
    }

    /**
     * custom compute nullable.
     */
    @Override
    public boolean nullable() {
        return children().stream().anyMatch(Expression::nullable);
    }

    /**
     * withChildren.
     */
    @Override
    public Uniform withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new Uniform(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUniform(this, context);
    }
}
