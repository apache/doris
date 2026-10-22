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
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'NgramSearch'.
 */
public class NgramSearch extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE,
                    IntegerType.INSTANCE));

    /**
     * constructor with 3 argument.
     */
    public NgramSearch(Expression arg0, Expression arg1, Expression arg2) {
        super("ngram_search", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private NgramSearch(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!getArgument(1).isConstant()) {
            throw new AnalysisException(
                    "ngram_search(text,pattern,gram_num): pattern support const value only.");
        }
        Expression gramNum = getArgument(2);
        if (!gramNum.isConstant() || !gramNum.getDataType().isIntegralType()) {
            throw new AnalysisException(
                    "ngram_search(text,pattern,gram_num): gram_num support const value only.");
        }
        // Constant folding has not run yet, so a constant expression such as `1 + 2` is not a
        // literal here. Reject the values FE can already determine now, before NULL propagation or
        // plan pruning can drop the whole call and skip checkLegalityAfterRewrite.
        checkGramNumValue(FoldConstantRuleOnFE.evaluateWithoutContext(gramNum));
    }

    @Override
    public void checkLegalityAfterRewrite() {
        // Constant folding (FE or BE) may have produced the literal by now. A constant that is
        // still not a literal is evaluated by BE, which rejects a nonpositive gram_num itself.
        checkGramNumValue(getArgument(2));
    }

    private static void checkGramNumValue(Expression gramNum) {
        if (gramNum instanceof NullLiteral) {
            throw new AnalysisException(
                    "ngram_search(text,pattern,gram_num): gram_num support const value only.");
        }
        if (gramNum instanceof IntegerLikeLiteral && ((IntegerLikeLiteral) gramNum).getLongValue() <= 0) {
            throw new AnalysisException(
                    "ngram_search(text,pattern,gram_num): gram_num must be a positive constant.");
        }
    }

    /**
     * withChildren.
     */
    @Override
    public NgramSearch withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new NgramSearch(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNgramSearch(this, context);
    }
}
