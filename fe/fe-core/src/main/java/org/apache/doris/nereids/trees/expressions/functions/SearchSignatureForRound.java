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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.coercion.Int32OrLessType;

import java.util.ArrayList;
import java.util.List;

/**
 * Signature search for round-like functions (round, round_bankers, ceil, floor, truncate).
 *
 * For float/double inputs the result type defaults to double, which is correct except
 * when a non-negative integer-literal scale is given: the rounded double cannot land on a
 * clean N-decimal value (e.g. round(23900/293, 2) stores 81.56999999999999488…
 * in IEEE-754 and renders as "81.56999999999999").
 * In that case we route the call through the existing decimal signature, which both
 * gives the user the value they asked for and matches MySQL's behavior.
 */
public interface SearchSignatureForRound extends ExplicitlyCastableSignature {
    @Override
    default FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        List<Expression> arguments = getArguments();
        if (arguments.get(0).getDataType().isFloatLikeType()) {
            if (arguments.size() == 1) {
                return FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE);
            } else if (arguments.size() == 2) {
                if (isNonNegativeIntegerLiteral(arguments.get(1))) {
                    return ExplicitlyCastableSignature.super.searchSignature(
                            withoutFloatLikeReturnTypes(signatures));
                }
                return FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE, IntegerType.INSTANCE);
            }
        }
        return ExplicitlyCastableSignature.super.searchSignature(signatures);
    }

    /**
     * True iff scale folds to a non-negative integer literal.
     */
    static boolean isNonNegativeIntegerLiteral(Expression scale) {
        Expression folded = scale;
        if (!folded.isLiteral() && !folded.isSlot()) {
            ExpressionRewriteContext ctx = new ExpressionRewriteContext(CascadesContext.initTempContext());
            folded = FoldConstantRuleOnFE.evaluate(folded, ctx);
        }
        Expression unwrapped = folded;
        if (unwrapped instanceof Cast && unwrapped.child(0).isLiteral()
                && unwrapped.child(0).getDataType() instanceof Int32OrLessType) {
            unwrapped = unwrapped.child(0);
        }
        if (unwrapped instanceof IntegerLikeLiteral) {
            return ((IntegerLikeLiteral) unwrapped).getIntValue() >= 0;
        }
        return false;
    }

    /** Drop signatures whose return type is a float-like type, so the search falls onto decimal. */
    static List<FunctionSignature> withoutFloatLikeReturnTypes(List<FunctionSignature> signatures) {
        List<FunctionSignature> result = new ArrayList<>(signatures.size());
        for (FunctionSignature signature : signatures) {
            if (!signature.returnType.isFloatLikeType()) {
                result.add(signature);
            }
        }
        return result;
    }
}
