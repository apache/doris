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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

/**
 * F = '2022-01-01 00:01:01' or F = 'date2' or ...
 * =>
 * min_date <= F <= max_date
 */
public class RewriteRangePartitionPredicate extends
        DefaultExpressionRewriter<CascadesContext> {
    private final double rangeLength;

    public RewriteRangePartitionPredicate(double rangeLength) {
        this.rangeLength = rangeLength;
    }

    public static Expression rewrite(Expression expression, double rangeLength, CascadesContext ctx) {
        return expression.accept(new RewriteRangePartitionPredicate(rangeLength), ctx);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, CascadesContext ctx) {
        if (inPredicate.getOptions().size() > 10) {
            Expression opt0 = inPredicate.getOptions().get(0);
            if (opt0 instanceof DateLiteral || opt0 instanceof DateTimeLiteral) {
                Literal minOpt = (Literal) inPredicate.getOptions().get(0);
                Double minVal = minOpt.getDouble();
                Literal maxOpt = (Literal) inPredicate.getOptions().get(0);
                Double maxVal = maxOpt.getDouble();
                for (int i = 1; i < inPredicate.getOptions().size(); i++) {
                    Expression opt = inPredicate.getOptions().get(i);
                    if (!(opt instanceof Literal)) {
                        return inPredicate;
                    }
                    double optValue = ((Literal) opt).getDouble();
                    if (optValue < minVal) {
                        minVal = optValue;
                        minOpt = (Literal) opt;
                    } else if (optValue > maxVal) {
                        maxVal = optValue;
                        maxOpt = (Literal) opt;
                    }
                }
                double inToMinMaxThreshold = ctx.getConnectContext()
                        .getSessionVariable().inToMinmaxParitionRewriteThreshold;
                if (((maxVal - minVal) / rangeLength) < inToMinMaxThreshold) {
                    return new And(
                            new GreaterThanEqual(inPredicate.getCompareExpr(), minOpt),
                            new LessThanEqual(inPredicate.getCompareExpr(), maxOpt));
                }
            }
        }
        return inPredicate;
    }
}
