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

import org.apache.doris.nereids.exceptions.AnalysisIllegalParamException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * ScalarFunction 'approx_cosine_similarity'
 */
public class ApproxCosineSimilarity extends ApproxVectorDistanceFunc {
    private static final String NAME = "approx_cosine_similarity";
    private static final String VECTOR_RANGE_ERROR_MSG = "The vectorRange needs to be between -1 and 1.";

    public ApproxCosineSimilarity(Expression arg0, Expression arg1) {
        super(NAME, arg0, arg1);
    }

    public static String name() {
        return NAME;
    }

    /**
     * withChildren.
     */
    @Override
    public ApproxCosineSimilarity withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ApproxCosineSimilarity(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitApproxCosineSimilarity(this, context);
    }

    @Override
    public void checkVectorRange(Expression expr) throws AnalysisIllegalParamException {
        if (expr instanceof LessThan) {
            if ((expr.child(0) instanceof ApproxCosineSimilarity) && expr.child(1) instanceof Cast) {
                Literal e = (Literal) expr.child(1).child(0);
                if (e.getDouble() <= -1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
            if ((expr.child(1) instanceof ApproxCosineSimilarity) && expr.child(0) instanceof Cast) {
                Literal e = (Literal) expr.child(0).child(0);
                if (e.getDouble() >= 1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
        }
        if (expr instanceof LessThanEqual) {
            if ((expr.child(0) instanceof ApproxCosineSimilarity) && expr.child(1) instanceof Cast) {
                Literal e = (Literal) expr.child(1).child(0);
                if (e.getDouble() < -1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
            if ((expr.child(1) instanceof ApproxCosineSimilarity) && expr.child(0) instanceof Cast) {
                Literal e = (Literal) expr.child(0).child(0);
                if (e.getDouble() > 1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
        }
        if (expr instanceof GreaterThan) {
            if ((expr.child(0) instanceof ApproxCosineSimilarity) && expr.child(1) instanceof Cast) {
                Literal e = (Literal) expr.child(1).child(0);
                if (e.getDouble() >= 1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
            if ((expr.child(1) instanceof ApproxCosineSimilarity) && expr.child(0) instanceof Cast) {
                Literal e = (Literal) expr.child(0).child(0);
                if (e.getDouble() <= -1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
        }
        if (expr instanceof GreaterThanEqual) {
            if ((expr.child(0) instanceof ApproxCosineSimilarity) && expr.child(1) instanceof Cast) {
                Literal e = (Literal) expr.child(1).child(0);
                if (e.getDouble() > 1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
            if ((expr.child(1) instanceof ApproxCosineSimilarity) && expr.child(0) instanceof Cast) {
                Literal e = (Literal) expr.child(0).child(0);
                if (e.getDouble() < -1) {
                    throw new AnalysisIllegalParamException(VECTOR_RANGE_ERROR_MSG);
                }
            }
        }
    }
}
