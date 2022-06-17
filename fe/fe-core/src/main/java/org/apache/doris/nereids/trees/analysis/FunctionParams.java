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

package org.apache.doris.nereids.trees.analysis;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * Function's Params.
 */
public class FunctionParams {
    private boolean isStar;
    private List<Expression> expression;
    private boolean isDistinct;

    /**
     * Constructor for FunctionParams.
     */
    public FunctionParams(boolean isDistinct, List<Expression> exprs) {
        isStar = false;
        this.isDistinct = isDistinct;
        this.expression = exprs;
    }

    public FunctionParams(boolean isDistinct, Expression expression) {
        this(isDistinct, Lists.newArrayList(expression));
    }


    // c'tor for non-star, non-distinct params
    public FunctionParams(Expression exprs) {
        this(false, exprs);
    }

    // c'tor for <agg>(*)
    private FunctionParams() {
        expression = null;
        isStar = true;
        isDistinct = false;
    }

    public List<Expression> getExpression() {
        return expression;
    }

    public boolean isStar() {
        return isStar;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public int hashCode() {
        int result = 31 * Boolean.hashCode(isStar) + Boolean.hashCode(isDistinct);
        if (expression != null) {
            for (Expression expr : expression) {
                result = 31 * result + Objects.hashCode(expr);
            }
        }
        return result;
    }
}
