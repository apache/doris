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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

/**
 * topk_merge_min(S, b, I, b_i, D, b_d, n, dn, k): merge a MIN top-K state with a delta batch.
 * See {@link TopKMergeFunction}.
 */
public class TopKMergeMin extends TopKMergeFunction {

    public TopKMergeMin(Expression s, Expression b, Expression i, Expression bI,
            Expression d, Expression bD, Expression n, Expression dn, Expression k) {
        super("topk_merge_min", Arrays.asList(s, b, i, bI, d, bD, n, dn, k));
    }

    /** constructor for withChildren and reuse signature */
    private TopKMergeMin(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public TopKMergeMin withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 9);
        return new TopKMergeMin(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTopKMergeMin(this, context);
    }
}
