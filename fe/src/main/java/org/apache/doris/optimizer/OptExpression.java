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

package org.apache.doris.optimizer;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.operator.OptOperator;

import java.util.List;
import java.util.stream.Collectors;

// OptExpression consists of OptOperator and its inputs
// New objects would be created in several scenario
// 1. When someone wants to optimize a new query, he/she should new an object
//    and copyIn it to OptMemo to start an optimization
// 2. When optimization has finished, an optimized OptExpression will be extracted
//    from OptMemo and returned to client
// 3. Each rules should create its own pattern OptExpression which will be used to
//    match MultiExpression
// 4. When a rule can reply, OptBinding will create a new OptExpression to represent
//    substituted expression for each binding
public class OptExpression {
    private OptOperator op;
    private List<OptExpression> inputs;

    // store where this Expression has bound from, used to
    private MultiExpression mExpr;

    public static OptExpression create(OptOperator op, OptExpression... inputs) {
        return new OptExpression(op, inputs);
    }
    public static OptExpression create(OptOperator op, List<OptExpression> inputs) {
        return new OptExpression(op, inputs);
    }

    public OptExpression(OptOperator op) {
        this.op = op;
        inputs = Lists.newArrayList();
    }

    public OptExpression(OptOperator op, OptExpression... inputs) {
        this.op = op;
        this.inputs = Lists.newArrayList(inputs);
    }

    public OptExpression(OptOperator op, List<OptExpression> inputs) {
        this.op = op;
        this.inputs = inputs;
    }

    public OptExpression(OptOperator op, MultiExpression mExpr) {
        this(op, Lists.newArrayList(), mExpr);
    }

    public OptExpression(OptOperator op, List<OptExpression> inputs, MultiExpression mExpr) {
        this.op = op;
        this.inputs = inputs;
        this.mExpr = mExpr;
    }

    public OptOperator getOp() { return op; }
    public List<OptExpression> getInputs() { return inputs; }
    public int arity() { return inputs.size(); }
    public OptExpression getInput(int idx) { return inputs.get(idx); }
    public MultiExpression getMExpr() { return mExpr; }

    // It's only used when this object is part of pattern. this function check if
    // MultiExpression can match this Expression
    // Pattern match doesn't care operator's arguments
    public boolean matchMultiExpression(MultiExpression mExpr) {
        // If op is a pattern, it can match all
        if (op.isPattern()) {
            return true;
        }
        // because this is used to pattern match, just check op type rather than all
        if (op.getType() != mExpr.getOp().getType()) {
            return false;
        }
        return arity() == mExpr.arity();
    }

    public String debugString() { return getExplainString("", ""); }

    public final String getExplainString() {
        return getExplainString("", "");
    }
    // used for debugging
    public String getExplainString(String headlinePrefix, String detailPrefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(headlinePrefix).append(op.getExplainString(detailPrefix)).append('\n');
        String childHeadlinePrefix = detailPrefix + OptUtils.HEADLINE_PREFIX;
        String childDetailPrefix = detailPrefix + OptUtils.DETAIL_PREFIX;
        for (OptExpression input : inputs) {
            sb.append(input.getExplainString(childHeadlinePrefix, childDetailPrefix));
        }
        return sb.toString();
    }
}
