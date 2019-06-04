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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.cost.OptCost;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptOperator;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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
    private static final Logger LOG = LogManager.getLogger(OptExpression.class);

    private OptOperator op;
    private List<OptExpression> inputs;

    // store where this Expression has bound from, used to
    private MultiExpression mExpr;
    // Store the logical property including schema ...
    private OptProperty property;
    private Statistics statistics;
    private OptCost cost;

    private OptExpression(OptOperator op) {
        this.op = op;
        inputs = Lists.newArrayList();
    }

    private OptExpression(OptOperator op, List<OptExpression> inputs) {
        this.op = op;
        this.inputs = inputs;
    }

    private OptExpression(OptOperator op, OptExpression... inputs) {
        this.op =  op;
        this.inputs = Lists.newArrayList(inputs);
    }

    private OptExpression(OptOperator op, List<OptExpression> inputs,
                          MultiExpression mExpr,
                          OptCost cost, Statistics stats) {
        this.op = op;
        this.inputs = inputs;
        this.mExpr = mExpr;
        this.cost = cost;
        this.statistics = stats;
    }

    private OptExpression(MultiExpression mExpr) {
        this(mExpr, Lists.newArrayList());
    }

    private OptExpression(MultiExpression mExpr, List<OptExpression> inputs) {
        this.inputs = inputs;
        this.mExpr = mExpr;
        copyPropertyAndStatistics();
    }

    private void copyPropertyAndStatistics() {
        this.op = mExpr.getOp();
        this.property = mExpr.getGroup().getProperty();
        this.statistics = mExpr.getGroup().getStatistics();
    }

    public static OptExpression create(OptOperator op, OptExpression... inputs) {
        return new OptExpression(op, inputs);
    }
    public static OptExpression create(OptOperator op, List<OptExpression> inputs) {
        return new OptExpression(op, inputs);
    }

    public static OptExpression create(OptOperator op, List<OptExpression> inputs,
                                       MultiExpression mExpr,
                                       OptCost cost, Statistics stats) {
        return new OptExpression(op, inputs, mExpr, cost, stats);
    }

    public static OptExpression createBindingLeafExpression(MultiExpression source) {
        return new OptExpression(source);
    }

    public static OptExpression createBindingInternalExpression(
            MultiExpression source, List<OptExpression> boundInputs) {
        return new OptExpression(source, boundInputs);
    }

    public OptOperator getOp() { return op; }
    public List<OptExpression> getInputs() { return inputs; }
    public int arity() { return inputs.size(); }
    public OptExpression getInput(int idx) { return inputs.get(idx); }
    public MultiExpression getMExpr() { return mExpr; }
    public void setProperty(OptProperty property) { this.property = property; };
    public OptProperty getProperty() { return property; }
    public OptLogicalProperty getLogicalProperty() {
        final OptProperty property = deriveProperty();
        Preconditions.checkArgument(property instanceof OptLogicalProperty);
        return (OptLogicalProperty)property;
    }
    public OptItemProperty getItemProperty() {
        final OptProperty property = deriveProperty();
        Preconditions.checkArgument(property instanceof OptItemProperty);
        return (OptItemProperty) deriveProperty();
    }
    public OptPhysicalProperty getPhysicalProperty() { return (OptPhysicalProperty) deriveProperty(); }
    public Statistics getStatistics() { return statistics; }
    public void setStatistics(Statistics statistics) { this.statistics = statistics; }


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

    // This function will drive inputs' property first, then derive itself's
    // property
    public OptProperty deriveProperty() {
        if (property != null) {
            return property;
        }
        for (OptExpression input : inputs) {
            input.deriveProperty();
        }
        OptExpressionHandle handle = new OptExpressionHandle(this);
        handle.deriveExpressionLogicalOrItemProperty();
        property = handle.getProperty();
        return property;
    }

    public Statistics deriveStat(RequiredLogicalProperty requiredProperty) {
        if (statistics != null
                && !requiredProperty.isDifferent(statistics.getProperty())) {
            return statistics;
        }

        final OptExpressionHandle expressionHandle = new OptExpressionHandle(this);
        expressionHandle.deriveExpressionStats(requiredProperty);
        statistics = expressionHandle.getStatistics();
        return statistics;
    }

    @Override
    public int hashCode() {
        int hash = op.hashCode();
        for (OptExpression input : inputs) {
            hash = OptUtils.combineHash(hash, input.hashCode());
        }
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof OptExpression)) {
            return false;
        }
        OptExpression rhs = (OptExpression) object;
        if (this == rhs) {
            return true;
        }
        if (arity() != rhs.arity()) return false;
        if (!op.equals(rhs.op)) return false;
        // TODO(zc): support order and unorder
        for (int i = 0; i < arity(); ++i) {
            if (!inputs.get(i).equals(rhs.inputs.get(i))) {
                return false;
            }
        }
        return true;
    }
}
