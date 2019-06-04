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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.base.OptCostContext;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.cost.CostModel;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptLogical;
import org.apache.doris.optimizer.operator.OptOperator;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

// MultiExpression is another way to represent OptExpression, which
// contains an operator and inputs.
// Because MultiExpression's inputs are Groups, so one MultiExpression
// equal with several logical equivalent Expression. As a result, this
// can reduce search space dramatically.
public class MultiExpression {
    private static final Logger LOG = LogManager.getLogger(MultiExpression.class);
    public static final int INVALID_ID = -1;
    private int id;
    private OptOperator op;
    private List<OptGroup> inputs;
    private MEState status;
    private List<MultiExpression> implementedMExprs;

    // OptGroup which this MultiExpression belongs to. Firstly it's null when object is created,
    // it will be assigned after it is inserted into OptMemo.
    // Note that parent group can't be involved in the calculation of the hash value and the equivalence!!!
    private OptGroup group;

    // next MultiExpression in same OptGroup, set with group
    private MultiExpression next;
    private OptRuleType ruleTypeDerivedFrom;
    private int sourceMExprId;

    public MultiExpression(OptOperator op, List<OptGroup> inputs, OptRuleType ruleTypeDerivedFrom, int sourceMExprId) {
        this.op = op;
        this.inputs = inputs;
        this.status = MEState.UnImplemented;
        this.ruleTypeDerivedFrom = OptRuleType.RULE_NONE;
        this.ruleTypeDerivedFrom = ruleTypeDerivedFrom;
        this.sourceMExprId = sourceMExprId;
        this.implementedMExprs = Lists.newArrayList();
    }

    public MultiExpression(OptOperator op, List<OptGroup> inputs) {
        this(op, inputs, OptRuleType.RULE_NONE, -1);
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public OptOperator getOp() { return op; }
    public int arity() { return inputs.size(); }
    public List<OptGroup> getInputs() { return inputs; }
    public OptGroup getInput(int idx) { return inputs.get(idx); }
    public void setGroup(OptGroup group) { this.group = group; }
    public OptGroup getGroup() { return group; }
    public OptRuleType getRuleTypeDerivedFrom() { return ruleTypeDerivedFrom; }
    public void setStatus(MEState status) { this.status = status; }
    public MEState getStatus() { return status; }
    public boolean isImplemented() { return status == MEState.Implemented || op.isPhysical(); }
    public boolean isOptimized() { return status == MEState.Optimized; }
    public void setNext(MultiExpression next) { this.next = next; }
    public void setInvalid() { this.group = null; }
    public boolean isValid() { return this.group != null; }
    // get next MultiExpression in same group
    public MultiExpression next() { return next; }
    public List<MultiExpression> getImplementedMExprs() { return implementedMExprs; }

    public void addImplementedMExpr(MultiExpression mExpr) {
        Preconditions.checkArgument(mExpr.getOp().isPhysical(),
                "Add logical MultiExpression to implemented MultiExpression list.");
        this.implementedMExprs.add(mExpr);
    }

    public OptCostContext computeCost(OptimizationContext optContext, CostModel costModel) {
        final OptCostContext costContext = new OptCostContext(this, optContext);
        optContext.getChildrenOptContext().stream().forEach(
                childOptContext -> costContext.addChildrenOptContext(childOptContext));
        costContext.compute(costModel);
        return costContext;
    }

    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(":").append(op.debugString()).append('[');
        Joiner joiner = Joiner.on(',');
        joiner.join(inputs.stream().map(group -> "" + group.getId()).collect(Collectors.toList()));
        sb.append(']');
        return sb.toString();
    }

    public final String getExplainString() {
        return getExplainString("", "", OptGroup.ExplainType.ALL);
    }

    public String getLogicalExplainString(String headlinePrefix, String detailPrefix) {
        return getExplainString("", "", OptGroup.ExplainType.LOGICAL);
    }

    public String getPhysicalExplainString(String headlinePrefix, String detailPrefix) {
        return getExplainString("", "", OptGroup.ExplainType.PHYSICAL);
    }

    public String getExplainString(String headlinePrefix, String detailPrefix, OptGroup.ExplainType type) {
        StringBuilder sb = new StringBuilder();
        sb.append(headlinePrefix).append("MultiExpression ").append(id);
        if (ruleTypeDerivedFrom != OptRuleType.RULE_NONE) {
            sb.append(" (from MultiExpression: ")
                    .append(sourceMExprId)
                    .append(" rule:")
                    .append(ruleTypeDerivedFrom)
                    .append(" Status:")
                    .append(status.toString())
                    .append(")");
        }
        sb.append(' ')
                .append(op.getExplainString(detailPrefix)).append('\n');
        String childHeadlinePrefix = detailPrefix + OptUtils.HEADLINE_PREFIX;
        String childDetailPrefix = detailPrefix + OptUtils.DETAIL_PREFIX;
        for (OptGroup input : inputs) {
            sb.append(input.getExplain(childHeadlinePrefix, childDetailPrefix, type));
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = op.hashCode();
        for (OptGroup group : inputs) {
            hash = OptUtils.combineHash(hash, group.hashCode());
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof MultiExpression)) {
            return false;
        }
        MultiExpression rhs = (MultiExpression) obj;
        if (this == rhs) {
            return true;
        }
        if (arity() != rhs.arity() || !op.equals(rhs.getOp())) {
            return false;
        }
        for (int i = 0; i < arity(); ++i) {
            if (!inputs.get(i).duplicateWith(rhs.getInput(i))) {
                return false;
            }
        }
        return true;
    }

    public enum MEState {
        UnImplemented,
        Implementing,
        Implemented,
        Optimizing,
        Optimized
    }
}
