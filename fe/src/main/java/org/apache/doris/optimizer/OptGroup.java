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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.search.OptimizationContext;

import java.util.List;
import java.util.Map;

// A Group contains all logical equivalent logical MultiExpressions
// and physical MultiExpressions
public class OptGroup {
    private int id;
    private List<MultiExpression> mExprs = Lists.newArrayList();
    private int nextMExprId = 1;
    private GState status;
    private Map<OptimizationContext, OptimizationContext> optContextMap;

    public OptGroup(int id) {
        this.id = id;
        this.status = GState.Unimplemented;
        this.optContextMap = Maps.newHashMap();
    }

    // Add a MultiExpression,
    // this function will create relationship between this group and MultiExpression
    public void addMExpr(MultiExpression mExpr) {
        int numExprs = mExprs.size();
        if (numExprs > 0) {
            mExprs.get(numExprs - 1).setNext(mExpr);
        }
        mExpr.setId(nextMExprId++);
        mExpr.setGroup(this);
        mExprs.add(mExpr);
    }

    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Group: ").append(id);
        return sb.toString();
    }

    public String getExplain(String headlinePrefix, String detailPrefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(headlinePrefix).append("Group:").append(id).append('\n');
        String childHeadlinePrefix = detailPrefix + OptUtils.HEADLINE_PREFIX;
        String childDetailPrefix = detailPrefix + OptUtils.DETAIL_PREFIX;
        for (MultiExpression mExpr : mExprs) {
            sb.append(mExpr.getExplainString(childHeadlinePrefix, childDetailPrefix));
        }
        return sb.toString();
    }

    public boolean duplicateWith(OptGroup other) {
        return this == other;
    }


    public MultiExpression getFirstMultiExpression() {
        if (mExprs.isEmpty()) { return null; }
        return mExprs.get(0);
    }

    public int getId() { return id; }
    public boolean isImplemented() { return status == GState.Implemented; }
    public boolean isOptimized() { return status == GState.Optimized; }
    public List<MultiExpression> getMultiExpressions() { return mExprs; }
    public OptimizationContext lookUp(OptimizationContext newContext) { return optContextMap.get(newContext); }
    public void setStatus(GState status) { this.status = status; }

    public enum GState {
        Unimplemented,
        Implementing,
        Implemented,
        Optimizing,
        Optimized
    }
}
