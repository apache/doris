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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

// A Group contains all logical equivalent logical MultiExpressions
// and physical MultiExpressions
public class OptGroup {
    private static final Logger LOG = LogManager.getLogger(OptGroup.class);

    private int id;
    private boolean isItem;
    private List<MultiExpression> mExprs = Lists.newArrayList();
    private static int nextMExprId = 1;
    private GState status;
    private Map<OptimizationContext, OptimizationContext> optContextMap;
    private OptProperty property;
    private Statistics statistics;
    private OptExpression itemExpression;
    private boolean isItemGroup;

    public OptGroup(int id, OptProperty property) {
        this.id = id;
        this.status = GState.Unimplemented;
        this.property = property;
        this.optContextMap = Maps.newHashMap();
        this.isItemGroup = false;
    }

    // Add a new MultiExpression which haven't been added to other group.
    // this function will create relationship between this group and MultiExpression
    public void addMExpr(MultiExpression mExpr) {
        insertMExpr(mExpr);
        mExpr.setId(nextMExprId++);
    }

    // Move a MultiExpression which have been added to other group to this group.
    public void moveMExpr(MultiExpression mExpr) {
        insertMExpr(mExpr);
    }

    private void insertMExpr(MultiExpression mExpr) {
        int numExprs = mExprs.size();
        if (numExprs > 0) {
            mExprs.get(numExprs - 1).setNext(mExpr);
        }
        mExprs.add(mExpr);
        mExpr.setGroup(this);
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

    public String getExplain() {
        return getExplain("", "");
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
    public boolean isItem() { return isItem; }
    public List<MultiExpression> getMultiExpressions() { return mExprs; }
    public OptimizationContext lookUp(OptimizationContext newContext) { return optContextMap.get(newContext); }
    public void setStatus(GState status) { this.status = status; }
    public GState getStatus() { return status; }
    public OptProperty getProperty() { return property; }
    public void setProperty(OptProperty property) { this.property = property; }
    public Statistics getStatistics() { return statistics; }
    public void setStatistics(Statistics statistics) { this.statistics = statistics; }
    public OptExpression getItemExpression() { return itemExpression; }

    public void mergeGroup(OptGroup other) {
        if (other == this) {
            return;
        }

        Preconditions.checkState(mExprs.size() > 0);
        MultiExpression lastMExpr = mExprs.get(mExprs.size() - 1);
        Preconditions.checkState(lastMExpr.next() == null);

        for (MultiExpression mExpr : other.getMultiExpressions()) {
            lastMExpr.setNext(mExpr);
            lastMExpr = mExpr;
            mExprs.add(mExpr);
            mExpr.setGroup(this);
        }
        other.mExprs.clear();
    }

    public void removeMExpr(MultiExpression removeMExpr) {
        final Set<MultiExpression> allRemovedMExpr = Sets.newHashSet();
        allRemovedMExpr.add(removeMExpr);
        // The physical MultiExpression from the removeMExpr
        for (MultiExpression mExpr : removeMExpr.getImplementedMExprs()) {
            allRemovedMExpr.add(mExpr);
        }

        final Iterator<MultiExpression> iterator = mExprs.iterator();
        MultiExpression lastMExpr = null;
        while (iterator.hasNext()) {
            final MultiExpression mExpr = iterator.next();
            if (allRemovedMExpr.remove(mExpr)) {
                iterator.remove();
                mExpr.setInvalid();
                if (lastMExpr != null) {
                    lastMExpr.setNext(mExpr.next());
                }
            }
            if (lastMExpr == null || lastMExpr.next() == mExpr) {
                lastMExpr = mExpr;
            }
        }
    }

    public void deriveStat(RequiredLogicalProperty reqLogicalProperty) {
        if (isItemGroup) {
            return;
        }

        if (statistics != null && reqLogicalProperty != null) {
            if (statistics.getStatColumns().contains(reqLogicalProperty.getColumns())) {
                return;
            }
        }

        final MultiExpression bestMExpr = getBestPromiseMExpr();
        final OptExpressionHandle exprHandle = new OptExpressionHandle(bestMExpr);
        exprHandle.deriveStat(reqLogicalProperty);
        statistics = exprHandle.getStatistics();
    }

    private MultiExpression getBestPromiseMExpr() {
        return mExprs.get(0);
    }

    public enum GState {
        Unimplemented,
        Implementing,
        Implemented,
        Optimizing,
        Optimized
    }

    public void updateBestCost(OptimizationContext optContext, OptCostContext costContext) {
        final OptimizationContext existOptContext = optContextMap.get(optContext);
        if (existOptContext != null) {
            final OptCostContext existCostContext = existOptContext.getBestCostCtx();
            if (costContext.isBetterThan(existCostContext)) {
                existOptContext.setBestCostCtx(costContext);
            }
        } else {
            optContext.setBestCostCtx(costContext);
            optContextMap.put(optContext, optContext);
        }
    }

    public OptimizationContext lookupBest(RequiredPhysicalProperty property) {
        OptimizationContext optCtx = new OptimizationContext(this, property);
        return optContextMap.get(optCtx);
    }

}
