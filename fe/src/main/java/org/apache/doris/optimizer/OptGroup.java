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
    private static int nextMExprId = 1;
    private int id;
    private List<MultiExpression> mExprs = Lists.newArrayList();
    private GState status;
    private Map<OptimizationContext, OptimizationContext> optContextMap;
    private OptProperty logicalOrItemProperty;
    private Statistics statistics;
    private OptExpression itemExpression;
    private boolean isItemGroup;

    public OptGroup(int id) {
        this(id, false);
    }

    public OptGroup(int id, boolean isItemGroup) {
        this.id = id;
        this.status = GState.Unimplemented;
        this.optContextMap = Maps.newHashMap();
        this.isItemGroup = isItemGroup;
    }

    // Add a new MultiExpression which haven't been added to other group.
    // this function will create relationship between this group and MultiExpression
    public void addMExpr(MultiExpression mExpr) {
        insertMExpr(mExpr);
        mExpr.setId(nextMExprId++);
        checkMExprListStatus();
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
        checkMExprListStatus();
    }

    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Group: ").append(id);
        return sb.toString();
    }

    public String getExplain(String headlinePrefix, String detailPrefix, ExplainType type) {
        StringBuilder sb = new StringBuilder();
        sb.append(headlinePrefix).append("Group:").append(id).append(" Status:").append(status.toString()).append('\n');
        String childHeadlinePrefix = detailPrefix + OptUtils.HEADLINE_PREFIX;
        String childDetailPrefix = detailPrefix + OptUtils.DETAIL_PREFIX;
        for (MultiExpression mExpr : mExprs) {
            if ((type == ExplainType.LOGICAL && !mExpr.getOp().isLogical())
                    || (type == ExplainType.PHYSICAL && !mExpr.getOp().isPhysical())) {
                continue;
            }
            sb.append(mExpr.getExplainString(childHeadlinePrefix, childDetailPrefix, type));
        }
        return sb.toString();
    }

    public String getExplain() {
        return getExplain("", "", ExplainType.ALL);
    }

    public String getExplain(String headlinePrefix, String detailPrefix) {
        return getExplain(headlinePrefix, detailPrefix, ExplainType.ALL);
    }

    public String getPhysicalExplain() {
        return getExplain("", "", ExplainType.PHYSICAL);
    }

    public String getLogicalExplain() {
        return getExplain("", "", ExplainType.LOGICAL);
    }

    public boolean duplicateWith(OptGroup other) {
        return this == other;
    }

    public MultiExpression getFirstMultiExpression() {
        if (mExprs.isEmpty()) {
            return null;
        }
        return mExprs.get(0);
    }

    public MultiExpression getFirstLogicalMultiExpression() {
        if (mExprs.isEmpty()) {
            return null;
        }
        final MultiExpression first = mExprs.get(0);
        if (first.getOp().isLogical()) {
            return first;
        }
        return nextLogicalExpr(first);
    }

    public MultiExpression getFirstPhysicalMultiExpression() {
        if (mExprs.isEmpty()) {
            return null;
        }
        final MultiExpression first = mExprs.get(0);
        if (first.getOp().isPhysical()) {
            return first;
        }

        return nextPhysicalExpr(first);
    }

    public MultiExpression nextLogicalExpr(MultiExpression mExpr) {
        if (mExpr == null) {
            return null;
        }
        MultiExpression nextExpr = mExpr;
        while (nextExpr.next() != null && !nextExpr.next().getOp().isLogical()) {
            nextExpr = nextExpr.next();
        }
        return nextExpr.next();
    }

    public MultiExpression nextPhysicalExpr(MultiExpression mExpr) {
        if (mExpr == null) {
            return null;
        }
        MultiExpression nextExpr = mExpr;
        while (nextExpr.next() != null && !nextExpr.next().getOp().isPhysical()) {
            nextExpr = nextExpr.next();
        }
        return nextExpr.next();
    }

    public MultiExpression nextExpr(MultiExpression mExpr) {
        return mExpr.next();
    }

    public int getId() { return id; }

    public boolean isImplemented() { return status == GState.Implemented; }

    public boolean isOptimized() { return status == GState.Optimized; }

    public List<MultiExpression> getMultiExpressions() { return mExprs; }

    public GState getStatus() { return status; }

    public void setStatus(GState status) { this.status = status; }

    public OptProperty getProperty() { return logicalOrItemProperty; }

    public void setProperty(OptProperty property) { this.logicalOrItemProperty = property; }

    public Statistics getStatistics() { return statistics; }

    public void setStatistics(Statistics statistics) { this.statistics = statistics; }

    public boolean isItemGroup() {
        return isItemGroup;
    }

    public void setItemGroup(boolean value) {
        this.isItemGroup = value;
    }

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
        checkMExprListStatus();
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
        checkMExprListStatus();
    }

    public void deriveStat(RequiredLogicalProperty requiredLogicalProperty) {
        Preconditions.checkNotNull(requiredLogicalProperty, "Required property can't be null.");
        if (isItemGroup) {
            return;
        }

        if (statistics != null && requiredLogicalProperty != null) {
            if (!requiredLogicalProperty.isDifferent(statistics.getProperty())) {
                return;
            }
        }

        final MultiExpression bestMExpr = getBestPromiseMExpr();
        final OptExpressionHandle exprHandle = new OptExpressionHandle(bestMExpr);
        exprHandle.deriveMultiExpressionLogicalOrItemProperty();
        exprHandle.deriveMultiExpressionStats(requiredLogicalProperty);
        statistics = exprHandle.getStatistics();
    }

    public MultiExpression getBestPromiseMExpr() {
        MultiExpression candidate = null;
        for (MultiExpression mExpr : mExprs) {
            if (mExpr.getOp().isLogical()) {
                candidate = mExpr;
                break;
            }
        }
        return candidate;
    }

    public MultiExpression getBestPromiseMExpr(MultiExpression matchMExpr) {
        MultiExpression firstLogicalMExpr = getFirstLogicalMultiExpression();
        MultiExpression bestPromiseMExpr = null;
        while (firstLogicalMExpr != null) {
            if (matchNonItemMExprChildren(matchMExpr, firstLogicalMExpr)
                    && hasBetterPromise(matchMExpr, firstLogicalMExpr)) {
                bestPromiseMExpr = firstLogicalMExpr;
            }
            firstLogicalMExpr = nextLogicalExpr(firstLogicalMExpr);
        }
        return bestPromiseMExpr;
    }

    private boolean hasBetterPromise(MultiExpression matchMExpr, MultiExpression candidateMExpr) {
        //TODO ch
        return true;
    }

    private boolean matchNonItemMExprChildren(MultiExpression matchMExpr, MultiExpression candidateMExpr) {
        if (matchMExpr.getInputs().size() == 0 && candidateMExpr.getInputs().size() == 0) {
            return true;
        }

        if (matchMExpr.getInputs().size() != candidateMExpr.getInputs().size()) {
            return false;
        }

        for (int childIndex = 0; childIndex < matchMExpr.getInputs().size(); childIndex++) {
            if (matchMExpr.getInput(childIndex) != candidateMExpr.getInput(childIndex)) {
                return false;
            }
        }
        return true;
    }

    public void insert(OptimizationContext newContext) {
        Preconditions.checkArgument(optContextMap.put(newContext, newContext) == null);
    }

    public OptimizationContext lookUp(OptimizationContext newContext) {
        return optContextMap.get(newContext);
    }


    public void updateBestCost(OptimizationContext optContext, OptCostContext costContext) {
        final OptimizationContext existOptContext = optContextMap.get(optContext);
        if (existOptContext != null) {
            final OptCostContext existCostContext = existOptContext.getBestCostCtx();
            if (existCostContext == null || costContext.isBetterThan(existCostContext)) {
                existOptContext.setBestCostCtx(costContext);
            }
        } else {
            optContext.setBestCostCtx(costContext);
            optContextMap.put(optContext, optContext);
        }
    }

    public OptimizationContext lookupBest(RequiredPhysicalProperty property) {
        OptimizationContext optCtx = new OptimizationContext(this, null, property);
        return optContextMap.get(optCtx);
    }

    public void createItemExpression() {
        Preconditions.checkArgument(mExprs.size() == 1);
        Preconditions.checkArgument(isItemGroup);

        final MultiExpression rootItemMExpression = mExprs.get(0);
        final OptItemProperty property = (OptItemProperty)logicalOrItemProperty;
        if (property.isHavingSubQuery()){
            itemExpression = OptExpression.create(rootItemMExpression.getOp());
            return;
        }

        final List<OptExpression> childItemExpressions = Lists.newArrayList();
        for (int i = 0; i < rootItemMExpression.getInputs().size(); i++) {
            final OptExpression childItemExpression = rootItemMExpression.getInput(i).getItemExpression();
            childItemExpressions.add(childItemExpression);
        }

        itemExpression = OptExpression.create(rootItemMExpression.getOp(), childItemExpressions);
    }

    private void checkMExprListStatus() {
        Preconditions.checkState(!checkDeadCycleInMExpr());

        MultiExpression nextMExpr = getFirstMultiExpression();
        int mExprNum = 1;
        while (nextMExpr.next() != null) {
            nextMExpr = nextMExpr.next();
            mExprNum++;
        }

        Preconditions.checkState(mExprs.size() == mExprNum);
    }

    private boolean checkDeadCycleInMExpr() {
        MultiExpression mExpr1 = getFirstMultiExpression();
        MultiExpression mExpr2 = mExpr1;

        if (mExpr1 == null || mExpr1.next() == null) {
            return false;
        }

        while (mExpr1 != null && mExpr2 != null) {
            mExpr1 = mExpr1.next();
            mExpr2 = mExpr2.next();
            if (mExpr2 != null) {
                mExpr2 = mExpr2.next();
            }

            if (mExpr1 == mExpr2 && mExpr1 != null) {
                return true;
            }
        }
        return false;
    }

    public enum GState {
        Unimplemented,
        Implementing,
        Implemented,
        Optimizing,
        Optimized
    }

    enum ExplainType {
        PHYSICAL,
        LOGICAL,
        ALL
    }

}
