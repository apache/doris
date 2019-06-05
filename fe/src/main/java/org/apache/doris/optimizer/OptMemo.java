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
import org.apache.doris.optimizer.cost.OptCost;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.RequiredPhysicalProperty;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.utils.MemoStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

// This is key component of our optimizer. We utilize
// dynamic programing to search optimal query plan. When
// searching, sub-problem's result is needed to be stored.
// In our optimizer, we store all MultiExpression that have
// been searched in this struct.
// Memo store all MultiExpression in a hash table, which is efficient
// to look up if there is already MultiExpression
// All the group should be created from this class
// There are several scenario to call copyIn
// 1. When starting search a query plan, copy the origin expression to Memo
//    and set it as root group.
// 2. When exploring an expression, new generated expression would be copied
//    into this memo with an specified group.
public class OptMemo {
    private static final Logger LOG = LogManager.getLogger(OptMemo.class);

    // store all groups
    private int nextGroupId = 1;
    private List<OptGroup> groups = Lists.newArrayList();
    private List<OptGroup> unusedGroups = Lists.newArrayList();
    private List<MultiExpression> unusedMExprs = Lists.newArrayList();
    private Map<MultiExpression, MultiExpression> mExprs = Maps.newHashMap();
    private OptGroup root;

    public OptMemo() {
        reset();
    }

    // copy an expression into search space, this function will add an MultiExpression for
    // this Expression. If this Expression has children, this function will be called
    // recursively to create MultiExpression and Group for every single Expression
    // For example, Join(Scan(A), Scan(B)) will create 3 Groups and MultiExpressions for Join,
    // Scan(A) and Scan(B).
    // We return MultiExpression rather than OptGroup because we can get OptGroup from MultiExpression
    public MultiExpression init(OptExpression originExpr) {
        reset();
        final MultiExpression rootMExpr = copyIn(null,
                originExpr, OptRuleType.RULE_NONE, MultiExpression.INVALID_ID);
        this.root = rootMExpr.getGroup();
        return rootMExpr;
    }

    private void reset() {
        nextGroupId = 1;
        groups.clear();
        unusedGroups.clear();
        unusedMExprs.clear();
        mExprs.clear();
        root = null;
    }

    public MultiExpression copyIn(OptExpression expr, OptRuleType type, int sourceMExprid) {
        return copyIn(null, expr, type, sourceMExprid);
    }

    public MultiExpression copyIn(OptGroup targetGroup, OptExpression expr) { return copyIn(targetGroup, expr, OptRuleType.RULE_NONE, MultiExpression.INVALID_ID);};

    // used to copy an Expression into this memo.
    // This function will create a MultiExpression and try to search in already stored expressions.
    // If we found one, we return the found one.
    // If we found none and targetGroup is specified, we add new MultiExpression to the group. This is
    // often used when applying rules, targetGroup is the group which is explored
    // If targetGroup is not specified, we should create a new group.
    public MultiExpression copyIn(OptGroup targetGroup, OptExpression expr, OptRuleType fromRule, int sourceMExprid) {
        List<OptGroup> inputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            OptGroup inputGroup;
            if (input.getMExpr() != null) {
                inputGroup = input.getMExpr().getGroup();
            } else {
                inputGroup = copyIn(input, fromRule, sourceMExprid).getGroup();
            }
            LOG.debug("inputGroup is {}", inputGroup.debugString());
            inputs.add(inputGroup);
        }
        MultiExpression mExpr = new MultiExpression(expr.getOp(), inputs, fromRule, sourceMExprid);
        // first get expr
        MultiExpression foundExpr = mExprs.get(mExpr);
        if (foundExpr != null) {
            mergeGroup(targetGroup, foundExpr.getGroup(), true);
            return foundExpr;
        }
        // we need to put mExpr to MultiExpressions map
        mExprs.put(mExpr, mExpr);
        // If targetGroup is specified, we add MultiExpression to this group
        if (targetGroup != null) {
            targetGroup.addMExpr(mExpr);
            return mExpr;
        }

        Preconditions.checkArgument(!expr.getOp().isPhysical());
        // targetGroup is null, we should create new OptGroup
        // we should derive property first to make sure expression has property
        expr.deriveProperty();

        final OptGroup newGroup = new OptGroup(nextGroupId++, expr.getOp().isItem());
        newGroup.addMExpr(mExpr);
        newGroup.setProperty(expr.getProperty());
        if (newGroup.isItemGroup()) {
            newGroup.createItemExpression();
        }

        groups.add(newGroup);
        return mExpr;
    }

    // Merge duplicate or equivalent group.
    private void mergeGroup(OptGroup srcGroup, OptGroup destGroup, boolean merge) {
        if (srcGroup == null || destGroup == null || srcGroup == destGroup) {
            // srcGroup == destGroup merge duplicate group
            return;
        }

        groups.remove(srcGroup);
        unusedGroups.add(srcGroup);
        // Replace children srcGroup with destGroup MultExpression refered
        // and MulExpression's host srcGroup with destGroup.
        final List<MultiExpression> newExprsNeedReinserted = Lists.newArrayList();
        final List<OptGroup> newExprsGroupNeedReinserted = Lists.newArrayList();
        for (Iterator<Map.Entry<MultiExpression, MultiExpression>>
             iterator =  mExprs.entrySet().iterator(); iterator.hasNext(); ) {
            final MultiExpression mExpr = iterator.next().getValue();
            final List<Integer> referSrcGroupIndexs = Lists.newArrayList();
            for (int i = 0; i < mExpr.getInputs().size(); i++) {
                if (mExpr.getInputs().get(i) == srcGroup) {
                    referSrcGroupIndexs.add(i);
                }
            }

            // Hash value changes and reinsert
            if (referSrcGroupIndexs.size() > 0) {
                Preconditions.checkState(referSrcGroupIndexs.size() == 1);
                iterator.remove();
                newExprsGroupNeedReinserted.add(mExpr.getGroup());
                mExpr.getGroup().removeMExpr(mExpr);
                mExpr.getInputs().set(referSrcGroupIndexs.get(0), destGroup);
                mExpr.setNext(null);
                newExprsNeedReinserted.add(mExpr);
            }
        }

        for (int i = 0; i < newExprsNeedReinserted.size(); i++) {
            final MultiExpression candicateMExpr = newExprsNeedReinserted.get(i);
            final OptGroup candicateGroup = newExprsGroupNeedReinserted.get(i);
            final MultiExpression foundMExpr = mExprs.get(candicateMExpr);
            if (foundMExpr != null) {
                Preconditions.checkNotNull(foundMExpr.getGroup());
                unusedMExprs.add(candicateMExpr);
            } else {
                candicateGroup.moveMExpr(candicateMExpr);
                mExprs.put(candicateMExpr, candicateMExpr);
            }
        }

        if (merge) {
            destGroup.mergeGroup(srcGroup);
        }

        if (LOG.isDebugEnabled()) {
            final MemoStatus ms = new MemoStatus(this);
            if (ms.checkLeakInDag()) {
                throw new RuntimeException("Exist Leak Group or MultiExpression!");
            }
        }
    }

    // used to dump current state of memo
    public String debugString() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    public List<OptGroup> getGroups() { return groups; }
    public Map<MultiExpression, MultiExpression> getMExprs() { return mExprs; }

    public List<MultiExpression> getLogicalMExprs() {
        final List<MultiExpression> logicalExprs = Lists.newArrayList();
        for (MultiExpression mExpr : mExprs.keySet()) {
            if (mExpr.getOp().isLogical()) {
                logicalExprs.add(mExpr);
            }
        }
        return logicalExprs;
    }

    public List<MultiExpression> getPhysicalMExprs() {
        final List<MultiExpression> physicalExprs = Lists.newArrayList();
        for (MultiExpression mExpr : mExprs.keySet()) {
            if (mExpr.getOp().isPhysical()) {
                physicalExprs.add(mExpr);
            }
        }
        return physicalExprs;
    }

    public OptGroup getRoot() { return this.root; }

    public void dump() {
       final String dumpStr = root.getExplain();
       if (root != null) {
           LOG.info("Memo:\n" + dumpStr);
       }
    }

    public void dumpUnusedGroups() {
        final StringBuilder strBuilder = new StringBuilder("Unused Groups Ids:");
        for (OptGroup group : unusedGroups) {
            strBuilder.append(group.getId()).append(", ");
        }
        LOG.info(strBuilder.toString());
    }

    public void dumpUnusedMexprs() {
        final StringBuilder strBuilder = new StringBuilder("Unused MultiExpressions Ids:");
        for (MultiExpression mExpr : unusedMExprs) {
            strBuilder.append("MExpr:").append(mExpr.getId()).append(" input ");
            for (OptGroup group : mExpr.getInputs()) {
                strBuilder.append(group.getId()).append(" ");
            }
        }
        LOG.info(strBuilder.toString());
    }

    public OptExpression extractExpression(OptGroup groupRoot, RequiredPhysicalProperty propertyInput) {
        Statistics stats = null;
        OptCost cost = null;
        OptimizationContext optCtx = null;
        MultiExpression bestMExpr = null;
        if (groupRoot.isItemGroup()) {
            bestMExpr = groupRoot.getFirstMultiExpression();
        } else {
            optCtx = groupRoot.lookupBest(propertyInput);
            bestMExpr = optCtx.getBestMultiExpr();
            if (bestMExpr != null) {
                cost = optCtx.getBestCostCtx().getCost();
                stats = optCtx.getBestCostCtx().getStatistics();
            }
        }
        if (bestMExpr == null) {
            return null;
        }
        List<OptExpression> exprChildren = Lists.newArrayList();
        for (int i = 0; i < bestMExpr.arity(); ++i) {
            RequiredPhysicalProperty childProp = null;
            OptGroup childGroup = bestMExpr.getInput(i);
            if (!childGroup.isItemGroup()) {
                OptimizationContext childOptCtx = optCtx.getBestCostCtx().getInput(i);
                childProp = childOptCtx.getRequiredPhysicalProperty();
            }
            OptExpression childExpr = extractExpression(childGroup, childProp);
            exprChildren.add(childExpr);
        }
        OptExpression extractedExpr = OptExpression.create(bestMExpr.getOp(), exprChildren, bestMExpr, cost, stats);
        return extractedExpr;
    }
}
