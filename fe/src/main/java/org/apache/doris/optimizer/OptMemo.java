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
import org.apache.doris.optimizer.rule.OptRuleType;
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
    }

    // copy an expression into search space, this function will add an MultiExpression for
    // this Expression. If this Expression has children, this function will be called
    // recursively to create MultiExpression and Group for every single Expression
    // For example, Join(Scan(A), Scan(B)) will create 3 Groups and MultiExpressions for Join,
    // Scan(A) and Scan(B).
    // We return MultiExpression rather than OptGroup because we can get OptGroup from MultiExpression
    public MultiExpression init(OptExpression originExpr) {
        final MultiExpression rootMExpr = copyIn(null,
                originExpr, OptRuleType.RULE_NONE, MultiExpression.INVALID_ID);
        this.root = rootMExpr.getGroup();
        return rootMExpr;
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
            LOG.info("inputGroup is {}", inputGroup.debugString());
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
        // targetGroup is null, we should create new OptGroup
        OptGroup newGroup = new OptGroup(nextGroupId++);
        newGroup.addMExpr(mExpr);
        groups.add(newGroup);
        // Derive property
        expr.deriveProperty();
        newGroup.setLogicalProperty(expr.getLogicalProperty());
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
                mExpr.getInputs().set(referSrcGroupIndexs.get(0), destGroup);
                newExprsNeedReinserted.add(mExpr);
            }
        }

        for (MultiExpression mExpr : newExprsNeedReinserted) {
            final MultiExpression foundMExpr = mExprs.get(mExpr);
            if (foundMExpr != null) {
                if (foundMExpr.getGroup() != mExpr.getGroup()) {
                    // TODO ch.
                    // Remove mExpr and update parent MultiExpression.
                    mergeGroup(mExpr.getGroup(), foundMExpr.getGroup(), false);
                } else {
                    unusedMExprs.add(mExpr);
                    foundMExpr.getGroup().removeMExpr(mExpr);
                    Preconditions.checkState(foundMExpr.getGroup().getMultiExpressions().size() > 0);
                }
            } else {
                mExprs.put(mExpr, mExpr);
            }
        }

        if (merge) {
            destGroup.mergeGroup(srcGroup);
        }
    }

    // used to dump current state of memo
    public String debugString() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    public List<OptGroup> getGroups() { return groups; }
    public Map<MultiExpression, MultiExpression> getMExprs() { return mExprs; }
    public void setRoot(OptGroup root) { this.root = root; }

    public void dump() {
       final String dumpStr = root.getExplain();
       if (root != null) {
           LOG.info("Memo:\n" + dumpStr);
       }
    }
}
