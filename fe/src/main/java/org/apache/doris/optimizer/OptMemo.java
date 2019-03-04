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
    // store all groups
    private int nextGroupId = 1;
    private Map<Integer, OptGroup> groups;
    private Map<MultiExpression, MultiExpression> mExprs;

    public OptMemo() {
    }

    // copy an expression into search space, this function will add an MultiExpression for
    // this Expression. If this Expression has children, this function will be called
    // recursively to create MultiExpression and Group for every single Expression
    // For example, Join(Scan(A), Scan(B)) will create 3 Groups and MultiExpressions for Join,
    // Scan(A) and Scan(B).
    // We return MultiExpression rather than OptGroup because we can get OptGroup from MultiExpression
    public MultiExpression copyIn(OptExpression expr) {
        return copyIn(null, expr);
    }

    // used to copy an Expression into this memo.
    // This function will create a MultiExpression and try to search in already stored expressions.
    // If we found one, we return the found one.
    // If we found none and targetGroup is specified, we add new MultiExpression to the group. This is
    // often used when applying rules, targetGroup is the group which is explored
    // If targetGroup is not specified, we should create a new group.
    public MultiExpression copyIn(OptGroup targetGroup, OptExpression expr) {
        List<OptGroup> inputs = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            inputs.add(copyIn(input).getGroup());
        }
        MultiExpression mExpr = new MultiExpression(expr.getOp(), inputs);
        // first get expr
        MultiExpression foundExpr = mExprs.get(mExpr);
        if (foundExpr != null) {
            return foundExpr;
        }
        // If targetGroup is specified, we add MultiExpression to this group
        if (targetGroup != null) {
            targetGroup.addMExpr(mExpr);
            return mExpr;
        }
        // targetGroup is null, we should create new OptGroup
        OptGroup newGroup = new OptGroup(nextGroupId++);
        newGroup.addMExpr(mExpr);
        return mExpr;
    }

}
