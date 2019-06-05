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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// Used to extract matched expression from MultiExpression
public class OptBinding {
    private static final Logger LOG = LogManager.getLogger(OptBinding.class);

    // Extract a expression from MultiExpression which match the given pattern
    // pattern:  Bound expression should be matched
    // mExpr:    Search this for binding. Because MultiExpression's inputs are groups,
    //           several Expressions matched the pattern should be bound from it
    // lastExpr: If this is not null, its the last expression which has been bound,
    //           this function would bind next expression. If this is null, first matched
    //           expression would be bound
    public static OptExpression bind(OptExpression pattern, MultiExpression mExpr, OptExpression lastExpr) {
        if (!pattern.matchMultiExpression(mExpr)) {
            return null;
        }
        // if pattern is PatternLeaf or is patter's arity is zero, return
        if (pattern.getOp().isPatternAndLeaf()) {
            if (lastExpr != null) {
                // we have already binding this expression
                return null;
            }
            return OptExpression.createBindingLeafExpression(mExpr);
        }
        // If MultiExpression's arity isn't equal with pattern's, return null
        int arity = mExpr.arity();
        if (arity <  pattern.arity()) {
            return null;
        }
        // we should binding children
        List<OptExpression> boundInputs = Lists.newArrayList();
        if (lastExpr == null) {
            for (int i = 0; i < arity; ++i) {
                OptExpression inputPattern = extractPattern(pattern, i);
                OptGroup inputGroup = mExpr.getInput(i);

                OptExpression boundInput = bind(inputPattern, inputGroup, null);
                if (boundInput == null) {
                    return null;
                }
                boundInputs.add(boundInput);
            }
        } else {
            boolean hasBound = false;
            for (int i = 0; i < arity; ++i) {
                LOG.debug("binding i={}, hasBound={}", i, hasBound);
                OptExpression inputLastExpr = lastExpr.getInput(i);
                if (hasBound) {
                    boundInputs.add(inputLastExpr);
                } else {
                    OptExpression inputPattern = extractPattern(pattern, i);
                    OptGroup inputGroup = mExpr.getInput(i);
                    OptExpression boundInput = bind(inputPattern, inputGroup, inputLastExpr);
                    LOG.debug("going to bind, patter={}, group={}, lastExpr={}, boundInput={}",
                            inputPattern.debugString(), inputGroup.debugString(), inputLastExpr.debugString(),
                            (boundInput == null) ? "null" : boundInput.debugString());
                    if (boundInput != null) {
                        hasBound = true;
                    } else {
                        // fallback to first binding
                        boundInput = bind(inputPattern, inputGroup, null);
                        Preconditions.checkArgument(boundInput != null,
                                "bind return null, inputPatter=" + inputPattern.debugString()
                                        + ", inputGroup=" + inputGroup.debugString());
                    }
                    boundInputs.add(boundInput);
                }
            }
            // If hasBound is false means all inputs are fallback to first binding, which has been already returned
            // to client. So in this case, we return null to tell client we have bound all matched OptExpression.
            if (!hasBound) {
                return null;
            }
        }

        return OptExpression.createBindingInternalExpression(mExpr, boundInputs);
    }

    private static OptExpression bind(OptExpression pattern, OptGroup group, OptExpression lastExpr) {
        MultiExpression mExpr;
        if (lastExpr != null) {
            mExpr = lastExpr.getMExpr();
        } else if (group.isItemGroup()) {
            mExpr = group.getFirstMultiExpression();
        }  else {
            mExpr = group.getFirstLogicalMultiExpression();
        }

        // Fast path. When pattern is PatternLeaf and lastExpr is not null it means that
        // this group has already been matched, and always return null for other MultiExpression
        // in group. So we return here to avoid needless operation
        if (pattern.getOp().isPatternAndLeaf() && lastExpr != null) {
            return null;
        }
//
//        if (pattern.getOp().isPatternAndTree() || pattern.getOp().isPatternAndMultiTree()) {
//
//        }

        do {
            OptExpression expr = bind(pattern, mExpr, lastExpr);
            if (expr != null) {
                return expr;
            }
            lastExpr = null;
            if (group.isItemGroup()) {
                group.getItemExpression();
                mExpr = mExpr.next();
            } else {
                mExpr = group.nextLogicalExpr(mExpr);
            }

        } while (mExpr != null);
        return null;
    }

    private static OptExpression extractPattern(OptExpression parentPattern, int index) {
        if (parentPattern.getOp().isPatternAndTree()
                || parentPattern.getOp().isPatternAndMultiTree()) {
            return parentPattern;
        }
        return parentPattern.getInput(index);
    }
}
