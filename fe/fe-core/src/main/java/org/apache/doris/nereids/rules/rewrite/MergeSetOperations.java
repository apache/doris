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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Merge nodes of the same type and same qualifier.
 * <p>
 * eg: select k1, k2 from t1 union select 1, 2 union select d1, d2 from t2;
 * <pre>
 *     union
 *    /    \
 *   union  scan3
 *   /   \
 * scan1 scan2
 * -->
 *      union
 *     /  |  \
 * scan1 scan2 scan3
 * </pre>
 * Notice: this rule ignore Except.
 * Relational Algebra: Union (R U S), Intersect Syntax: (R ∩ S), Except Syntax: (R - S)
 * TODO: Except need other Rewrite.
 * <ul> (R - S) U T = (R U T) - S </ul>
 * <ul> (R - S) U (T - U) = (R U T) - (S U U) </ul>
 * <ul> R - (S U T) = (R - S) - T </ul>
 * <ul> R - (S - T) = (R - S) U T </ul>
 * <ul> ...... and so on </ul>
 */
public class MergeSetOperations extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalSetOperation(any(), any())
                .whenNot(LogicalExcept.class::isInstance)
                .when(MergeSetOperations::canMerge)
                .then(parentSetOperation -> {
                    ImmutableList.Builder<Plan> newChildren = ImmutableList.builder();
                    ImmutableList.Builder<List<SlotReference>> newChildrenOutputs = ImmutableList.builder();
                    for (int i = 0; i < parentSetOperation.arity(); i++) {
                        Plan child = parentSetOperation.child(i);
                        if (canMerge(parentSetOperation, child)) {
                            LogicalSetOperation logicalSetOperation = (LogicalSetOperation) child;
                            newChildren.addAll(logicalSetOperation.children());
                            newChildrenOutputs.addAll(logicalSetOperation.getRegularChildrenOutputs());
                        } else {
                            newChildren.add(child);
                            newChildrenOutputs.add(parentSetOperation.getRegularChildOutput(i));
                        }
                    }
                    return parentSetOperation.withChildrenAndTheirOutputs(
                            newChildren.build(), newChildrenOutputs.build());
                }).toRule(RuleType.MERGE_SET_OPERATION);
    }

    /** canMerge */
    public static boolean canMerge(LogicalSetOperation parent) {
        Plan left = parent.child(0);
        Plan right = parent.child(1);

        return canMerge(parent, left) || canMerge(parent, right);
    }

    public static boolean canMerge(LogicalSetOperation parent, Plan child) {
        return child.getClass().equals(parent.getClass())
                && isSameQualifierOrChildQualifierIsAll(parent, (LogicalSetOperation) child);
    }

    public static boolean isSameQualifierOrChildQualifierIsAll(LogicalSetOperation parentSetOperation,
            LogicalSetOperation childSetOperation) {
        return parentSetOperation.getQualifier() == childSetOperation.getQualifier()
                || childSetOperation.getQualifier() == Qualifier.ALL;
    }
}
