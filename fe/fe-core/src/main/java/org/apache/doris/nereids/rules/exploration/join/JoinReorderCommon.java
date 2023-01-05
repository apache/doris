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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;
import java.util.Set;

/**
 * Common
 */
class JoinReorderCommon {
    /**
     * check project inside Join.
     */
    static boolean checkProject(LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project) {
        Set<Slot> left = project.child().left().getOutputSet();
        Set<Slot> right = project.child().right().getOutputSet();
        List<NamedExpression> exprs = project.getProjects();
        // must be slot or Alias(slot)
        if (!exprs.stream().allMatch(expr -> {
            if (expr instanceof Slot) {
                return true;
            }
            if (expr instanceof Alias) {
                if (((Alias) expr).child() instanceof Slot) {
                    return true;
                }
            }
            return false;
        })) {
            return false;
        }

        // check project exist hyper edge
        return exprs.stream().allMatch(expr -> {
            Set<Slot> slots = expr.collect(SlotReference.class::isInstance);
            return !(ExpressionUtils.isIntersecting(left, slots) && ExpressionUtils.isIntersecting(right, slots));
        });
    }
}
