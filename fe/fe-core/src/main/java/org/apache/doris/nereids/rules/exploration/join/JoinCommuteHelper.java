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

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.List;

/**
 * Join Commute Helper
 */
class JoinCommuteHelper {
    enum SwapType {
        LEFT_DEEP, ZIG_ZAG, BUSHY
    }

    public static boolean check(SwapType swapType, LogicalJoin<GroupPlan, GroupPlan> join) {
        if (swapType == SwapType.LEFT_DEEP && isNotBottomJoin(join)) {
            return false;
        }

        return !join.getJoinReorderContext().hasCommute() && !join.getJoinReorderContext().hasExchange();
    }

    public static boolean isNotBottomJoin(LogicalJoin<GroupPlan, GroupPlan> join) {
        // TODO: tmp way to judge bottomJoin
        return containJoin(join.left()) || containJoin(join.right());
    }

    private static boolean containJoin(GroupPlan groupPlan) {
        // TODO: tmp way to judge containJoin
        List<Slot> output = groupPlan.getOutput();
        return !output.stream().map(Slot::getQualifier).allMatch(output.get(0).getQualifier()::equals);
    }
}
