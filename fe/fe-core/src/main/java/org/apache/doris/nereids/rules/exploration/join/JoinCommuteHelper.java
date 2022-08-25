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

import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;

/**
 * Common function for JoinCommute
 */
public class JoinCommuteHelper {

    enum SwapType {
        BOTTOM_JOIN, ZIG_ZAG, ALL
    }

    private final boolean swapOuter;
    private final SwapType swapType;

    public JoinCommuteHelper(boolean swapOuter, SwapType swapType) {
        this.swapOuter = swapOuter;
        this.swapType = swapType;
    }

    public static boolean check(LogicalJoin join) {
        if (join.getJoinReorderContext().hasCommute() || join.getJoinReorderContext().hasExchange()) {
            return false;
        }
        return true;
    }

    public static boolean check(LogicalProject project) {
        Preconditions.checkState(project.child() instanceof LogicalJoin);
        LogicalJoin join = (LogicalJoin) project.child();
        return check(join);
    }
}
