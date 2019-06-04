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

package org.apache.doris.optimizer.utils;

import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;

public class PlansStatus {
    // This will take long time for large dag.
    public static long getLogicalPlanCount(OptGroup root) {
        return getPlanCount(root, true);
    }

    public static long getPhysicalPlanCount(OptGroup root) {
        return getPlanCount(root, false);
    }

    private static long getPlanCount(OptGroup root, boolean isLogical) {
        long planCountTotal = 0;
        for (MultiExpression mExpr : root.getMultiExpressions()) {
            if ((mExpr.getOp().isLogical() && isLogical)
                  || (mExpr.getOp().isPhysical() && !isLogical)) {
                long planCount = 1;
                for (OptGroup child : mExpr.getInputs()) {
                    planCount *= getLogicalPlanCount(child);
                }
                planCountTotal += planCount;
            }
        }
        return planCountTotal;
    }
}
