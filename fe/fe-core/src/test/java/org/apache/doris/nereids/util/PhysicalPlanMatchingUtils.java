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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.stream.IntStream;

public class PhysicalPlanMatchingUtils {

    public static <TYPE extends Plan> boolean topDownFindMatching(Plan plan, Pattern<TYPE> pattern) {
        if (!pattern.matchRoot(plan)) {
            return false;
        }
        if (!pattern.getPredicates().stream().allMatch(pred -> pred.test((TYPE) plan))) {
            return false;
        }
        return IntStream.range(0, plan.children().size()).allMatch(i -> topDownFindMatching(plan.child(i), pattern.child(i)));
    }
}
