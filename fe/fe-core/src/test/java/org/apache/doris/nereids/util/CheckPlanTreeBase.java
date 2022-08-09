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

import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;

import apple.laf.JRSUIUtils.Tree;
import org.checkerframework.checker.units.qual.C;

import java.util.function.Function;
import java.util.function.Predicate;

public class CheckPlanTreeBase<NODE_TYPE extends TreeNode<NODE_TYPE>> {

    private final Predicate<TreeNode<NODE_TYPE>> beforeSearch;
    private final Function<TreeNode<NODE_TYPE>, Boolean> afterSearch;

    public CheckPlanTreeBase(Predicate<TreeNode<NODE_TYPE>> beforeSearch,
            Function<TreeNode<NODE_TYPE>, Boolean> afterSearch) {
        this.beforeSearch = beforeSearch;
        this.afterSearch = afterSearch;
    }

    public boolean execute(TreeNode<NODE_TYPE> plan) {
        if (beforeSearch != null && !beforeSearch.test(plan)) {
            return false;
        }
        if (plan.children().stream().anyMatch(p -> !execute(p))) {
            return false;
        }
        return afterSearch.apply(plan);
    }
}
