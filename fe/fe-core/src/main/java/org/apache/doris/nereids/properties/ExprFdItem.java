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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableSet;

/**
 * Expression level function dependence items.
 */
public class ExprFdItem extends FdItem {
    private ImmutableSet<SlotReference> childExprs;

    public ExprFdItem(ImmutableSet<SlotReference> parentExprs, boolean isUnique,
            ImmutableSet<SlotReference> childExprs) {
        super(parentExprs, isUnique, false);
        this.childExprs = ImmutableSet.copyOf(childExprs);
    }

    @Override
    public boolean checkExprInChild(SlotReference slot, LogicalPlan childPlan) {
        return childExprs.contains(slot);
    }
}
