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
 * Function dependence items.
 */
public class FdItem {
    private ImmutableSet<SlotReference> parentExprs;

    private boolean isUnique;

    private boolean isCandidate;

    public FdItem(ImmutableSet<SlotReference> parentExprs, boolean isUnique, boolean isCandidate) {
        this.parentExprs = ImmutableSet.copyOf(parentExprs);
        this.isUnique = isUnique;
        this.isCandidate = isCandidate;
    }

    public boolean isCandidate() {
        return isCandidate;
    }

    public void setCandidate(boolean isCandidate) {
        this.isCandidate = isCandidate;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    public ImmutableSet<SlotReference> getParentExprs() {
        return parentExprs;
    }

    public boolean checkExprInChild(SlotReference slot, LogicalPlan childPlan) {
        return false;
    }
}
