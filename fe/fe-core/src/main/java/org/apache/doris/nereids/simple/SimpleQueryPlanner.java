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

package org.apache.doris.nereids.simple;

import org.apache.doris.nereids.trees.SuperClassId;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;

import com.google.common.collect.ImmutableSet;

import java.util.BitSet;

/** SimpleQueryPlanner */
public class SimpleQueryPlanner {
    private static final BitSet SIMPLE_PLAN_TYPES = buildSimplePlanTypes();

    /** isSimplePlan */
    public static boolean isSimplePlan(LogicalPlan plan) {
        BitSet existsTypes = (BitSet) plan.getAllChildrenTypes().clone();
        for (int i = existsTypes.nextSetBit(0); i >= 0; i = existsTypes.nextSetBit(i + 1)) {
            if (!SIMPLE_PLAN_TYPES.get(i)) {
                return false;
            }
        }
        return true;
    }

    private static BitSet buildSimplePlanTypes() {
        BitSet simplePlanTypes = new BitSet();
        ImmutableSet<Class<? extends AbstractLogicalPlan>> classes = ImmutableSet.of(
                LogicalCatalogRelation.class,
                LogicalProject.class,
                LogicalFilter.class,
                LogicalSubQueryAlias.class,
                LogicalView.class,
                LogicalSort.class,
                LogicalLimit.class,
                LogicalAggregate.class,
                LogicalRepeat.class
        );
        for (Class<? extends AbstractLogicalPlan> clazz : classes) {
            simplePlanTypes.set(SuperClassId.getClassId(clazz));
        }
        return simplePlanTypes;
    }
}
