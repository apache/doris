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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;

/**
 * generate fragment id for nereids physical plan
 */
public class FragmentProcessor extends PlanPostProcessor {
    private int frId = 0;

    public PhysicalDistribute visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            CascadesContext ctx) {
        frId++;
        distribute.child().accept(this, ctx);
        return distribute;
    }

    public PhysicalHashJoin visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext ctx) {
        join.setMutableState(AbstractPlan.FRAGMENT_ID, frId);
        join.left().accept(this, ctx);
        join.right().accept(this, ctx);
        return join;
    }

    public PhysicalRelation visitPhysicalRelation(PhysicalRelation scan, CascadesContext ctx) {
        scan.setMutableState(AbstractPlan.FRAGMENT_ID, frId);
        return scan;
    }
}
