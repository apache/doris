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

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;

import com.google.common.collect.Lists;

/**
 * Spec of data distribution.
 * GPORCA has more type in CDistributionSpec.
 */
public abstract class DistributionSpec {
    /**
     * Self satisfies other DistributionSpec.
     * Example:
     * `DistributionSpecGather` satisfies `DistributionSpecAny`
     */
    public abstract boolean satisfy(DistributionSpec other);

    /**
     * Add physical operator of enforcer.
     */
    public GroupExpression addEnforcer(Group child) {
        // TODO:maybe we need to new a LogicalProperties or just do not set logical properties for this node.
        // If we don't set LogicalProperties explicitly, node will compute a applicable LogicalProperties for itself.
        PhysicalDistribute<GroupPlan> distribution = new PhysicalDistribute<>(
                this,
                new GroupPlan(child));
        return new GroupExpression(distribution, Lists.newArrayList(child));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    public String shapeInfo() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
