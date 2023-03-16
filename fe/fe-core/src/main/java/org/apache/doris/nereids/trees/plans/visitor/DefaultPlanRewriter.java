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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for plan rewriting, delegating to child plans and rewrite current root
 * when any one of its children changed.
 */
public abstract class DefaultPlanRewriter<C> extends PlanVisitor<Plan, C> {

    @Override
    public Plan visit(Plan plan, C context) {
        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Plan newChild = child.accept(this, context);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? plan.withChildren(newChildren) : plan;
    }

    @Override
    public Plan visitPhysicalStorageLayerAggregate(PhysicalStorageLayerAggregate storageLayerAggregate, C context) {
        PhysicalOlapScan olapScan = (PhysicalOlapScan) storageLayerAggregate.getRelation().accept(this, context);
        if (olapScan != storageLayerAggregate.getRelation()) {
            return storageLayerAggregate.withPhysicalOlapScan(olapScan);
        }
        return storageLayerAggregate;
    }
}
