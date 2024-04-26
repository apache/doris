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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableSet;

/**
 * Propagate fd, keep children's fd
 */
public interface PropagateFuncDeps extends LogicalPlan {
    @Override
    default FunctionalDependencies computeFuncDeps() {
        if (children().size() == 1) {
            // Note when changing function dependencies, we always clone it.
            // So it's safe to return a reference
            return child(0).getLogicalProperties().getFunctionalDependencies();
        }
        FunctionalDependencies.Builder builder = new FunctionalDependencies.Builder();
        children().stream()
                .map(p -> p.getLogicalProperties().getFunctionalDependencies())
                .forEach(builder::addFunctionalDependencies);
        ImmutableSet<FdItem> fdItems = computeFdItems();
        builder.addFdItems(fdItems);
        return builder.build();
    }

    @Override
    default ImmutableSet<FdItem> computeFdItems() {
        if (children().size() == 1) {
            // Note when changing function dependencies, we always clone it.
            // So it's safe to return a reference
            return child(0).getLogicalProperties().getFunctionalDependencies().getFdItems();
        }
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();
        children().stream()
                .map(p -> p.getLogicalProperties().getFunctionalDependencies().getFdItems())
                .forEach(builder::addAll);
        return builder.build();
    }

    @Override
    default void computeUnique(FunctionalDependencies.Builder fdBuilder) {
        fdBuilder.addUniqueSlot(child(0).getLogicalProperties().getFunctionalDependencies());
    }

    @Override
    default void computeUniform(FunctionalDependencies.Builder fdBuilder) {
        fdBuilder.addUniformSlot(child(0).getLogicalProperties().getFunctionalDependencies());
    }

    @Override
    default void computeEqualSet(FunctionalDependencies.Builder fdBuilder) {
        fdBuilder.addEqualSet(child(0).getLogicalProperties().getFunctionalDependencies());
    }
}
