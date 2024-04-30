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
 * Block fd propagation, it always returns an empty fd
 */
public interface BlockFuncDepsPropagation extends LogicalPlan {
    @Override
    default FunctionalDependencies computeFuncDeps() {
        return FunctionalDependencies.EMPTY_FUNC_DEPS;
    }

    @Override
    default ImmutableSet<FdItem> computeFdItems() {
        return ImmutableSet.of();
    }

    @Override
    default void computeUnique(FunctionalDependencies.Builder fdBuilder) {
        // don't generate
    }

    @Override
    default void computeUniform(FunctionalDependencies.Builder fdBuilder) {
        // don't generate
    }

    @Override
    default void computeEqualSet(FunctionalDependencies.Builder fdBuilder) {
        // don't generate
    }
}
