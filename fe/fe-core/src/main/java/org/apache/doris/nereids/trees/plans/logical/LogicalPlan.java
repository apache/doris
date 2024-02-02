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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Abstract class for all logical plan in Nereids.
 */
public interface LogicalPlan extends Plan {

    /**
     * Map a [[LogicalPlan]] to another [[LogicalPlan]] if the passed context exists using the
     * passed function. The original plan is returned when the context does not exist.
     */
    default <C> LogicalPlan optionalMap(C ctx, BiFunction<C, LogicalPlan, LogicalPlan> f) {
        if (ctx != null) {
            return f.apply(ctx, this);
        } else {
            return this;
        }
    }

    default <C> LogicalPlan optionalMap(Optional<C> ctx, Supplier<LogicalPlan> f) {
        return ctx.map(a -> f.get()).orElse(this);
    }

    default LogicalPlan recomputeLogicalProperties() {
        return (LogicalPlan) withChildren(ImmutableList.copyOf(children()));
    }

    /**
     * Compute FunctionalDependencies for different plan
     * Note: Unless you really know what you're doing, please use the following interface.
     *   - BlockFDPropagation: clean the fd
     *   - PropagateFD: propagate the fd
     */
    FunctionalDependencies computeFuncDeps(Supplier<List<Slot>> outputSupplier);

    ImmutableSet<FdItem> computeFdItems(Supplier<List<Slot>> outputSupplier);
}
