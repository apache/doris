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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;
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
     * Compute DataTrait for different plan
     * Note: Unless you really know what you're doing, please use the following interface.
     *   - BlockFDPropagation: clean the fd
     *   - PropagateFD: propagate the fd
     */
    default DataTrait computeDataTrait() {
        DataTrait.Builder fdBuilder = new DataTrait.Builder();
        computeUniform(fdBuilder);
        computeUnique(fdBuilder);
        computeEqualSet(fdBuilder);
        computeFd(fdBuilder);

        for (Slot slot : getOutput()) {
            Set<Slot> o = ImmutableSet.of(slot);
            // all slots dependent unique slot
            for (Set<Slot> uniqueSlot : fdBuilder.getAllUniqueAndNotNull()) {
                fdBuilder.addDeps(uniqueSlot, o);
            }
            // uniform slot dependents all slots
            for (Set<Slot> uniformSlot : fdBuilder.getAllUniformAndNotNull()) {
                fdBuilder.addDeps(o, uniformSlot);
            }
        }
        for (Set<Slot> equalSet : fdBuilder.calEqualSetList()) {
            Set<Slot> validEqualSet = Sets.intersection(getOutputSet(), equalSet);
            fdBuilder.addDepsByEqualSet(validEqualSet);
            fdBuilder.addUniformByEqualSet(validEqualSet);
            fdBuilder.addUniqueByEqualSet(validEqualSet);
        }
        Set<Slot> output = this.getOutputSet();
        for (Plan child : children()) {
            if (!output.containsAll(child.getOutputSet())) {
                fdBuilder.pruneSlots(output);
                break;
            }
        }
        return fdBuilder.build();
    }

    void computeUnique(DataTrait.Builder builder);

    void computeUniform(DataTrait.Builder builder);

    void computeEqualSet(DataTrait.Builder builder);

    void computeFd(DataTrait.Builder builder);

    default StmtType stmtType() {
        return StmtType.OTHER;
    }
}
