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

import org.apache.doris.common.Id;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.LazyCompute;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Logical properties used for analysis and optimize in Nereids.
 */
public class LogicalProperties {
    protected final Supplier<List<Slot>> outputSupplier;
    protected final Supplier<List<Id<?>>> outputExprIdsSupplier;
    protected final Supplier<Set<Slot>> outputSetSupplier;
    protected final Supplier<Map<Slot, Slot>> outputMapSupplier;
    protected final Supplier<Set<ExprId>> outputExprIdSetSupplier;
    protected final Supplier<List<Slot>> asteriskOutputSupplier;
    protected final Supplier<DataTrait> dataTraitSupplier;
    private Integer hashCode = null;

    /**
     * constructor when output same as asterisk's output.
     */
    public LogicalProperties(Supplier<List<Slot>> outputSupplier, Supplier<DataTrait> dataTraitSupplier) {
        // the second parameters should be null to reuse memorized output supplier
        this(outputSupplier, null, dataTraitSupplier);
    }

    /**
     * constructor of LogicalProperties.
     *
     * @param outputSupplier provide the output. Supplier can lazy compute output without
     *                       throw exception for which children have UnboundRelation
     * @param asteriskOutputSupplier provide the output when do select *.
     * @param dataTraitSupplier provide the data trait.
     */
    public LogicalProperties(
            Supplier<List<Slot>> outputSupplier,
            Supplier<List<Slot>> asteriskOutputSupplier,
            Supplier<DataTrait> dataTraitSupplier) {
        this.outputSupplier = LazyCompute.of(
                Objects.requireNonNull(outputSupplier, "outputSupplier can not be null")
        );
        this.outputExprIdsSupplier = LazyCompute.of(() -> {
            List<Slot> output = this.outputSupplier.get();
            ImmutableList.Builder<Id<?>> exprIdSet
                    = ImmutableList.builderWithExpectedSize(output.size());
            for (Slot slot : output) {
                exprIdSet.add(slot.getExprId());
            }
            return exprIdSet.build();
        });
        this.outputSetSupplier = LazyCompute.of(() -> {
            List<Slot> output = this.outputSupplier.get();
            ImmutableSet.Builder<Slot> slots = ImmutableSet.builderWithExpectedSize(output.size());
            for (Slot slot : output) {
                slots.add(slot);
            }
            return slots.build();
        });
        this.outputMapSupplier = LazyCompute.of(() -> {
            Set<Slot> slots = this.outputSetSupplier.get();
            ImmutableMap.Builder<Slot, Slot> map = ImmutableMap.builderWithExpectedSize(slots.size());
            for (Slot slot : slots) {
                map.put(slot, slot);
            }
            return map.build();
        });
        this.outputExprIdSetSupplier = LazyCompute.of(() -> {
            List<Slot> output = this.outputSupplier.get();
            ImmutableSet.Builder<ExprId> exprIdSet
                    = ImmutableSet.builderWithExpectedSize(output.size());
            for (Slot slot : output) {
                exprIdSet.add(slot.getExprId());
            }
            return exprIdSet.build();
        });
        this.asteriskOutputSupplier = asteriskOutputSupplier == null ? this.outputSupplier : LazyCompute.of(
                Objects.requireNonNull(asteriskOutputSupplier, "asteriskOutputSupplier can not be null")
        );
        this.dataTraitSupplier = LazyCompute.of(
                Objects.requireNonNull(dataTraitSupplier, "Data Trait can not be null")
        );
    }

    public List<Slot> getOutput() {
        return outputSupplier.get();
    }

    public Set<Slot> getOutputSet() {
        return outputSetSupplier.get();
    }

    public Map<Slot, Slot> getOutputMap() {
        return outputMapSupplier.get();
    }

    public Set<ExprId> getOutputExprIdSet() {
        return outputExprIdSetSupplier.get();
    }

    public List<Id<?>> getOutputExprIds() {
        return outputExprIdsSupplier.get();
    }

    public List<Slot> getAsteriskOutput() {
        return asteriskOutputSupplier.get();
    }

    public DataTrait getTrait() {
        return dataTraitSupplier.get();
    }

    @Override
    public String toString() {
        return "LogicalProperties{"
                + "\noutputSupplier=" + outputSupplier.get()
                + "\noutputExprIdsSupplier=" + outputExprIdsSupplier.get()
                + "\noutputSetSupplier=" + outputSetSupplier.get()
                + "\noutputMapSupplier=" + outputMapSupplier.get()
                + "\noutputExprIdSetSupplier=" + outputExprIdSetSupplier.get()
                + "\nasteriskOutputSupplier=" + asteriskOutputSupplier.get()
                + "\nhashCode=" + hashCode
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalProperties that = (LogicalProperties) o;
        Set<Slot> thisOutSet = this.outputSetSupplier.get();
        Set<Slot> thatOutSet = that.outputSetSupplier.get();
        if (!Objects.equals(thisOutSet, thatOutSet)) {
            return false;
        }
        for (Slot thisOutSlot : thisOutSet) {
            Slot thatOutSlot = that.getOutputMap().get(thisOutSlot);
            if (thisOutSlot.nullable() != thatOutSlot.nullable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = Objects.hash(outputExprIdSetSupplier.get());
        }
        return hashCode;
    }
}
