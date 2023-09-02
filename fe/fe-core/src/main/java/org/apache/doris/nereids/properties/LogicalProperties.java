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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Logical properties used for analysis and optimize in Nereids.
 */
public class LogicalProperties {
    protected final Supplier<List<Slot>> outputSupplier;
    protected final Supplier<List<Id>> outputExprIdsSupplier;
    protected final Supplier<Set<Slot>> outputSetSupplier;
    protected final Supplier<Map<Slot, Slot>> outputMapSupplier;
    protected final Supplier<Set<ExprId>> outputExprIdSetSupplier;
    private Integer hashCode = null;

    public LogicalProperties(Supplier<List<Slot>> outputSupplier) {
        this(outputSupplier, ImmutableList::of);
    }

    /**
     * constructor of LogicalProperties.
     *
     * @param outputSupplier provide the output. Supplier can lazy compute output without
     *                       throw exception for which children have UnboundRelation
     */
    public LogicalProperties(Supplier<List<Slot>> outputSupplier, Supplier<List<Slot>> nonUserVisibleOutputSupplier) {
        this.outputSupplier = Suppliers.memoize(
                Objects.requireNonNull(outputSupplier, "outputSupplier can not be null")
        );
        this.outputExprIdsSupplier = Suppliers.memoize(
                () -> this.outputSupplier.get().stream().map(NamedExpression::getExprId).map(Id.class::cast)
                        .collect(ImmutableList.toImmutableList())
        );
        this.outputSetSupplier = Suppliers.memoize(
                () -> ImmutableSet.copyOf(this.outputSupplier.get())
        );
        this.outputMapSupplier = Suppliers.memoize(
                () -> this.outputSetSupplier.get().stream().collect(ImmutableMap.toImmutableMap(s -> s, s -> s))
        );
        this.outputExprIdSetSupplier = Suppliers.memoize(
                () -> this.outputSupplier.get().stream()
                        .map(NamedExpression::getExprId)
                        .collect(ImmutableSet.toImmutableSet())
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

    public List<Id> getOutputExprIds() {
        return outputExprIdsSupplier.get();
    }

    @Override
    public String toString() {
        return "LogicalProperties{"
                + "\noutputSupplier=" + outputSupplier.get()
                + "\noutputExprIdsSupplier=" + outputExprIdsSupplier.get()
                + "\noutputSetSupplier=" + outputSetSupplier.get()
                + "\noutputMapSupplier=" + outputMapSupplier.get()
                + "\noutputExprIdSetSupplier=" + outputExprIdSetSupplier.get()
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
