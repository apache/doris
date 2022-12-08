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
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical properties used for analysis and optimize in Nereids.
 */
public class LogicalProperties {
    protected final Supplier<List<Slot>> outputSupplier;
    protected final Supplier<List<Id>> outputExprIdsSupplier;
    protected final Supplier<Set<Slot>> outputSetSupplier;
    protected final Supplier<Set<ExprId>> outputExprIdSetSupplier;
    private Integer hashCode = null;

    /**
     * constructor of LogicalProperties.
     *
     * @param outputSupplier provide the output. Supplier can lazy compute output without
     *                       throw exception for which children have UnboundRelation
     */
    public LogicalProperties(Supplier<List<Slot>> outputSupplier) {
        this.outputSupplier = Suppliers.memoize(
                Objects.requireNonNull(outputSupplier, "outputSupplier can not be null")
        );
        this.outputExprIdsSupplier = Suppliers.memoize(
                () -> this.outputSupplier.get().stream().map(NamedExpression::getExprId).map(Id.class::cast)
                        .collect(Collectors.toList())
        );
        this.outputSetSupplier = Suppliers.memoize(
                () -> Sets.newHashSet(this.outputSupplier.get())
        );
        this.outputExprIdSetSupplier = Suppliers.memoize(
                () -> this.outputSupplier.get().stream().map(NamedExpression::getExprId)
                        .collect(Collectors.toCollection(HashSet::new))
        );
    }

    public List<Slot> getOutput() {
        return outputSupplier.get();
    }

    public Set<Slot> getOutputSet() {
        return outputSetSupplier.get();
    }

    public Set<ExprId> getOutputExprIdSet() {
        return outputExprIdSetSupplier.get();
    }

    public List<Id> getOutputExprIds() {
        return outputExprIdsSupplier.get();
    }

    public LogicalProperties withOutput(List<Slot> output) {
        return new LogicalProperties(Suppliers.ofInstance(output));
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
        return Objects.equals(outputExprIdSetSupplier.get(), that.outputExprIdSetSupplier.get());
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = Objects.hash(outputExprIdSetSupplier.get());
        }
        return hashCode;
    }
}
