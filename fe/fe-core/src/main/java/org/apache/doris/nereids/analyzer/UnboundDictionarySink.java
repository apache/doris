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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represent a dictionary sink plan node that has not been bound.
 */
public class UnboundDictionarySink<CHILD_TYPE extends Plan> extends UnboundLogicalSink<CHILD_TYPE>
        implements Unbound, Sink, BlockFuncDepsPropagation {

    private final Dictionary dictionary;
    private final boolean allowAdaptiveLoad;

    /**
     * create unbound sink for dictionary sink
     */
    public UnboundDictionarySink(Dictionary dictionary, CHILD_TYPE child, boolean adaptiveLoad) {
        // all the empty arguments is like UnboundTableSink
        super(ImmutableList.copyOf(dictionary.getNameWithFullQualifiers().split("\\.")), // nameParts
                PlanType.LOGICAL_UNBOUND_DICTIONARY_SINK, // type
                ImmutableList.of(), // outputExprs
                Optional.empty(), // groupExpression
                Optional.empty(), // logicalProperties
                dictionary.getColumnNames(), // colNames from dictionary
                DMLCommandType.INSERT, // dmlCommandType
                child);
        this.dictionary = dictionary;
        this.allowAdaptiveLoad = adaptiveLoad;
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    public boolean allowAdaptiveLoad() {
        return allowAdaptiveLoad;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "UnboundDictionarySink only accepts one child");
        return new UnboundDictionarySink<>(dictionary, children.get(0), allowAdaptiveLoad);
    }

    @Override
    public UnboundDictionarySink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        throw new UnboundException("could not call withOutputExprs on UnboundDictionarySink");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundDictionarySink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundDictionarySink<>(dictionary, child(), allowAdaptiveLoad);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new UnboundDictionarySink<>(dictionary, children.get(0), allowAdaptiveLoad);
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        return UnboundLogicalProperties.INSTANCE;
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnboundDictionarySink<?> that = (UnboundDictionarySink<?>) o;
        return Objects.equals(dictionary, that.dictionary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dictionary);
    }

    @Override
    public String toString() {
        return "UnboundDictionarySink{dictionary=" + dictionary + "}";
    }
}
