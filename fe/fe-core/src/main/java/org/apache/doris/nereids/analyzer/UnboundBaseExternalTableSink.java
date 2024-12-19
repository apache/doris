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
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represent an external table sink plan node that has not been bound.
 */
public abstract class UnboundBaseExternalTableSink<CHILD_TYPE extends Plan> extends UnboundLogicalSink<CHILD_TYPE>
        implements Unbound, Sink, BlockFuncDepsPropagation {
    List<String> hints;
    List<String> partitions;

    /**
     * constructor
     */
    public UnboundBaseExternalTableSink(List<String> nameParts,
                                        PlanType type,
                                        List<NamedExpression> outputExprs,
                                        Optional<GroupExpression> groupExpression,
                                        Optional<LogicalProperties> logicalProperties,
                                        List<String> colNames,
                                        DMLCommandType dmlCommandType,
                                        CHILD_TYPE child,
                                        List<String> hints,
                                        List<String> partitions) {
        super(nameParts, type, outputExprs, groupExpression,
                logicalProperties, colNames, dmlCommandType, child);
        this.hints = Utils.copyRequiredList(hints);
        this.partitions = Utils.copyRequiredList(partitions);
    }

    public List<String> getColNames() {
        return colNames;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public List<String> getHints() {
        return hints;
    }

    @Override
    public UnboundBaseExternalTableSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        throw new UnboundException("could not call withOutputExprs on " + this.getClass().getSimpleName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnboundBaseExternalTableSink<?> that = (UnboundBaseExternalTableSink<?>) o;
        return Objects.equals(nameParts, that.nameParts)
                && Objects.equals(colNames, that.colNames)
                && Objects.equals(hints, that.hints)
                && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameParts, colNames, hints, partitions);
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        return UnboundLogicalProperties.INSTANCE;
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }
}
