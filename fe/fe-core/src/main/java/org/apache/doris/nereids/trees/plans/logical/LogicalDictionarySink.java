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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical sink node for dictionary operations.
 */
public class LogicalDictionarySink<CHILD_TYPE extends Plan> extends LogicalTableSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {

    private final Database database;
    private final Dictionary dictionary;
    private final List<Column> cols; // sink table columns
    private final boolean allowAdaptiveLoad;

    public LogicalDictionarySink(Database database, Dictionary dictionary, boolean allowAdaptiveLoad, List<Column> cols,
            List<NamedExpression> outputExprs, CHILD_TYPE child) {
        this(database, dictionary, allowAdaptiveLoad, cols, outputExprs, Optional.empty(), Optional.empty(), child);
    }

    public LogicalDictionarySink(Database database, Dictionary dictionary, boolean allowAdaptiveLoad, List<Column> cols,
            List<NamedExpression> outputExprs, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_DICTIONARY_SINK, outputExprs, groupExpression, logicalProperties, cols, child);
        this.database = Objects.requireNonNull(database, "database != null in LogicalDictionarySink");
        this.dictionary = Objects.requireNonNull(dictionary, "dictionary != null in LogicalDictionarySink");
        this.cols = ImmutableList.copyOf(cols);
        this.allowAdaptiveLoad = allowAdaptiveLoad;
    }

    public Database getDatabase() {
        return database;
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    public List<Column> getCols() {
        return cols;
    }

    public boolean allowAdaptiveLoad() {
        return allowAdaptiveLoad;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalDictionarySink only accepts one child");
        return new LogicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, Optional.empty(),
                Optional.empty(), children.get(0));
    }

    // This function will really set outputExprs in base class
    public Plan withChildAndUpdateOutput(Plan child) {
        List<NamedExpression> output = child.getOutput().stream().map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new LogicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, output, Optional.empty(),
                Optional.empty(), child);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDictionarySink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                logicalProperties, children.get(0));
    }

    @Override
    public TableIf getTargetTable() {
        return dictionary;
    }

    @Override
    public LogicalSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, Optional.empty(),
                Optional.empty(), child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalDictionarySink<?> that = (LogicalDictionarySink<?>) o;
        return Objects.equals(database, that.database) && Objects.equals(dictionary, that.dictionary)
                && Objects.equals(cols, that.cols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, dictionary, cols);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalDictionarySink[" + id.asInt() + "]", "outputExprs", outputExprs, "database",
                database.getFullName(), "dictionary", dictionary.getName(), "cols", cols);
    }
}
