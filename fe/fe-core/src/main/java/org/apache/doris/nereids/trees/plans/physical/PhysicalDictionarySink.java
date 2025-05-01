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
// PhysicalDictionarySink.java

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * PhysicalDictionarySink
 */
public class PhysicalDictionarySink<CHILD_TYPE extends Plan> extends PhysicalTableSink<CHILD_TYPE> implements Sink {
    private final Database database;
    private final Dictionary dictionary;
    private final List<Column> cols;
    private final boolean allowAdaptiveLoad;

    /**
     * constructor
     */
    public PhysicalDictionarySink(Database database, Dictionary dictionary, boolean allowAdaptiveLoad,
            List<Column> cols, List<NamedExpression> outputExprs, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_DICTIONARY_SINK, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.ALL_SINGLETON, statistics, child);
        this.database = Objects.requireNonNull(database, "database cannot be null");
        this.dictionary = Objects.requireNonNull(dictionary, "dictionary cannot be null");
        this.cols = ImmutableList.copyOf(cols);
        this.allowAdaptiveLoad = allowAdaptiveLoad;
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        return PhysicalProperties.ALL_SINGLETON;
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
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public TableIf getTargetTable() {
        return dictionary;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDictionarySink(this, context);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                getLogicalProperties(), statistics, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                getLogicalProperties(), statistics, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                logicalProperties.get(), statistics, children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                getLogicalProperties(), statistics, child());
    }

    @Override
    public PhysicalDictionarySink<Plan> resetLogicalProperties() {
        return new PhysicalDictionarySink<>(database, dictionary, allowAdaptiveLoad, cols, outputExprs, groupExpression,
                getLogicalProperties(), null, child());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, dictionary, allowAdaptiveLoad, cols);
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
        PhysicalDictionarySink<?> that = (PhysicalDictionarySink<?>) o;
        return Objects.equals(database, that.database) && Objects.equals(dictionary, that.dictionary)
                && Objects.equals(cols, that.cols);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalDictionarySink", "qualified",
                Utils.qualifiedName(ImmutableList.of(), dictionary.getName()), "output", getOutput(), "stats",
                statistics);
    }
}
