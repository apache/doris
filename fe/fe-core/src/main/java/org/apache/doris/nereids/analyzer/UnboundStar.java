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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Star expression.
 */
public class UnboundStar extends NamedExpression implements LeafExpression, Unbound, PropagateNullable {
    private final List<String> qualifier;
    // the start and end position of the sql substring(e.g. "*", "table.*")
    private final Optional<Pair<Integer, Integer>> indexInSqlString;

    private final List<NamedExpression> exceptedSlots;

    private final List<NamedExpression> replacedAlias;

    public UnboundStar(List<String> qualifier) {
        super(ImmutableList.of());
        this.qualifier = Objects.requireNonNull(ImmutableList.copyOf(qualifier), "qualifier can not be null");
        this.indexInSqlString = Optional.empty();
        this.exceptedSlots = ImmutableList.of();
        this.replacedAlias = ImmutableList.of();
    }

    public UnboundStar(List<String> qualifier, Optional<Pair<Integer, Integer>> indexInSqlString) {
        super(ImmutableList.of());
        this.qualifier = Objects.requireNonNull(ImmutableList.copyOf(qualifier), "qualifier can not be null");
        this.indexInSqlString = indexInSqlString;
        this.exceptedSlots = ImmutableList.of();
        this.replacedAlias = ImmutableList.of();
    }

    /**
     * The star expression is used in the select list, and the exceptedSlots is the column that is not selected.
     *
     * @param qualifier for example, "table1.*"
     * @param exceptedSlots for example, * EXCEPT(a, b)
     * @param replacedAlias for example, * REPLACE(a + 1 AS a)
     */
    public UnboundStar(List<String> qualifier, List<NamedExpression> exceptedSlots,
            List<NamedExpression> replacedAlias) {
        super(ImmutableList.of());
        this.qualifier = Objects.requireNonNull(ImmutableList.copyOf(qualifier), "qualifier can not be null");
        this.indexInSqlString = Optional.empty();
        this.exceptedSlots = Objects.requireNonNull(ImmutableList.copyOf(exceptedSlots),
                "except columns can not be null");
        this.replacedAlias = Objects.requireNonNull(ImmutableList.copyOf(replacedAlias),
                "replace columns can not be null");
    }

    /**
     * The star expression is used in the select list, and the exceptedSlots is the column that is not selected.
     *
     * @param qualifier for example, "table1.*"
     * @param exceptedSlots for example, * EXCEPT(a, b)
     * @param replacedAlias for example, * REPLACE(a + 1 AS a)
     * @param indexInSqlString see {@link UnboundStar#indexInSqlString}
     */
    public UnboundStar(List<String> qualifier, List<NamedExpression> exceptedSlots, List<NamedExpression> replacedAlias,
            Optional<Pair<Integer, Integer>> indexInSqlString) {
        super(ImmutableList.of());
        this.qualifier = Objects.requireNonNull(ImmutableList.copyOf(qualifier), "qualifier can not be null");
        this.indexInSqlString = indexInSqlString;
        this.exceptedSlots = Objects.requireNonNull(ImmutableList.copyOf(exceptedSlots),
                "except columns can not be null");
        this.replacedAlias = Objects.requireNonNull(ImmutableList.copyOf(replacedAlias),
                "replace columns can not be null");
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append(Utils.qualifiedName(qualifier, "*"));
        if (!exceptedSlots.isEmpty()) {
            String exceptStr = exceptedSlots.stream().map(NamedExpression::toSql)
                    .collect(Collectors.joining(", ", " EXCEPT(", ")"));
            builder.append(exceptStr);
        }
        if (!replacedAlias.isEmpty()) {
            String replaceStr = replacedAlias.stream().map(NamedExpression::toSql)
                    .collect(Collectors.joining(", ", " REPLACE(", ")"));
            builder.append(replaceStr);
        }
        return builder.toString();
    }

    @Override
    public List<String> getQualifier() throws UnboundException {
        return qualifier;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public Optional<Pair<Integer, Integer>> getIndexInSqlString() {
        return indexInSqlString;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundStar(this, context);
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
        UnboundStar that = (UnboundStar) o;
        return qualifier.equals(that.qualifier) && exceptedSlots.equals(that.exceptedSlots) && replacedAlias.equals(
                that.replacedAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifier, exceptedSlots, replacedAlias);
    }

    public UnboundStar withIndexInSql(Pair<Integer, Integer> index) {
        return new UnboundStar(qualifier, exceptedSlots, replacedAlias, Optional.ofNullable(index));
    }

    public List<NamedExpression> getReplacedAlias() {
        return replacedAlias;
    }

    public List<NamedExpression> getExceptedSlots() {
        return exceptedSlots;
    }
}
