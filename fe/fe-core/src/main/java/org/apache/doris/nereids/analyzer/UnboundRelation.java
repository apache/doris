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

import org.apache.doris.nereids.analyzer.identifier.TableIdentifier;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represent a relation plan node that has not been bound.
 */
public class UnboundRelation extends LogicalLeaf implements Relation, Unbound {
    private final List<String> nameParts;

    public UnboundRelation(List<String> nameParts) {
        this(nameParts, Optional.empty(), Optional.empty());
    }

    public UnboundRelation(List<String> nameParts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(PlanType.LOGICAL_UNBOUND_RELATION, groupExpression, logicalProperties);
        this.nameParts = nameParts;
    }

    public UnboundRelation(TableIdentifier identifier) {
        this(identifier, Optional.empty(), Optional.empty());
    }

    /**
     * Constructor for UnboundRelation.
     *
     * @param identifier relation identifier
     */
    public UnboundRelation(TableIdentifier identifier, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(PlanType.LOGICAL_UNBOUND_RELATION, groupExpression, logicalProperties);
        this.nameParts = Lists.newArrayList();
        if (identifier.getDatabaseName().isPresent()) {
            nameParts.add(identifier.getDatabaseName().get());
        }
        nameParts.add(identifier.getTableName());
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public String getTableName() {
        return nameParts.stream().map(Utils::quoteIfNeeded)
                .reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public LogicalProperties computeLogicalProperties() {
        return UnboundLogicalProperties.INSTANCE;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundRelation(nameParts, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new UnboundRelation(nameParts, Optional.empty(), logicalProperties);
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundRelation",
                "nameParts", StringUtils.join(nameParts, ".")
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundRelation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " don't support getExpression()");
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
        UnboundRelation that = (UnboundRelation) o;
        return Objects.equals(nameParts, that.nameParts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameParts);
    }
}
