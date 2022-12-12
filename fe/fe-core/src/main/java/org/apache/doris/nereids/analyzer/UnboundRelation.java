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

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.analyzer.identifier.TableIdentifier;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
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
public class UnboundRelation extends LogicalRelation implements Unbound {
    private final List<String> nameParts;

    public UnboundRelation(RelationId id, List<String> nameParts) {
        this(id, nameParts, Optional.empty(), Optional.empty());
    }

    public UnboundRelation(RelationId id, List<String> nameParts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_UNBOUND_RELATION, groupExpression, logicalProperties);
        this.nameParts = nameParts;
    }

    public UnboundRelation(RelationId id, TableIdentifier identifier) {
        this(id, identifier, Optional.empty(), Optional.empty());
    }

    /**
     * Constructor for UnboundRelation.
     *
     * @param identifier relation identifier
     */
    public UnboundRelation(RelationId id, TableIdentifier identifier, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_UNBOUND_RELATION, groupExpression, logicalProperties);
        this.nameParts = Lists.newArrayList();
        if (identifier.getDatabaseName().isPresent()) {
            nameParts.add(identifier.getDatabaseName().get());
        }
        nameParts.add(identifier.getTableName());
    }

    @Override
    public Table getTable() {
        throw new UnsupportedOperationException("unbound relation cannot get table");
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
        return new UnboundRelation(id, nameParts, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new UnboundRelation(id, nameParts, Optional.empty(), logicalProperties);
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundRelation",
                "id", id,
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

    public RelationId getId() {
        return id;
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
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
