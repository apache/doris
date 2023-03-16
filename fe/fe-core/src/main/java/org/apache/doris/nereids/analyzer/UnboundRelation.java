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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.UnboundLogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represent a relation plan node that has not been bound.
 */
public class UnboundRelation extends LogicalRelation implements Unbound {

    private final List<String> nameParts;
    private final List<String> partNames;
    private final boolean isTempPart;
    private final List<String> hints;

    public UnboundRelation(ObjectId id, List<String> nameParts) {
        this(id, nameParts, Optional.empty(), Optional.empty(), ImmutableList.of(), false, ImmutableList.of());
    }

    public UnboundRelation(ObjectId id, List<String> nameParts, List<String> partNames, boolean isTempPart) {
        this(id, nameParts, Optional.empty(), Optional.empty(), partNames, isTempPart, ImmutableList.of());
    }

    public UnboundRelation(ObjectId id, List<String> nameParts, List<String> partNames, boolean isTempPart,
            List<String> hints) {
        this(id, nameParts, Optional.empty(), Optional.empty(), partNames, isTempPart, hints);
    }

    public UnboundRelation(ObjectId id, List<String> nameParts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<String> partNames, boolean isTempPart,
            List<String> hints) {
        super(id, PlanType.LOGICAL_UNBOUND_RELATION, groupExpression, logicalProperties);
        this.nameParts = ImmutableList.copyOf(Objects.requireNonNull(nameParts, "nameParts should not null"));
        this.partNames = ImmutableList.copyOf(Objects.requireNonNull(partNames, "partNames should not null"));
        this.isTempPart = isTempPart;
        this.hints = ImmutableList.copyOf(Objects.requireNonNull(hints, "hints should not be null."));
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
        return new UnboundRelation(id, nameParts, groupExpression, Optional.of(getLogicalProperties()), partNames,
                isTempPart, hints);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new UnboundRelation(id, nameParts, Optional.empty(), logicalProperties, partNames,
                isTempPart, hints);
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList(
                "id", id,
                "nameParts", StringUtils.join(nameParts, ".")
        );
        if (CollectionUtils.isNotEmpty(hints)) {
            args.add("hints");
            args.add(StringUtils.join(hints, ", "));
        }
        return Utils.toSqlString("UnboundRelation", args.toArray());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundRelation(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " don't support getExpression()");
    }

    public ObjectId getId() {
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

    public List<String> getPartNames() {
        return partNames;
    }

    public boolean isTempPart() {
        return isTempPart;
    }

    public List<String> getHints() {
        return hints;
    }
}
