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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** BoundStar is used to wrap list of slots for temporary. */
public class BoundStar extends NamedExpression implements PropagateNullable {
    private Pair<Integer, Integer> indexInSqlString;

    public BoundStar(List<Slot> children) {
        super((List) children);
        Preconditions.checkArgument(children.stream().noneMatch(slot -> slot instanceof UnboundSlot),
                "BoundStar can not wrap UnboundSlot"
        );
    }

    public BoundStar(List<Slot> children, Pair<Integer, Integer> indexInSqlString) {
        super((List) children);
        Preconditions.checkArgument(children.stream().noneMatch(slot -> slot instanceof UnboundSlot),
                "BoundStar can not wrap UnboundSlot"
        );
        this.indexInSqlString = indexInSqlString;
    }

    public String toSql() {
        return children.stream().map(Expression::toSql).collect(Collectors.joining(", "));
    }

    public String toSqlWithBacktick() {
        return children.stream().map(slot -> ((SlotReference) slot).getQualifiedNameWithBacktick())
                .collect(Collectors.joining(", "));
    }

    /** toSqlWithBacktick */
    public String toSqlWithBacktick(List<NamedExpression> excepts) {
        List<Expression> outputs = Lists.newArrayList();
        Set<Expression> exceptSet = Sets.newHashSet();
        exceptSet.addAll(excepts);
        for (Expression child : children) {
            if (!exceptSet.contains(child)) {
                outputs.add(child);
            }
        }
        return outputs.stream().map(slot -> ((SlotReference) slot).getQualifiedNameWithBacktick())
                .collect(Collectors.joining(", "));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Slot> getSlots() {
        return (List) children();
    }

    public Pair<Integer, Integer> getIndexInSqlString() {
        return indexInSqlString;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBoundStar(this, context);
    }
}
