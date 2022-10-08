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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;

/**
 * Star expression.
 */
public class UnboundStar extends NamedExpression implements LeafExpression, Unbound, PropagateNullable {
    private final List<String> qualifier;

    public UnboundStar(List<String> qualifier) {
        this.qualifier = Objects.requireNonNull(qualifier, "qualifier can not be null");
    }

    @Override
    public String toSql() {
        return Utils.qualifiedName(qualifier, "*");
    }

    @Override
    public List<String> getQualifier() throws UnboundException {
        return qualifier;
    }

    @Override
    public String toString() {
        return toSql();
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
        return qualifier.equals(that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifier);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundStar(this, context);
    }
}
