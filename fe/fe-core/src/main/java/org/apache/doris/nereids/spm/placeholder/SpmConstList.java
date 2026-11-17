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

package org.apache.doris.nereids.spm.placeholder;

import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Marker for the options of an IN predicate. The id selects a matched user IN predicate whose options replace this
 * marker during replay. Captured children preserve payload values and type information. AST rendering includes
 * those children; frozen SQL rendering emits only the id.
 */
public class SpmConstList extends Expression implements AlwaysNullable {

    // Conservatively NULLABLE (like SpmConstVar): the value list is substituted at
    // rewrite time and may contain NULL whatever the captured list was, so a NOT NULL
    // comparand's IN predicate stays nullable. Implementing AlwaysNotNullable made
    // InPredicate.nullable false, so the enabled NullableDependentExpressionRewrite
    // folded a surrounding COALESCE(k IN (1, NULL), false) into k IN (1, NULL) while
    // the frozen plan was decompiled - after list substitution even the identical
    // query returned NULL instead of false (integer widening casts preserve the false
    // non-nullability, so the marker itself must be nullable).

    /** Marker id, normally allocated by SPMPlaceholderBuilder. */
    private final long id;

    /**
     * Constructs a SpmConstList.
     *
     * Parameter id: placeholder id
     * Parameter values: the original value list (kept as children)
     */
    public SpmConstList(long id, List<Expression> values) {
        super(ImmutableList.copyOf(values));
        this.id = id;
    }

    /**
     * Factory method for SPMPlaceholderBuilder.
     */
    public static SpmConstList of(long id, List<Expression> values) {
        return new SpmConstList(id, values);
    }

    public long getId() {
        return id;
    }

    /** Returns the current payload expressions, stored as the marker children. */
    public List<Expression> getValues() {
        return children();
    }

    /**
     * Returns this node when the replacement payload list equals the current children; otherwise rebuilds the
     * marker with the same id and new payload expressions. The first payload child supplies capture-time type
     * information.
     */
    @Override
    public Expression withChildren(List<Expression> children) {
        if (children.equals(this.children())) {
            return this;
        }
        return new SpmConstList(id, children);
    }

    /** Uses the first payload expression type, or NullType for an empty payload. */
    @Override
    public DataType getDataType() {
        return children().isEmpty() ? NullType.INSTANCE : child(0).getDataType();
    }

    /** Prevents constant folding from erasing the list marker before replay substitution. */
    @Override
    public boolean foldable() {
        return false;
    }

    /** Keeps the list marker out of constant-expression rewrites. */
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public Optional<Origin> getOrigin() {
        return super.getOrigin();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public String computeToSql() {
        StringBuilder sb = new StringBuilder("_spm_const_list(").append(id);
        for (Expression value : children()) {
            sb.append(", ").append(value.toSql());
        }
        return sb.append(")").toString();
    }

    @Override
    public String toString() {
        return "SpmConstList(" + id + ", " + children() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpmConstList other = (SpmConstList) o;
        return id == other.id && Objects.equals(children(), other.children());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, children());
    }
}
