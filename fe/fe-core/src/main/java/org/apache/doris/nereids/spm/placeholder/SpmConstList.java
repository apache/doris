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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * SpmConstList - SPM IN-list placeholder function expression.
 *
 * The list placeholder function is rendered as _spm_const_list(id, v1, v2, ...). During
 * parameterization,
 * SPMPlaceholderBuilder replaces the list part of "IN (constant list)" with a
 * SpmConstList. During query rewrite the actual value set is extracted from the user's IN
 * list by id and substituted back into planSql.
 *
 * This class is an internal AST node: the value list is kept as children to preserve the
 * original values used when the baseline was created (only for readability, not part of
 * the semantics).
 */
public class SpmConstList extends Expression implements AlwaysNotNullable {

    /** Auto-increment placeholder id. */
    private final long id;

    /**
     * Constructs a SpmConstList.
     *
     * @param id     placeholder id
     * @param values the original value list (kept as children)
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

    /** Returns the original value list (all children). */
    public List<Expression> getValues() {
        return children();
    }

    /**
     * Rebuilds the placeholder with new children (see
     * {@link SpmConstVar#withChildren} for why the base implementation cannot be
     * used). An unchanged value list returns the same node; otherwise the
     * placeholder id is kept and only the readability payload is updated.
     */
    @Override
    public Expression withChildren(List<Expression> children) {
        if (children.equals(this.children())) {
            return this;
        }
        return new SpmConstList(id, children);
    }

    /**
     * A list placeholder carries its original value literals as children; expose the
     * type of the first value so plan-node construction / type derivation (which may run
     * while the placeholder is still in the tree) never hits the unbound default.
     */
    @Override
    public DataType getDataType() {
        return children().isEmpty() ? NullType.INSTANCE : child(0).getDataType();
    }

    /**
     * A placeholder must never be treated as a foldable expression: while it keeps its
     * original value list as children, the placeholder is a marker whose values are
     * replaced by the user query at rewrite time. Folding it during the baseline CREATE
     * optimization would bake the captured constants into the frozen plan and break the
     * later value substitution.
     */
    @Override
    public boolean foldable() {
        return false;
    }

    /**
     * A placeholder must never be treated as a constant even though its children are
     * literals: constant-collecting / constant-moving rules would fold / move / evaluate
     * it and lose the placeholder marker before the physical plan is decompiled.
     */
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
