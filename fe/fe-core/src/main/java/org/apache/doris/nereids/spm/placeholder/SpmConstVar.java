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
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * SpmConstVar - SPM scalar placeholder function expression.
 *
 * The scalar placeholder function is rendered as _spm_const_var(id, value). During
 * parameterization,
 * SPMPlaceholderBuilder replaces each Literal in the query with SpmConstVar(id, original
 * value). During query rewrite the actual value is extracted from the user query by id
 * and substituted back into the placeholder in planSql.
 *
 * This class is an internal AST node (not a user-callable built-in function): the value
 * is kept as the only child to preserve the original literal used when the baseline was
 * created (only for readability, not part of the semantics).
 */
public class SpmConstVar extends Expression implements UnaryExpression, AlwaysNullable {

    /** Auto-increment placeholder id. */
    private final long id;

    /**
     * Constructs a SpmConstVar.
     *
     * @param id    placeholder id
     * @param value the original literal (kept as the child)
     */
    public SpmConstVar(long id, Expression value) {
        super(value);
        this.id = id;
    }

    /**
     * Factory method for SPMPlaceholderBuilder.
     */
    public static SpmConstVar of(long id, Expression value) {
        return new SpmConstVar(id, value);
    }

    public long getId() {
        return id;
    }

    /** Returns the original placeholder value (the only child). */
    public Expression getValue() {
        return child();
    }

    /**
     * Rebuilds the placeholder with new children. Generic expression walkers (e.g.
     * OrToIn rebuilding every non-leaf predicate node) invoke withChildren
     * unconditionally, and the base Expression implementation throws; a placeholder
     * therefore has to implement the contract like any other expression. The value
     * child is only a readability payload: an unchanged child returns the same node
     * (identity preserved), a changed one keeps the placeholder id and updates the
     * payload.
     */
    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1,
                "SpmConstVar must have exactly one child");
        if (children.get(0).equals(child())) {
            return this;
        }
        return new SpmConstVar(id, children.get(0));
    }

    /**
     * The placeholder keeps its original literal as the child, so its data type is the
     * child's type. A placeholder lives in plan trees during parameterization / matching /
     * substitution; plan nodes eagerly compute their output (Alias.toSlot etc.) and the
     * analyzer derives expression types, so the placeholder must expose a real type
     * instead of falling back to the unbound default.
     */
    @Override
    public DataType getDataType() {
        return child().getDataType();
    }

    /**
     * A placeholder must never be treated as a foldable expression: while it keeps its
     * original literal as the child, the placeholder is a marker whose value is replaced
     * by the user query at rewrite time. Folding (or constant-folding) it during the
     * baseline CREATE optimization would bake the captured constant into the frozen
     * plan and break the later value substitution.
     */
    @Override
    public boolean foldable() {
        return false;
    }

    /**
     * A placeholder must never be treated as a constant even though its child is a
     * literal: constant-collecting / constant-moving rules (FE and BE constant folding,
     * predicate normalization, partition pruning, ...) would treat it as a plain
     * constant and fold / move / evaluate it, losing the placeholder marker before the
     * physical plan is decompiled.
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
        return "_spm_const_var(" + id + ", " + child().toSql() + ")";
    }

    @Override
    public String toString() {
        return "SpmConstVar(" + id + ", " + child() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpmConstVar other = (SpmConstVar) o;
        return id == other.id && Objects.equals(child(), other.child());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, child());
    }

}
