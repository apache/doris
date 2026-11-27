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
 * Scalar marker identified by a builder-assigned id. Its child preserves the captured expression and supplies its
 * data type during planning. Matching records a replacement under the id; replay replaces the marker with that
 * expression. AST SQL rendering includes the payload, while frozen SQL rendering emits only the id.
 */
public class SpmConstVar extends Expression implements UnaryExpression, AlwaysNullable {

    /** Marker id, normally allocated by SPMPlaceholderBuilder. */
    private final long id;

    /** Constructs a scalar marker with the supplied id and captured payload expression. */
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

    /** Returns the current captured payload expression. */
    public Expression getValue() {
        return child();
    }

    /**
     * Requires exactly one payload child. Returns this node when that child is equal to the current payload;
     * otherwise rebuilds the marker with the same id and the new payload. The payload supplies capture-time type
     * information.
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

    /** Uses the payload type so capture-time analysis can type expressions containing the marker. */
    @Override
    public DataType getDataType() {
        return child().getDataType();
    }

    /** Prevents constant folding from erasing the marker before replay substitution. */
    @Override
    public boolean foldable() {
        return false;
    }

    /** Keeps the marker out of rewrites that treat constant expressions as replaceable values. */
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
