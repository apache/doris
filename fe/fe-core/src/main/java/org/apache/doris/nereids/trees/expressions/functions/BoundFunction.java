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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** BoundFunction. */
public abstract class BoundFunction extends Function implements ComputeSignature {

    private final Supplier<FunctionSignature> signatureCache = Suppliers.memoize(() -> {
        // first step: find the candidate signature in the signature list
        FunctionSignature matchedSignature = searchSignature(getSignatures());
        // second step: change the signature, e.g. fill precision for decimal v2
        return computeSignature(matchedSignature);
    });

    public BoundFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public BoundFunction(String name, List<Expression> children) {
        super(name, children);
    }

    @Override
    public String getExpressionName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(getName(), DEFAULT_EXPRESSION_NAME));
        }
        return this.exprName.get();
    }

    public FunctionSignature getSignature() {
        return signatureCache.get();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBoundFunction(this, context);
    }

    @Override
    protected boolean extraEquals(Expression that) {
        return Objects.equals(getName(), ((BoundFunction) that).getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), children);
    }

    @Override
    public String toSql() throws UnboundException {
        StringBuilder sql = new StringBuilder(getName()).append("(");
        int arity = arity();
        for (int i = 0; i < arity; i++) {
            Expression arg = child(i);
            sql.append(arg.toSql());
            if (i + 1 < arity) {
                sql.append(", ");
            }
        }
        return sql.append(")").toString();
    }

    @Override
    public String toString() {
        String args = children()
                .stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", "));
        return getName() + "(" + args + ")";
    }
}
