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

import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.common.io.Text;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** BoundFunction. */
public abstract class BoundFunction extends Function implements ComputeSignature {
    private final String name;

    private final Supplier<FunctionSignature> signatureCache = Suppliers.memoize(() -> {
        // first step: find the candidate signature in the signature list
        FunctionSignature matchedSignature = searchSignature(getSignatures());
        // second step: change the signature, e.g. fill precision for decimal v2
        return computeSignature(matchedSignature);
    });

    public BoundFunction(String name, Expression... arguments) {
        super(arguments);
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    public BoundFunction(String name, List<Expression> children) {
        super(children);
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    public String getName() {
        return name;
    }

    public FunctionSignature getSignature() {
        return signatureCache.get();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBoundFunction(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoundFunction that = (BoundFunction) o;
        return Objects.equals(name, that.name) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, children);
    }

    @Override
    public String toSql() throws UnboundException {
        String args = children()
                .stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", "));
        return name + "(" + args + ")";
    }

    @Override
    public String toString() {
        String args = children()
                .stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", "));
        return name + "(" + args + ")";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(MetaCode.BOUND_FUNCTION.getCode());
        Text.writeString(out, name);
        out.writeInt(children.size());
        for (Expression expression : children) {
            expression.write(out);
        }
    }

    /**
     * read a function from metadata
     */
    public static BoundFunction read(DataInput input) throws IOException {
        String name = Text.readString(input);
        int count = input.readInt();
        List<Expression> children = Lists.newArrayList();
        for (int i = 0; i < count; ++i) {
            children.add(Expression.read(input));
        }
        FunctionRegistry functionRegistry = new FunctionRegistry();
        return functionRegistry.findFunctionBuilder(name, children).build(name, children);
    }
}
