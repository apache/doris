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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Functions used by all groupingSets.
 */
public abstract class GroupingScalarFunction extends ScalarFunction {

    protected final Optional<List<Expression>> realChildren;

    public GroupingScalarFunction(String name, Expression... arguments) {
        super(name, arguments);
        this.realChildren = Optional.empty();
    }

    public GroupingScalarFunction(String name, Optional<List<Expression>> realChildren, Expression... arguments) {
        super(name, arguments);
        this.realChildren = realChildren.map(ImmutableList::copyOf);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingScalarFunction(this, context);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BigIntType.INSTANCE;
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return children().stream().map(Expression::getDataType).collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return null;
    }

    @Override
    public FunctionSignature searchSignature() {
        return null;
    }

    public Optional<List<Expression>> getRealChildren() {
        return realChildren;
    }

    public abstract GroupingScalarFunction repeatChildrenWithVirtualRef(
            VirtualSlotReference virtualSlotReference,
            Optional<List<Expression>> realChildren);
}
