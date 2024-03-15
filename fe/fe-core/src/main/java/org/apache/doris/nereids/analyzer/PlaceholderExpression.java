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

import org.apache.doris.nereids.parser.CommonFnCallTransformer.PlaceholderCollector;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Expression placeHolder, the expression in PlaceHolderExpression will be collected by
 *
 * @see PlaceholderCollector
 */
public class PlaceholderExpression extends Expression implements AlwaysNotNullable {

    private final ImmutableSet<Class<? extends Expression>> delegateClazzSet;
    /**
     * start from 1, set the index of this placeholderExpression in sourceFnTransformedArguments
     * this placeholderExpression will be replaced later
     */
    private final int position;

    public PlaceholderExpression(List<Expression> children, Class<? extends Expression> delegateClazz, int position) {
        super(children);
        this.delegateClazzSet = ImmutableSet.of(
                Objects.requireNonNull(delegateClazz, "delegateClazz should not be null"));
        this.position = position;
    }

    public PlaceholderExpression(List<Expression> children,
                                 Set<Class<? extends Expression>> delegateClazzSet, int position) {
        super(children);
        this.delegateClazzSet = ImmutableSet.copyOf(delegateClazzSet);
        this.position = position;
    }

    public static PlaceholderExpression of(Class<? extends Expression> delegateClazz, int position) {
        return new PlaceholderExpression(ImmutableList.of(), delegateClazz, position);
    }

    public static PlaceholderExpression of(Set<Class<? extends Expression>> delegateClazzSet, int position) {
        return new PlaceholderExpression(ImmutableList.of(), delegateClazzSet, position);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        visitor.visitPlaceholderExpression(this, context);
        return visitor.visit(this, context);
    }

    public Set<Class<? extends Expression>> getDelegateClazzSet() {
        return delegateClazzSet;
    }

    public int getPosition() {
        return position;
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
        PlaceholderExpression that = (PlaceholderExpression) o;
        return position == that.position && Objects.equals(delegateClazzSet, that.delegateClazzSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delegateClazzSet, position);
    }
}
