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

import org.apache.doris.nereids.parser.trino.TrinoFnCallTransformer.PlaceholderCollector;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Expression placeHolder, the expression in PlaceHolderExpression will be collected by
 *
 * @see PlaceholderCollector
 */
public class PlaceholderExpression extends Expression implements AlwaysNotNullable {

    private final Class<? extends Expression> delegateClazz;
    /**
     * 1 based
     */
    private final int position;

    public PlaceholderExpression(List<Expression> children, Class<? extends Expression> delegateClazz, int position) {
        super(children);
        this.delegateClazz = Objects.requireNonNull(delegateClazz, "delegateClazz should not be null");
        this.position = position;
    }

    public static PlaceholderExpression of(Class<? extends Expression> delegateClazz, int position) {
        return new PlaceholderExpression(ImmutableList.of(), delegateClazz, position);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    public Class<? extends Expression> getDelegateClazz() {
        return delegateClazz;
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
        return position == that.position && Objects.equals(delegateClazz, that.delegateClazz);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delegateClazz, position);
    }
}
