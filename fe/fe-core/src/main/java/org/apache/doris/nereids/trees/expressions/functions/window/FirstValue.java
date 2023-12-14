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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Window function: First_value()
 */
public class FirstValue extends FirstOrLastValue {

    public FirstValue(Expression child) {
        super("first_value", child);
    }

    public FirstValue(Expression child, Expression ignoreNullValue) {
        super("first_value", child, ignoreNullValue);
    }

    public FirstValue(List<Expression> children) {
        super("first_value", children);
    }

    @Override
    public FirstValue withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
        return new FirstValue(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitFirstValue(this, context);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }
}
