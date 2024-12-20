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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.SetType;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;

import java.util.Objects;

/**
 * Variable class
 */
public class VariableDesc extends Expression implements LeafExpression {
    final boolean isSystemVariable;
    final SetType setType;
    final String name;

    public VariableDesc(boolean isSystemVariable, SetType setType, String name) {
        super(ImmutableList.of());
        this.isSystemVariable = isSystemVariable;
        this.setType = setType;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public SetType getSetType() {
        return setType;
    }

    @Override
    public String computeToSql() {
        return toString();
    }

    @Override
    public boolean nullable() throws UnboundException {
        return false;
    }

    @Override
    public String toString() {
        if (isSystemVariable) {
            String s = setType.equals(SetType.GLOBAL) ? "global." : "session.";
            return "@@" + s + name;
        } else {
            return "@" + name;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSystemVariable, setType, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariableDesc other = (VariableDesc) o;
        return other.hashCode() == this.hashCode();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVariableDesc(this, context);
    }
}
