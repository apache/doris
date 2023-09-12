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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.Objects;

/**
 * unbound variable
 */
public class UnboundVariable extends Expression implements Unbound {

    private final String name;
    private final VariableType type;

    public UnboundVariable(String name, VariableType type) {
        this.name = Objects.requireNonNull(name, "name should not be null");
        this.type = Objects.requireNonNull(type, "type should not be null");
    }

    public String getName() {
        return name;
    }

    public VariableType getType() {
        return type;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundVariable(this, context);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public String toString() throws UnboundException {
        if (type == VariableType.USER) {
            return "@" + name;
        } else {
            return "@@" + name;
        }
    }

    /**
     * variable type
     */
    public enum VariableType {
        GLOBAL,
        SESSION,
        DEFAULT,
        USER,
    }
}
