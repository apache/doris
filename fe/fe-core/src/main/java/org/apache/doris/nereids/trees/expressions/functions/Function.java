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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;

import java.util.List;
import java.util.Objects;

/**
 * function in nereids.
 */
public abstract class Function extends Expression {
    private final String name;

    public Function(String name, Expression... children) {
        super(children);
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    public Function(String name, List<Expression> children) {
        super(children);
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    public boolean isHighOrder() {
        return !children.isEmpty() && children.get(0) instanceof Lambda;
    }

    public final String getName() {
        return name;
    }
}
