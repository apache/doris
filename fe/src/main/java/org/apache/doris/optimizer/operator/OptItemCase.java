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

package org.apache.doris.optimizer.operator;

import org.apache.doris.catalog.Type;
import org.apache.doris.optimizer.OptUtils;

// Indicates case expression 'CASE case_expr WHEN when_expr THEN then_expr ... ELSE else_expr'
// or 'CASE WHEN when_expr THEN then_expr ... ELSE else_expr'
// OptItemCase
// |--- case_expr
// |--- when_expr
// |--- then_expr
// ...
// |--- else_expr
public class OptItemCase extends OptItem {
    private boolean hasCase;
    private boolean hasElse;
    private Type type;

    public OptItemCase(boolean hasCase, boolean hasElse, Type type) {
        super(OptOperatorType.OP_ITEM_CASE);
        this.hasCase = hasCase;
        this.hasElse = hasElse;
        this.type = type;
    }

    public boolean hasCase() { return hasCase; }
    public boolean hasElse() { return hasElse; }

    @Override
    public Type getReturnType() {
        return type;
    }

    @Override
    public int hashCode() {
        int hash = OptUtils.combineHash(super.hashCode(), hasCase);
        hash = OptUtils.combineHash(hash, hasElse);
        hash = OptUtils.combineHash(hash, type.getPrimitiveType());
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptItemCase rhs = (OptItemCase) object;
        return hasCase == rhs.hasCase && hasElse == rhs.hasElse && type.equals(rhs.type);
    }

    @Override
    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("ItemCase(type=").append(type).append(")");
        return sb.toString();
    }
}
