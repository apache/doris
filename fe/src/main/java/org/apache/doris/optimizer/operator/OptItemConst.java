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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.optimizer.OptUtils;

public class OptItemConst extends OptItem {
    // now use this
    private LiteralExpr literal;
    private boolean isNull = false;

    public OptItemConst(LiteralExpr literal) {
        super(OptOperatorType.OP_ITEM_CONST);
        this.literal = literal;
        this.isNull = false;
    }
    public OptItemConst(LiteralExpr literal, boolean isNull) {
        super(OptOperatorType.OP_ITEM_CONST);
        this.literal = literal;
        this.isNull = isNull;
    }

    public static OptItemConst createBool(boolean value) {
        return new OptItemConst(new BoolLiteral(value));
    }
    public static OptItemConst createBool(boolean value, boolean isNull) {
        return new OptItemConst(new BoolLiteral(value), isNull);
    }
    public LiteralExpr getLiteral() { return literal; }

    @Override
    public Type getReturnType() {
        return null;
    }

    @Override
    public int hashCode() {
        return OptUtils.combineHash(super.hashCode(), literal.hashCode());
    }

    @Override
    public boolean equals(Object object) {
        if (!super.equals(object)) {
            return false;
        }
        OptItemConst rhs = (OptItemConst) object;
        return literal.equals(rhs.literal);
    }

    @Override
    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("ItemBinaryPredicate(op=").append(literal).append(")");
        return sb.toString();
    }
}
