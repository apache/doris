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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;

/**
 * Represents Boolean literal
 */
public class BooleanLiteral extends Literal {

    public static final BooleanLiteral TRUE = new BooleanLiteral(true);
    public static final BooleanLiteral FALSE = new BooleanLiteral(false);

    private final boolean value;

    private BooleanLiteral(boolean value) {
        super(BooleanType.INSTANCE);
        this.value = value;
    }

    public static BooleanLiteral of(boolean value) {
        if (value) {
            return TRUE;
        } else {
            return FALSE;
        }
    }

    public static BooleanLiteral of(Boolean value) {
        return of(value.booleanValue());
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public String toString() {
        return Boolean.toString(value).toUpperCase();
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new BoolLiteral(value);
    }

    @Override
    public double getDouble() {
        if (value) {
            return 1.0;
        } else {
            return 0;
        }
    }
}
