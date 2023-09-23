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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.VarcharType;

/**
 * Varchar type literal, in theory,
 * the difference from StringLiteral is that VarcharLiteral keeps the length information.
 */
public class VarcharLiteral extends StringLikeLiteral {

    public VarcharLiteral(String value) {
        super(value, VarcharType.createVarcharType(value.length()));
    }

    public VarcharLiteral(String value, int len) {
        super(len >= 0 ? value.substring(0, Math.min(value.length(), len)) : value, VarcharType.createVarcharType(len));
    }

    @Override
    public String getValue() {
        return getStringValue();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVarcharLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new StringLiteral(value);
    }
}
