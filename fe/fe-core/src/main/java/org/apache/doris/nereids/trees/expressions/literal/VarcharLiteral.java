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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Varchar type literal, in theory,
 * the difference from StringLiteral is that VarcharLiteral keeps the length information.
 */
public class VarcharLiteral extends Literal {

    private final String value;

    public VarcharLiteral(String value) {
        super(VarcharType.SYSTEM_DEFAULT);
        this.value = Objects.requireNonNull(value);
    }

    public VarcharLiteral(String value, int len) {
        super(VarcharType.createVarcharType(len));
        this.value = Objects.requireNonNull(value);
        Preconditions.checkArgument(value.length() <= len);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVarcharLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new StringLiteral(value);
    }

    @Override
    public String toString() {
        return "'" + value + "'";
    }

    private DateLiteral convertToDate(DataType targetType) throws AnalysisException {
        DateLiteral dateLiteral = null;
        if (targetType.isDate()) {
            dateLiteral = new DateLiteral(value);
        } else if (targetType.isDateTime()) {
            dateLiteral = new DateTimeLiteral(value);
        }
        return dateLiteral;
    }

    @Override
    public double getDouble() {
        long v = 0;
        int pos = 0;
        int len = Math.min(value.length(), 8);
        while (pos < len) {
            v += ((long) value.charAt(pos)) << ((7 - pos) * 8);
            pos++;
        }
        return (double) v;
    }
}
