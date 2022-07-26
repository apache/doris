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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;

/**
 * Represents String literal
 */
public class StringLiteral extends Literal {

    private final String value;

    /**
     * Constructor for Literal.
     *
     * @param value real value stored in java object
     */
    public StringLiteral(String value) {
        super(StringType.INSTANCE);
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStringLiteral(this, context);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (getDataType().equals(targetType)) {
            return this;
        }
        if (targetType.isDateType()) {
            return convertToDate(targetType);
        } else if (targetType.isIntType()) {
            return new IntegerLiteral(Integer.parseInt(value));
        }
        //todo other target type cast
        return this;
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
    public String toSql() {
        return "'" + value + "'";
    }
}
