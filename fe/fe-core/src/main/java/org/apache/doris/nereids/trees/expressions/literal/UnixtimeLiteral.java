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

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.UnixtimeType;

/**
 * Represents Unixtime literal
 */
public class UnixtimeLiteral extends IntegerLikeLiteral {

    private final int value;

    /**
     * It's same as integer except it's -1 when the value > INT:MAX || value < INT:MIN
     */
    public UnixtimeLiteral(long value) {
        super(UnixtimeType.INSTANCE);
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            this.value = -1;
        } else {
            this.value = (int) value;
        }
    }

    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnixtimeLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new IntLiteral(value, Type.INT);
        } catch (AnalysisException e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "Can not convert to legacy literal: " + value, e);
        }
    }

    @Override
    public Number getNumber() {
        return value;
    }
}
