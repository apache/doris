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

import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DoubleType;

import java.math.BigDecimal;

/**
 * Double literal
 */
public class DoubleLiteral extends FractionalLiteral {

    private final double value;

    public DoubleLiteral(double value) {
        super(DoubleType.INSTANCE);
        this.value = value;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(String.valueOf(value));
    }

    @Override
    public Double getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDoubleLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new FloatLiteral(value, Type.DOUBLE);
    }

    @Override
    public String getStringValue() {
        Double num = getValue();
        if (Double.isNaN(num)) {
            return "nan";
        } else if (Double.isInfinite(num)) {
            return num > 0 ? "inf" : "-inf";
        }

        // Use %.17g to format the result，replace 'E' with 'e'
        String formatted = String.format("%.17g", num).replace('E', 'e');

        // Remove trailing .0 in scientific notation.
        if (formatted.contains("e")) {
            String[] parts = formatted.split("e");
            String mantissa = parts[0];
            String exponent = parts.length > 1 ? "e" + parts[1] : "";
            mantissa = mantissa.replaceAll("\\.?0+$", "");
            if (mantissa.isEmpty()) {
                mantissa = "0";
            }
            formatted = mantissa + exponent;
        } else if (formatted.contains(".")) {
            // remove trailing .0 in fixed-point representation
            formatted = formatted.replaceAll("\\.?0+$", "");
        }

        return formatted;
    }
}
