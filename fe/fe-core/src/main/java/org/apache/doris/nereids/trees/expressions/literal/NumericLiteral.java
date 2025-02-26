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

import org.apache.doris.nereids.types.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * numeric literal
 */
public abstract class NumericLiteral extends Literal implements ComparableLiteral {
    /**
     * Constructor for NumericLiteral.
     *
     * @param dataType logical data type in Nereids
     */
    public NumericLiteral(DataType dataType) {
        super(dataType);
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof NumericLiteral) {
            if (this instanceof IntegerLikeLiteral && other instanceof IntegerLikeLiteral) {
                IntegerLikeLiteral thisInteger = (IntegerLikeLiteral) this;
                IntegerLikeLiteral otherInteger = (IntegerLikeLiteral) other;
                if (this instanceof LargeIntLiteral || other instanceof LargeIntLiteral) {
                    BigInteger leftValue = this instanceof LargeIntLiteral ? ((LargeIntLiteral) this).getValue()
                            : new BigInteger(String.valueOf(thisInteger.getLongValue()));
                    BigInteger rightValue = other instanceof LargeIntLiteral ? ((LargeIntLiteral) other).getValue()
                            : new BigInteger(String.valueOf(otherInteger.getLongValue()));
                    return leftValue.compareTo(rightValue);
                } else {
                    return Long.compare(((IntegerLikeLiteral) this).getLongValue(),
                            ((IntegerLikeLiteral) other).getLongValue());
                }
            }
            if (this instanceof DecimalLiteral || this instanceof DecimalV3Literal
                    || other instanceof DecimalLiteral || other instanceof DecimalV3Literal) {
                return this.getBigDecimalValue().compareTo(((NumericLiteral) other).getBigDecimalValue());
            }
            return Double.compare(this.getDouble(), ((Literal) other).getDouble());
        }
        if (other instanceof NullLiteral) {
            return 1;
        }
        if (other instanceof MaxLiteral) {
            return -1;
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + dataType + ") vs " + other + " (" + ((Literal) other).dataType + ")");
    }

    public abstract BigDecimal getBigDecimalValue();
}
