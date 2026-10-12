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

package org.apache.doris.nereids.types.coercion;

import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;

import org.apache.commons.lang3.NotImplementedException;

/**
 * Abstract class for all integral data type in Nereids.
 */
public class IntegralType extends NumericType {

    public static final IntegralType INSTANCE = new IntegralType();

    @Override
    public DataType defaultConcreteType() {
        return BigIntType.INSTANCE;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof IntegralType;
    }

    @Override
    public String simpleString() {
        return "integral";
    }

    @Override
    public boolean isInjectiveCastTo(DataType target) {
        if (target instanceof IntegralType) {
            return this.equals(target) || ((IntegralType) target).widerThan(this);
        }
        // IEEE-754 FLOAT and DOUBLE have 24 and 53 bits of integer precision respectively.
        // Consequently every TINYINT/SMALLINT value is exact in FLOAT, and every value through
        // INT is exact in DOUBLE; wider integer domains contain values that would collide.
        if (target instanceof FloatType) {
            return range() <= SmallIntType.RANGE;
        }
        if (target instanceof DoubleType) {
            return range() <= IntegerType.RANGE;
        }
        if (target instanceof DecimalV2Type) {
            // DECIMALV2 is deprecated, so every cast involving it is conservatively non-injective.
            return false;
        }
        // Decimal casts preserve an integer exactly when the target has enough integer digits.
        // This also covers LARGEINT -> DECIMAL256 when a precision of at least 39 is available.
        if (target instanceof DecimalV3Type) {
            return ((DecimalV3Type) target).getRange() >= range();
        }
        return target instanceof CharacterType;
    }

    public boolean widerThan(IntegralType other) {
        return this.width() > other.width();
    }

    // The maximum number of digits that Integer can represent.
    public int range() {
        throw new NotImplementedException("should be implemented by derived class");
    }
}
