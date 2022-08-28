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

package org.apache.doris.nereids.types;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataTypeTest {
    @Test
    public void testDataTypeEquals() {
        // NullType nullType1 = new NullType();
        // NullType nullType2 = new NullType();
        // Assertions.assertEquals(nullType1, nullType2);
        // Assertions.assertEquals(nullType1.hashCode(), nullType2.hashCode());
        //
        // BooleanType booleanType1 = new BooleanType();
        // BooleanType booleanType2 = new BooleanType();
        // Assertions.assertEquals(booleanType1, booleanType2);
        // Assertions.assertEquals(booleanType1.hashCode(), booleanType2.hashCode());
        //
        // TinyIntType tinyIntType1 = new TinyIntType();
        // TinyIntType tinyIntType2 = new TinyIntType();
        // Assertions.assertEquals(tinyIntType1, tinyIntType2);
        // Assertions.assertEquals(tinyIntType1.hashCode(), tinyIntType2.hashCode());
        //
        // SmallIntType smallIntType1 = new SmallIntType();
        // SmallIntType smallIntType2 = new SmallIntType();
        // Assertions.assertEquals(smallIntType1, smallIntType2);
        // Assertions.assertEquals(smallIntType1.hashCode(), smallIntType2.hashCode());
        //
        // IntegerType integerType1 = new IntegerType();
        // IntegerType integerType2 = new IntegerType();
        // Assertions.assertEquals(integerType1, integerType2);
        // Assertions.assertEquals(integerType1.hashCode(), integerType2.hashCode());
        //
        // BigIntType bigIntType1 = new BigIntType();
        // BigIntType bigIntType2 = new BigIntType();
        // Assertions.assertEquals(bigIntType1, bigIntType2);
        // Assertions.assertEquals(bigIntType1.hashCode(), bigIntType2.hashCode());
        //
        // LargeIntType largeIntType1 = new LargeIntType();
        // LargeIntType largeIntType2 = new LargeIntType();
        // Assertions.assertEquals(largeIntType1, largeIntType2);
        // Assertions.assertEquals(largeIntType1.hashCode(), largeIntType2.hashCode());
        //
        // FloatType floatType1 = new FloatType();
        // FloatType floatType2 = new FloatType();
        // Assertions.assertEquals(floatType1, floatType2);
        // Assertions.assertEquals(floatType1.hashCode(), floatType2.hashCode());
        //
        // DoubleType doubleType1 = new DoubleType();
        // DoubleType doubleType2 = new DoubleType();
        // Assertions.assertEquals(doubleType1, doubleType2);
        // Assertions.assertEquals(doubleType1.hashCode(), doubleType2.hashCode());

        DecimalType decimalType1 = new DecimalType(27, 9);
        DecimalType decimalType2 = new DecimalType(27, 9);
        DecimalType decimalType3 = new DecimalType(28, 9);
        DecimalType decimalType4 = new DecimalType(27, 10);
        Assertions.assertEquals(decimalType1, decimalType2);
        Assertions.assertEquals(decimalType1.hashCode(), decimalType2.hashCode());
        Assertions.assertNotEquals(decimalType1, decimalType3);
        Assertions.assertNotEquals(decimalType1.hashCode(), decimalType3.hashCode());
        Assertions.assertNotEquals(decimalType1, decimalType4);
        Assertions.assertNotEquals(decimalType1.hashCode(), decimalType4.hashCode());

        CharType charType1 = new CharType(10);
        CharType charType2 = new CharType(10);
        CharType charType3 = new CharType(15);
        Assertions.assertEquals(charType1, charType2);
        Assertions.assertEquals(charType1.hashCode(), charType2.hashCode());
        Assertions.assertNotEquals(charType1, charType3);
        Assertions.assertNotEquals(charType1.hashCode(), charType3.hashCode());

        VarcharType varcharType1 = new VarcharType(32);
        VarcharType varcharType2 = new VarcharType(32);
        VarcharType varcharType3 = new VarcharType(64);
        Assertions.assertEquals(varcharType1, varcharType2);
        Assertions.assertEquals(varcharType1.hashCode(), varcharType2.hashCode());
        Assertions.assertNotEquals(varcharType1, varcharType3);
        Assertions.assertNotEquals(varcharType1.hashCode(), varcharType3.hashCode());

        // StringType stringType1 = new StringType();
        // StringType stringType2 = new StringType();
        // Assertions.assertEquals(stringType1, stringType2);
        // Assertions.assertEquals(stringType1.hashCode(), stringType2.hashCode());

        // DateType dateType1 = new DateType();
        // DateType dateType2 = new DateType();
        // Assertions.assertEquals(dateType1, dateType2);
        // Assertions.assertEquals(dateType1.hashCode(), dateType2.hashCode());

        // DateTimeType dateTimeType1 = new DateTimeType();
        // DateTimeType dateTimeType2 = new DateTimeType();
        // Assertions.assertEquals(dateTimeType1, dateTimeType2);
        // Assertions.assertEquals(dateTimeType1.hashCode(), dateTimeType2.hashCode());
    }
}
