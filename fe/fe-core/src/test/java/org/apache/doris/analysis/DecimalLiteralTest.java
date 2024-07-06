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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FormatOptions;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class DecimalLiteralTest {

    @Test
    public void testGetStringInFe() {
        BigDecimal decimal = new BigDecimal("-123456789123456789.123456789");
        DecimalLiteral literal = new DecimalLiteral(decimal);
        String s = literal.getStringValueInFe(FormatOptions.getDefault());
        Assert.assertEquals("-123456789123456789.123456789", s);
        Assert.assertEquals("-123456789123456789.123456789", literal.getStringValueInFe(FormatOptions.getForPresto()));
    }

    @Test
    public void testHashValue() throws AnalysisException {
        BigDecimal decimal = new BigDecimal("-123456789123456789.123456789");
        DecimalLiteral literal = new DecimalLiteral(decimal);

        ByteBuffer buffer = literal.getHashValue(PrimitiveType.DECIMALV2);
        long longValue = buffer.getLong();
        int fracValue = buffer.getInt();
        System.out.println("long: " + longValue);
        System.out.println("frac: " + fracValue);
        Assert.assertEquals(-123456789123456789L, longValue);
        Assert.assertEquals(-123456789, fracValue);

        // if DecimalLiteral need to cast to Decimal and Decimalv2, need to cast
        // to themselves
        if (!Config.enable_decimal_conversion) {
            Assert.assertEquals(literal, literal.uncheckedCastTo(Type.DECIMALV2));
        }

        Assert.assertEquals(1, literal.compareLiteral(new NullLiteral()));
    }

    @Test
    public  void testTypePrecision() {
        BigDecimal decimal = new BigDecimal("-123456789123456789.123456789");
        DecimalLiteral literal = new DecimalLiteral(decimal);
        int precision = ((ScalarType) literal.getType()).getScalarPrecision();
        int scale = ((ScalarType) literal.getType()).getScalarScale();
        Assert.assertEquals(27, precision);
        Assert.assertEquals(9, scale);

        decimal = new BigDecimal("-0.00123");
        literal = new DecimalLiteral(decimal);
        precision = ((ScalarType) literal.getType()).getScalarPrecision();
        scale = ((ScalarType) literal.getType()).getScalarScale();
        Assert.assertEquals(5, precision);
        Assert.assertEquals(5, scale);

        decimal = new BigDecimal("20000");
        literal = new DecimalLiteral(decimal);
        precision = ((ScalarType) literal.getType()).getScalarPrecision();
        scale = ((ScalarType) literal.getType()).getScalarScale();
        Assert.assertEquals(5, precision);
        Assert.assertEquals(0, scale);

        decimal = new BigDecimal("0.123");
        literal = new DecimalLiteral(decimal);
        precision = ((ScalarType) literal.getType()).getScalarPrecision();
        scale = ((ScalarType) literal.getType()).getScalarScale();
        Assert.assertEquals(3, precision);
        Assert.assertEquals(3, scale);

        decimal = new BigDecimal("197323961.520000000000000000000000000000");
        literal = new DecimalLiteral(decimal);
        precision = ((ScalarType) literal.getType()).getScalarPrecision();
        scale = ((ScalarType) literal.getType()).getScalarScale();
        Assert.assertEquals(38, precision);
        Assert.assertEquals(29, scale);
    }
}
