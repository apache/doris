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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class DecimalLiteralTest {

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
        Assert.assertEquals(literal, literal.uncheckedCastTo(Type.DECIMALV2));

        Assert.assertEquals(1, literal.compareLiteral(new NullLiteral()));
    }
}
