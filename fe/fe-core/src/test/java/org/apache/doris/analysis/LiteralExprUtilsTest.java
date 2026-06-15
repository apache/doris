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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

public class LiteralExprUtilsTest {

    @Test
    public void testDecimalExactTrailingZeros() throws AnalysisException {
        // '10.000' for DECIMAL(10,2) should succeed: trailing zeros are exact
        ScalarType dec102 = ScalarType.createDecimalV3Type(10, 2);
        LiteralExpr lit = LiteralExprUtils.createLiteral("10.000", dec102);
        Assert.assertTrue(lit instanceof DecimalLiteral);
        Assert.assertEquals("10.00", lit.getStringValue());
    }

    @Test
    public void testDecimalExactNoExtraScale() throws AnalysisException {
        // '10.00' for DECIMAL(10,2) should succeed
        ScalarType dec102 = ScalarType.createDecimalV3Type(10, 2);
        LiteralExpr lit = LiteralExprUtils.createLiteral("10.00", dec102);
        Assert.assertTrue(lit instanceof DecimalLiteral);
    }

    @Test
    public void testDecimalExactSmallerScale() throws AnalysisException {
        // '10' for DECIMAL(10,2) should succeed (allows widening)
        ScalarType dec102 = ScalarType.createDecimalV3Type(10, 2);
        LiteralExpr lit = LiteralExprUtils.createLiteral("10", dec102);
        Assert.assertTrue(lit instanceof DecimalLiteral);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalRejectRounding() throws AnalysisException {
        // '10.005' for DECIMAL(10,2) should FAIL: would require rounding
        ScalarType dec102 = ScalarType.createDecimalV3Type(10, 2);
        LiteralExprUtils.createLiteral("10.005", dec102);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalRejectRoundingOneDigit() throws AnalysisException {
        // '10.5' for DECIMAL(10,0) should FAIL: would require rounding
        ScalarType dec100 = ScalarType.createDecimalV3Type(10, 0);
        LiteralExprUtils.createLiteral("10.5", dec100);
    }
}
