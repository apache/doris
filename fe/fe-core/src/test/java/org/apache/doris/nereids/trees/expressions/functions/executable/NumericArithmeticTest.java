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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class NumericArithmeticTest {

    @Test
    public void testDecimalV3Abs() {
        DecimalV3Literal decimalV3Literal = new DecimalV3Literal(
                DecimalV3Type.createDecimalV3Type(10, 0), new BigDecimal(1));
        DecimalV3Literal result = (DecimalV3Literal) NumericArithmetic.abs(decimalV3Literal);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(10, 0), result.getDataType());
    }

    @Test
    public void testDecimalV2Abs() {
        DecimalLiteral decimalV3Literal = new DecimalLiteral(
                DecimalV2Type.createDecimalV2Type(10, 0), new BigDecimal(1));
        DecimalLiteral result = (DecimalLiteral) NumericArithmetic.abs(decimalV3Literal);
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(10, 0), result.getDataType());
    }
}
