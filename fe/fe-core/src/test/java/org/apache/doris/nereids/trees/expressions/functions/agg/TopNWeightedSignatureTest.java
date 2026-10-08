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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopNWeightedSignatureTest {
    @Test
    void testDateTimePrecision() {
        for (int scale = 0; scale <= 6; scale++) {
            assertReturnType(DateTimeV2Type.of(scale));
        }
    }

    @Test
    void testDecimalV2() {
        assertReturnType(DecimalV2Type.CATALOG_DEFAULT);
    }

    @Test
    void testAllOverloadsReturnInputArray() {
        for (FunctionSignature signature : TopNWeighted.SIGNATURES) {
            Assertions.assertEquals(ArrayType.of(signature.getArgType(0)), signature.returnType,
                    signature.toString());
        }
    }

    private void assertReturnType(DataType inputType) {
        Expression value = SlotReference.of("value", inputType);
        TopNWeighted threeArguments = new TopNWeighted(value, new BigIntLiteral(1), new IntegerLiteral(1));
        TopNWeighted fourArguments = new TopNWeighted(value, new BigIntLiteral(1),
                new IntegerLiteral(1), new IntegerLiteral(100));
        for (TopNWeighted function : new TopNWeighted[] {threeArguments, fourArguments}) {
            FunctionSignature signature = function.getSignature();
            Assertions.assertEquals(inputType, signature.getArgType(0));
            Assertions.assertEquals(ArrayType.of(inputType), signature.returnType);
        }
    }
}
