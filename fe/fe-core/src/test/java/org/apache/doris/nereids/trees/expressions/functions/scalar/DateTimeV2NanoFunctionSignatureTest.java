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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DateTimeV2NanoFunctionSignatureTest {

    @Test
    void testMicrosecondsAddUsesLegacyScaleForStringsAndPreservesNanoScale() {
        MicroSecondsAdd stringInput =
                new MicroSecondsAdd(new StringLiteral("0000-01-05"), new BigIntLiteral(1));
        FunctionSignature stringSignature =
                stringInput.computeSignature(stringInput.getSignatures().get(1));
        Assertions.assertEquals(DateTimeV2Type.of(6), stringSignature.argumentsTypes.get(0));
        Assertions.assertEquals(DateTimeV2Type.of(6), stringSignature.returnType);

        MicroSecondsAdd nanoInput = new MicroSecondsAdd(
                new DateTimeV2Literal(DateTimeV2Type.of(9), "1970-01-01 00:00:00.000000001"),
                new BigIntLiteral(1));
        FunctionSignature nanoSignature =
                nanoInput.computeSignature(nanoInput.getSignatures().get(0));
        Assertions.assertEquals(DateTimeV2Type.of(9), nanoSignature.argumentsTypes.get(0));
        Assertions.assertEquals(DateTimeV2Type.of(9), nanoSignature.returnType);
    }

    @Test
    void testTimeScaleIsClampedToTimeV2Maximum() {
        Assertions.assertEquals(TimeV2Type.MAX, TimeV2Type.forType(DateTimeV2Type.of(7)));
        Assertions.assertEquals(TimeV2Type.MAX, TimeV2Type.forType(DateTimeV2Type.of(9)));
    }

    @Test
    void testMicrosecondArithmeticPreservesNanoScaleWithoutWideningLegacyInputs() {
        Assertions.assertEquals(DateTimeV2Type.of(6),
                DateTimeV2Type.forTypeWithMinimumScale(DateTimeV2Type.of(3), 6));
        Assertions.assertEquals(DateTimeV2Type.of(7),
                DateTimeV2Type.forTypeWithMinimumScale(DateTimeV2Type.of(7), 6));
        Assertions.assertEquals(DateTimeV2Type.of(9),
                DateTimeV2Type.forTypeWithMinimumScale(DateTimeV2Type.of(9), 6));

        MicroSecondsAdd microSecondsAdd = new MicroSecondsAdd(
                new DateTimeV2Literal(DateTimeV2Type.of(9), "1970-01-01 00:00:00.000000001"),
                new IntegerLiteral(1));
        FunctionSignature signature =
                microSecondsAdd.computeSignature(microSecondsAdd.getSignatures().get(0));
        Assertions.assertEquals(DateTimeV2Type.of(9), signature.returnType);
    }

    @Test
    void testTimeDiffScaleIsClampedToTimeV2Maximum() {
        TimeDiff timeDiff = new TimeDiff(
                new DateTimeV2Literal(DateTimeV2Type.of(9), "1970-01-01 00:00:00.000000001"),
                new DateTimeV2Literal(DateTimeV2Type.of(7), "1970-01-01 00:00:00.0000000"));
        FunctionSignature signature = timeDiff.computeSignature(timeDiff.getSignatures().get(1));

        Assertions.assertEquals(TimeV2Type.MAX, signature.returnType);
    }

    @Test
    void testUtcTimestampAcceptsNanosecondScale() {
        UtcTimestamp utcTimestamp = new UtcTimestamp(new IntegerLiteral(9));
        FunctionSignature signature =
                utcTimestamp.computeSignature(utcTimestamp.getSignatures().get(1));

        Assertions.assertEquals(DateTimeV2Type.of(9), signature.returnType);
        Assertions.assertThrows(AnalysisException.class,
                () -> new UtcTimestamp(new IntegerLiteral(10))
                        .computeSignature(UtcTimestamp.SIGNATURES.get(1)));
    }

    @Test
    void testToDaysHasNativeDateTimeV2Signature() {
        ToDays toDays =
                new ToDays(new DateTimeV2Literal("1970-01-01 00:00:00.000000000"));

        Assertions.assertEquals(DateTimeV2Type.WILDCARD,
                toDays.getSignatures().get(0).argumentsTypes.get(0));
    }
}
