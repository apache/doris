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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DateTimeMicrosecondFunctionTest {

    @Test
    void testDateTimeV2MicrosecondArithmeticMinimumScale() {
        assertDateTimeV2Scale(0, DateTimeV2Type.MAX_SCALE);
        assertDateTimeV2Scale(6, DateTimeV2Type.MAX_SCALE);
    }

    @Test
    void testTimestampTzMicrosecondArithmeticScale() {
        Expression timestamp = new SlotReference("timestamp", TimeStampTzType.of(3));
        for (ScalarFunction function : createFunctions(timestamp)) {
            FunctionSignature signature = function.getSignature();
            Assertions.assertEquals(TimeStampTzType.MAX, signature.getArgType(0), function.getName());
            Assertions.assertEquals(TimeStampTzType.MAX, signature.returnType, function.getName());
        }
    }

    @Test
    void testTimestampTzStringLiteralKeepsTimezoneType() {
        SecondMicrosecondAdd function = new SecondMicrosecondAdd(
                new VarcharLiteral("2004-11-30 01:00:00+08:00"), new VarcharLiteral("10.123456"));
        FunctionSignature signature = function.getSignature();
        Assertions.assertEquals(TimeStampTzType.MAX, signature.getArgType(0));
        Assertions.assertEquals(TimeStampTzType.MAX, signature.returnType);
    }

    @Test
    void testImplicitStringDateTimeFunctionsKeepMicrosecondPrecision() {
        Expression stringSlot = new SlotReference("string", StringType.INSTANCE);
        Expression nullableString = new Nullable(new VarcharLiteral("9999-01-01 00:00:00"));

        assertDateTimeV2MicrosecondSignature(new AddTime(stringSlot,
                new SlotReference("time", TimeV2Type.MAX)), 0);
        assertDateTimeV2MicrosecondSignature(new DateFormat(stringSlot,
                new VarcharLiteral("%Y-%m-%d")), 0);
        assertDateTimeV2MicrosecondSignature(new ToSeconds(stringSlot), 0);
        assertDateTimeV2MicrosecondSignature(new YearsAdd(nullableString, new IntegerLiteral(-1)), 0);
        assertDateTimeV2MicrosecondSignature(new Timestamp(
                new VarcharLiteral("1st Jun 2007 09:45:30")), 0);

        FunctionSignature timeSignature = new Time(nullableString).getSignature();
        Assertions.assertEquals(DateTimeV2Type.MAX, timeSignature.getArgType(0));
        Assertions.assertEquals(TimeV2Type.MAX, timeSignature.returnType);
    }

    private void assertDateTimeV2MicrosecondSignature(ScalarFunction function, int argumentIndex) {
        FunctionSignature signature = function.getSignature();
        Assertions.assertEquals(DateTimeV2Type.MAX,
                signature.getArgType(argumentIndex), function.getName());
        if (signature.returnType instanceof DateTimeV2Type) {
            Assertions.assertEquals(DateTimeV2Type.MAX,
                    signature.returnType, function.getName());
        }
    }

    private void assertDateTimeV2Scale(int inputScale, int expectedScale) {
        Expression datetime = new SlotReference("datetime", DateTimeV2Type.of(inputScale));
        DataType expectedType = DateTimeV2Type.of(expectedScale);
        for (ScalarFunction function : createFunctions(datetime)) {
            FunctionSignature signature = function.getSignature();
            Assertions.assertEquals(expectedType, signature.getArgType(0), function.getName());
            Assertions.assertEquals(expectedType, signature.returnType, function.getName());
        }
    }

    private List<ScalarFunction> createFunctions(Expression datetime) {
        BigIntLiteral numericInterval = new BigIntLiteral(1);
        VarcharLiteral compositeInterval = new VarcharLiteral("1");
        return List.of(
                new MicroSecondsAdd(datetime, numericInterval),
                new MicroSecondsSub(datetime, numericInterval),
                new MilliSecondsAdd(datetime, numericInterval),
                new MilliSecondsSub(datetime, numericInterval),
                new DayMicrosecondAdd(datetime, compositeInterval),
                new DayMicrosecondSub(datetime, compositeInterval),
                new HourMicrosecondAdd(datetime, compositeInterval),
                new HourMicrosecondSub(datetime, compositeInterval),
                new MinuteMicrosecondAdd(datetime, compositeInterval),
                new MinuteMicrosecondSub(datetime, compositeInterval),
                new SecondMicrosecondAdd(datetime, compositeInterval),
                new SecondMicrosecondSub(datetime, compositeInterval));
    }
}
