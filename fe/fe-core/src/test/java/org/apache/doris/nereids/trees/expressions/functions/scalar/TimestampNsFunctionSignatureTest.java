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
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceMatch;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNWeighted;
import org.apache.doris.nereids.trees.expressions.functions.agg.WindowFunnel;
import org.apache.doris.nereids.trees.expressions.functions.agg.WindowFunnelV2;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VarcharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TimestampNsFunctionSignatureTest {

    private final Expression timestampNs = SlotReference.of("timestamp_ns", TimeStampNsType.INSTANCE);

    @Test
    void testTimestampNsUsesDedicatedScalarFunctionSignatures() {
        assertSignature(new WeeksAdd(timestampNs, new IntegerLiteral(1)),
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE, IntegerType.INSTANCE);
        assertSignature(new Year(timestampNs), SmallIntType.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new Date(timestampNs), DateV2Type.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new DateFormat(timestampNs, new VarcharLiteral("%Y-%m-%d")),
                VarcharType.SYSTEM_DEFAULT, TimeStampNsType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        assertSignature(new DateTrunc(timestampNs, new VarcharLiteral("day")),
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
    }

    @Test
    void testMixedDateTimeV2DiffPromotesToTimestampNsSignature() {
        Expression datetime = SlotReference.of("datetime", DateTimeV2Type.MAX);
        assertSignature(new DateDiff(timestampNs, datetime), IntegerType.INSTANCE,
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new TimeDiff(timestampNs, datetime), TimeV2Type.MAX,
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
    }

    @Test
    void testMixedDatetimeArgumentsNeverDemoteTimestampNs() {
        Expression datetime = SlotReference.of("datetime", DateTimeV2Type.MAX);
        assertSignature(new SecondFloor(timestampNs, datetime), TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new ArrayRange(timestampNs, datetime),
                ArrayType.of(TimeStampNsType.INSTANCE),
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new Field(timestampNs, datetime), IntegerType.INSTANCE,
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
    }

    @Test
    void testMicrosecondArithmeticDoesNotRewriteTimestampNsToDatetimeV2() {
        VarcharLiteral interval = new VarcharLiteral("1.000001");
        assertTimestampNsBinary(new MicroSecondsAdd(timestampNs, new IntegerLiteral(1)));
        assertTimestampNsBinary(new MicroSecondsSub(timestampNs, new IntegerLiteral(1)));
        assertTimestampNsBinary(new MilliSecondsAdd(timestampNs, new IntegerLiteral(1)));
        assertTimestampNsBinary(new MilliSecondsSub(timestampNs, new IntegerLiteral(1)));
        assertTimestampNsStringBinary(new DayMicrosecondAdd(timestampNs, interval));
        assertTimestampNsStringBinary(new DayMicrosecondSub(timestampNs, interval));
        assertTimestampNsStringBinary(new HourMicrosecondAdd(timestampNs, interval));
        assertTimestampNsStringBinary(new HourMicrosecondSub(timestampNs, interval));
        assertTimestampNsStringBinary(new MinuteMicrosecondAdd(timestampNs, interval));
        assertTimestampNsStringBinary(new MinuteMicrosecondSub(timestampNs, interval));
        assertTimestampNsStringBinary(new SecondMicrosecondAdd(timestampNs, interval));
        assertTimestampNsStringBinary(new SecondMicrosecondSub(timestampNs, interval));
    }

    @Test
    void testTimeAndTimeArithmeticSignatures() {
        Expression time = SlotReference.of("time", TimeV2Type.MAX);
        assertSignature(new Time(timestampNs), TimeV2Type.MAX, TimeStampNsType.INSTANCE);
        assertSignature(new AddTime(timestampNs, time), TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE, TimeV2Type.MAX);
        assertSignature(new SubTime(timestampNs, time), TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE, TimeV2Type.MAX);
        assertSignature(new ConvertTz(timestampNs, new VarcharLiteral("+08:00"),
                        new VarcharLiteral("+00:00")),
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE,
                VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
    }

    @Test
    void testTimestampNsAggregateFunctionSignatures() {
        Expression condition1 = SlotReference.of("condition1", BooleanType.INSTANCE);
        Expression condition2 = SlotReference.of("condition2", BooleanType.INSTANCE);
        Expression weight = SlotReference.of("weight", BigIntType.INSTANCE);
        VarcharLiteral pattern = new VarcharLiteral("(?1)(?2)");
        VarcharLiteral mode = new VarcharLiteral("default");

        assertSignature(new SequenceCount(pattern, timestampNs, condition1, condition2),
                BigIntType.INSTANCE, StringType.INSTANCE,
                TimeStampNsType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        assertSignature(new SequenceMatch(pattern, timestampNs, condition1, condition2),
                BooleanType.INSTANCE, StringType.INSTANCE,
                TimeStampNsType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        assertSignature(new WindowFunnel(new BigIntLiteral(3600), mode,
                        timestampNs, condition1, condition2),
                IntegerType.INSTANCE, BigIntType.INSTANCE, StringType.INSTANCE,
                TimeStampNsType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        assertSignature(new WindowFunnelV2(new BigIntLiteral(3600), mode,
                        timestampNs, condition1, condition2),
                IntegerType.INSTANCE, BigIntType.INSTANCE, StringType.INSTANCE,
                TimeStampNsType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        assertSignature(new TopNWeighted(timestampNs, weight, new IntegerLiteral(2)),
                ArrayType.of(TimeStampNsType.INSTANCE), TimeStampNsType.INSTANCE,
                BigIntType.INSTANCE, IntegerType.INSTANCE);
    }

    private void assertTimestampNsBinary(ScalarFunction function) {
        assertSignature(function, TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE, BigIntType.INSTANCE);
    }

    private void assertTimestampNsStringBinary(ScalarFunction function) {
        assertSignature(function, TimeStampNsType.INSTANCE,
                TimeStampNsType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
    }

    private void assertSignature(BoundFunction function, DataType returnType, DataType... argumentTypes) {
        FunctionSignature signature = function.getSignature();
        Assertions.assertEquals(returnType, signature.returnType, function.getName());
        Assertions.assertArrayEquals(argumentTypes, signature.argumentsTypes.toArray(), function.getName());
    }
}
