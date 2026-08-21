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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.SequenceMatch;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopNWeighted;
import org.apache.doris.nereids.trees.expressions.functions.agg.WindowFunnel;
import org.apache.doris.nereids.trees.expressions.functions.agg.WindowFunnelV2;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TimestampNsFunctionSignatureTest {

    private final Expression timestampNs = SlotReference.of("timestamp_ns", TimeStampNsType.INSTANCE);
    private final NereidsParser parser = new NereidsParser();

    @Test
    void testTimestampNsScalarFunctionSignatures() {
        assertSignature(new WeeksAdd(timestampNs, new IntegerLiteral(1)),
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE, IntegerType.INSTANCE);
        assertSignature(new Year(timestampNs), SmallIntType.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new Date(timestampNs), DateV2Type.INSTANCE, TimeStampNsType.INSTANCE);
        assertSignature(new DateFormat(timestampNs, new VarcharLiteral("%Y-%m-%d")),
                VarcharType.SYSTEM_DEFAULT, TimeStampNsType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        assertSignature(new DateTrunc(timestampNs, new VarcharLiteral("day")),
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        assertSignature(new UnixTimestamp(timestampNs),
                DecimalV3Type.createDecimalV3Type(21, 9), TimeStampNsType.INSTANCE);
    }

    @Test
    void testWidthBucketMatchesDateTimeV2Signature() {
        Expression dateTimeV2 = SlotReference.of("datetimev2", DateTimeV2Type.MAX);
        assertSignature(new WidthBucket(timestampNs, timestampNs, timestampNs,
                        new IntegerLiteral(1)),
                BigIntType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE,
                DoubleType.INSTANCE, TinyIntType.INSTANCE);
        assertSignature(new WidthBucket(dateTimeV2, dateTimeV2, dateTimeV2,
                        new IntegerLiteral(1)),
                BigIntType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE,
                DoubleType.INSTANCE, TinyIntType.INSTANCE);
    }

    @Test
    void testNowUsesTimestampNsForNanosecondPrecision() {
        assertAnalyzedType("now(6)", DateTimeV2Type.MAX);
        assertAnalyzedType("now(7)", TimeStampNsType.INSTANCE);
        assertAnalyzedType("now(8)", TimeStampNsType.INSTANCE);
        assertAnalyzedType("now(9)", TimeStampNsType.INSTANCE);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> ExpressionAnalyzer.analyzeFunction(null, null, parser.parseExpression("now(10)")));
    }

    @Test
    void testUntypedDatetimeInputsDoNotSelectTimestampNsSignatures() {
        VarcharLiteral first = new VarcharLiteral("2010-01-01 01:00:00");
        VarcharLiteral second = new VarcharLiteral("2010-01-02 01:00:00");
        assertSignature(new TimeDiff(first, second), TimeV2Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT);
        assertSignature(new HourCeil(first, new IntegerLiteral(1), second),
                DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                IntegerType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);

        CharLiteral charDate = new CharLiteral("2012-12-01", 16);
        assertSignature(new DayOfWeek(charDate), TinyIntType.INSTANCE, DateV2Type.INSTANCE);

        Expression varcharColumn = SlotReference.of("varchar_column", VarcharType.SYSTEM_DEFAULT);
        assertNoTimestampNsSignature(new TimeDiff(varcharColumn, varcharColumn));
        assertNoTimestampNsSignature(new Date(varcharColumn));
        assertNoTimestampNsSignature(new MonthCeil(varcharColumn));
        assertNoTimestampNsSignature(
                new Date(new VarcharLiteral("170141183460469231731687303715884105727")));
        assertSignature(new DayMicrosecondAdd(new VarcharLiteral("2024-07-31"),
                        new VarcharLiteral("0 10:20:30.123456")),
                DateTimeV2Type.MAX, DateTimeV2Type.MAX, VarcharType.SYSTEM_DEFAULT);
        assertSignature(new SecondMicrosecondAdd(new VarcharLiteral("2025-10-29 10:10:10"),
                        new VarcharLiteral("1.1")),
                DateTimeV2Type.MAX, DateTimeV2Type.MAX, VarcharType.SYSTEM_DEFAULT);
    }

    @Test
    void testUntypedDatetimeExpressionsKeepDateTimeV2SignaturesAfterAnalysis() {
        assertAnalyzedType(
                "date_add('2024-07-31', interval '0 10:20:30.123456' day_microsecond)",
                DateTimeV2Type.MAX);
        assertAnalyzedType(
                "date_add('2025-10-29 10:10:10', interval '1.1' second_microsecond)",
                DateTimeV2Type.MAX);
        assertAnalyzedType("ifnull(date(substring('2020-02-09', 1, 1024)), null)",
                DateV2Type.INSTANCE);
    }

    @Test
    void testMixedDateLikeColumnsRequireExplicitCast() {
        Expression datetime = SlotReference.of("datetime", DateTimeV2Type.MAX);
        Expression timestampTz = SlotReference.of("timestamp_tz", TimeStampTzType.MAX);
        Assertions.assertThrows(AnalysisException.class,
                () -> new DateDiff(timestampNs, datetime).getSignature());
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeDiff(timestampNs, datetime).getSignature());
        Assertions.assertThrows(AnalysisException.class,
                () -> new DateDiff(timestampNs, timestampTz).getSignature());
        Assertions.assertThrows(AnalysisException.class,
                () -> new TimeDiff(timestampTz, timestampNs).getSignature());
        Assertions.assertThrows(AnalysisException.class,
                () -> new SecondsDiff(timestampNs, timestampTz).getSignature());
        Assertions.assertThrows(AnalysisException.class,
                () -> new Field(timestampTz,
                        new TimeStampNsLiteral("2024-01-02 03:04:05.123456789")).getSignature());
    }

    @Test
    void testDateTimeV2AndTimestampTzFunctionsUseDateTimeV2Signature() {
        Expression datetime = SlotReference.of("datetime", DateTimeV2Type.of(3));
        Expression timestampTz = SlotReference.of("timestamp_tz", TimeStampTzType.MAX);
        assertSignature(new DateDiff(datetime, timestampTz), IntegerType.INSTANCE,
                DateTimeV2Type.MAX, DateTimeV2Type.MAX);
        assertSignature(new TimeDiff(timestampTz, datetime), TimeV2Type.MAX,
                DateTimeV2Type.MAX, DateTimeV2Type.MAX);
        assertSignature(new SecondsDiff(datetime, timestampTz), BigIntType.INSTANCE,
                DateTimeV2Type.MAX, DateTimeV2Type.MAX);
    }

    @Test
    void testMixedDateTimeV2LiteralUsesTimestampNsOnlyWhenRepresentable() {
        DateTimeV2Literal insideRange = new DateTimeV2Literal(DateTimeV2Type.MAX,
                2024, 1, 2, 3, 4, 5, 123456);
        DateTimeV2Literal outsideRange = new DateTimeV2Literal(DateTimeV2Type.MAX,
                2500, 1, 2, 3, 4, 5, 123456);

        assertSignature(new DateDiff(timestampNs, insideRange), IntegerType.INSTANCE,
                TimeStampNsType.INSTANCE, TimeStampNsType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class,
                () -> new DateDiff(timestampNs, outsideRange).getSignature());

        Expression datetime = SlotReference.of("datetime", DateTimeV2Type.MAX);
        TimeStampNsLiteral exactTimestampNs = new TimeStampNsLiteral(
                "2024-01-02 03:04:05.123456000");
        TimeStampNsLiteral inexactTimestampNs = new TimeStampNsLiteral(
                "2024-01-02 03:04:05.123456001");
        assertSignature(new DateDiff(datetime, exactTimestampNs), IntegerType.INSTANCE,
                DateTimeV2Type.MAX, DateTimeV2Type.MAX);
        Expression exactTimestampNsCast = new Cast(
                new VarcharLiteral("2024-01-02 03:04:05.123456000"), TimeStampNsType.INSTANCE);
        Expression coerced = TypeCoercionUtils.processBoundFunction(
                new DateDiff(datetime, exactTimestampNsCast));
        Assertions.assertEquals(DateTimeV2Type.MAX, coerced.child(1).getDataType());
        Assertions.assertTrue(coerced.checkInputDataTypes().success());
        Assertions.assertThrows(AnalysisException.class,
                () -> new DateDiff(datetime, inexactTimestampNs).getSignature());

        Assertions.assertThrows(AnalysisException.class,
                () -> new SecondFloor(timestampNs, datetime).getSignature());
        Assertions.assertThrows(AnalysisException.class,
                () -> new ArrayRange(timestampNs, datetime).getSignature());
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
        assertSignature(new TopNWeighted(timestampNs, weight,
                        new IntegerLiteral(2), new IntegerLiteral(100)),
                ArrayType.of(TimeStampNsType.INSTANCE), TimeStampNsType.INSTANCE,
                BigIntType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
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

    private void assertNoTimestampNsSignature(BoundFunction function) {
        FunctionSignature signature = function.getSignature();
        Assertions.assertFalse(signature.returnType.isTimeStampNsType(), function.getName());
        Assertions.assertTrue(signature.argumentsTypes.stream().noneMatch(DataType::isTimeStampNsType),
                function.getName());
    }

    private void assertAnalyzedType(String sql, DataType expectedType) {
        Expression analyzed = ExpressionAnalyzer.analyzeFunction(null, null, parser.parseExpression(sql));
        String details = analyzed instanceof BoundFunction
                ? sql + ": " + analyzed + ", signature=" + ((BoundFunction) analyzed).getSignature()
                : sql + ": " + analyzed;
        Assertions.assertEquals(expectedType, analyzed.getDataType(), details);
        Assertions.assertTrue(analyzed.checkInputDataTypes().success(), sql);
    }
}
