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
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.TimeZone;

public class LiteralExprCompareTest {

    @BeforeClass
    public static void setUp() {
        TimeZone tz = TimeZone.getTimeZone("ETC/GMT-0");
        TimeZone.setDefault(tz);
    }

    @Test
    public void boolTest() {
        LiteralExpr boolTrue1 = new BoolLiteral(true);
        LiteralExpr boolFalse1 = new BoolLiteral(false);
        LiteralExpr boolTrue2 = new BoolLiteral(true);

        // value equal
        Assert.assertEquals(boolTrue1, boolTrue2);
        // self equal
        Assert.assertEquals(boolTrue1, boolTrue1);

        // value compare
        Assert.assertTrue(!boolTrue1.equals(boolFalse1) && 1 == boolTrue1.compareLiteral(boolFalse1));
        Assert.assertTrue(-1 == boolFalse1.compareLiteral(boolTrue1));
        // value equal
        Assert.assertTrue(0 == boolTrue1.compareLiteral(boolTrue2));
        // self equal
        Assert.assertTrue(0 == boolTrue1.compareLiteral(boolTrue1));
    }

    @Test(expected = AnalysisException.class)
    public void dateFormat1Test() throws AnalysisException {
        new DateLiteral("2015-02-15 12:12:12", ScalarType.DATE);
        Assert.fail();
    }

    @Test(expected = AnalysisException.class)
    public void dateV2Format1Test() throws AnalysisException {
        new DateLiteral("2015-02-15 12:12:12", ScalarType.DATEV2);
        Assert.fail();
    }

    @Test(expected = AnalysisException.class)
    public void dateFormat2Test() throws AnalysisException {
        new DateLiteral("2015-02-15", ScalarType.DATETIME);
        Assert.fail();
    }

    @Test(expected = AnalysisException.class)
    public void dateV2Format2Test() throws AnalysisException {
        new DateLiteral("2015-02-15", ScalarType.DATETIMEV2);
        Assert.fail();
    }

    @Test
    public void dateTest() throws AnalysisException {
        LiteralExpr date1 = new DateLiteral("2015-02-15", ScalarType.DATE);
        LiteralExpr date1Same = new DateLiteral("2015-02-15", ScalarType.DATE);
        LiteralExpr date1Large = new DateLiteral("2015-02-16", ScalarType.DATE);
        LiteralExpr datetime1 = new DateLiteral("2015-02-15 13:14:00", ScalarType.DATETIME);
        LiteralExpr datetime1Same = new DateLiteral("2015-02-15 13:14:00", ScalarType.DATETIME);
        LiteralExpr datetime1Large = new DateLiteral("2015-02-15 13:14:15", ScalarType.DATETIME);

        // infinity
        LiteralExpr maxDate1 = new DateLiteral(ScalarType.DATE, true);
        LiteralExpr maxDate1Same = new DateLiteral(ScalarType.DATE, true);
        LiteralExpr minDate1 = new DateLiteral(ScalarType.DATE, false);
        LiteralExpr minDate1Same = new DateLiteral(ScalarType.DATE, false);
        LiteralExpr maxDatetime1 = new DateLiteral(ScalarType.DATETIME, true);
        LiteralExpr maxDatetime1Same = new DateLiteral(ScalarType.DATETIME, true);
        LiteralExpr minDatetime1 = new DateLiteral(ScalarType.DATETIME, false);
        LiteralExpr minDatetime1Same = new DateLiteral(ScalarType.DATETIME, false);
        LiteralExpr date8 = new DateLiteral("9999-12-31", ScalarType.DATE);
        LiteralExpr date9 = new DateLiteral("9999-12-31 23:59:59", ScalarType.DATETIME);
        LiteralExpr date10 = new DateLiteral("0000-01-01", ScalarType.DATE);
        LiteralExpr date11 = new DateLiteral("0000-01-01 00:00:00", ScalarType.DATETIME);

        Assert.assertTrue(date1.equals(date1Same) && date1.compareLiteral(date1Same) == 0);
        Assert.assertTrue(date1.equals(date1Same) && date1.compareLiteral(date1Same) == 0);
        Assert.assertTrue(datetime1.equals(datetime1Same) && datetime1.compareLiteral(datetime1Same) == 0);
        Assert.assertTrue(datetime1.equals(datetime1) && datetime1.compareLiteral(datetime1) == 0);

        // value compare
        Assert.assertTrue(!date1Large.equals(date1Same) && 1 == date1Large.compareLiteral(date1Same));
        Assert.assertTrue(!datetime1Large.equals(datetime1Same) && 1 == datetime1Large.compareLiteral(datetime1Same));
        Assert.assertTrue(!datetime1Same.equals(datetime1Large) && -1 == datetime1Same.compareLiteral(datetime1Large));

        // infinity
        Assert.assertTrue(maxDate1.equals(maxDate1) && maxDate1.compareLiteral(maxDate1) == 0);
        Assert.assertTrue(maxDate1.equals(maxDate1Same) && maxDate1.compareLiteral(maxDate1Same) == 0);
        Assert.assertTrue(minDate1.equals(minDate1) && minDate1.compareLiteral(minDate1) == 0);
        Assert.assertTrue(minDate1.equals(minDate1Same) && minDate1.compareLiteral(minDate1Same) == 0);
        Assert.assertTrue(maxDatetime1.equals(maxDatetime1) && maxDatetime1.compareLiteral(maxDatetime1) == 0);
        Assert.assertTrue(maxDatetime1.equals(maxDatetime1Same) && maxDatetime1.compareLiteral(maxDatetime1Same) == 0);
        Assert.assertTrue(minDatetime1.equals(minDatetime1) && minDatetime1.compareLiteral(minDatetime1) == 0);
        Assert.assertTrue(minDatetime1.equals(minDatetime1Same) && minDatetime1.compareLiteral(minDatetime1Same) == 0);

        Assert.assertTrue(maxDate1.equals(date8) && maxDate1.compareLiteral(date8) == 0);
        Assert.assertTrue(minDate1.equals(date10) && minDate1.compareLiteral(date10) == 0);
        Assert.assertTrue(maxDatetime1.equals(date9) && maxDatetime1.compareLiteral(date9) == 0);
        Assert.assertTrue(minDatetime1.equals(date11) && minDatetime1.compareLiteral(date11) == 0);

        Assert.assertTrue(!maxDate1.equals(date1) && maxDate1.compareLiteral(date1) > 0);
        Assert.assertTrue(!minDate1.equals(date1) && minDate1.compareLiteral(date1) < 0);
        Assert.assertTrue(!maxDatetime1.equals(datetime1) && maxDatetime1.compareLiteral(datetime1) > 0);
        Assert.assertTrue(!minDatetime1.equals(datetime1) && minDatetime1.compareLiteral(datetime1) < 0);
    }

    @Test
    public void dateV2Test() throws AnalysisException {
        LiteralExpr date1 = new DateLiteral("2015-02-15", ScalarType.DATEV2);
        LiteralExpr date1Same = new DateLiteral("2015-02-15", ScalarType.DATEV2);
        LiteralExpr date1Large = new DateLiteral("2015-02-16", ScalarType.DATEV2);
        LiteralExpr datetime1 = new DateLiteral("2015-02-15 13:14:00", ScalarType.DATETIMEV2);
        LiteralExpr datetime1Same = new DateLiteral("2015-02-15 13:14:00", ScalarType.DATETIMEV2);
        LiteralExpr datetime1Large = new DateLiteral("2015-02-15 13:14:15", ScalarType.DATETIMEV2);

        // infinity
        LiteralExpr maxDate1 = new DateLiteral(ScalarType.DATEV2, true);
        LiteralExpr maxDate1Same = new DateLiteral(ScalarType.DATEV2, true);
        LiteralExpr minDate1 = new DateLiteral(ScalarType.DATEV2, false);
        LiteralExpr minDate1Same = new DateLiteral(ScalarType.DATEV2, false);
        LiteralExpr maxDatetime1 = new DateLiteral(ScalarType.DATETIMEV2, true);
        LiteralExpr maxDatetime1Same = new DateLiteral(ScalarType.DATETIMEV2, true);
        LiteralExpr minDatetime1 = new DateLiteral(ScalarType.DATETIMEV2, false);
        LiteralExpr minDatetime1Same = new DateLiteral(ScalarType.DATETIMEV2, false);
        LiteralExpr date8 = new DateLiteral("9999-12-31", ScalarType.DATEV2);
        LiteralExpr date9 = new DateLiteral("9999-12-31 23:59:59", ScalarType.DATETIMEV2);
        LiteralExpr date10 = new DateLiteral("0000-01-01", ScalarType.DATEV2);
        LiteralExpr date11 = new DateLiteral("0000-01-01 00:00:00", ScalarType.DATETIMEV2);

        Assert.assertTrue(date1.equals(date1Same) && date1.compareLiteral(date1Same) == 0);
        Assert.assertTrue(date1.equals(date1Same) && date1.compareLiteral(date1Same) == 0);
        Assert.assertTrue(datetime1.equals(datetime1Same) && datetime1.compareLiteral(datetime1Same) == 0);
        Assert.assertTrue(datetime1.equals(datetime1) && datetime1.compareLiteral(datetime1) == 0);

        // value compare
        Assert.assertTrue(!date1Large.equals(date1Same) && 1 == date1Large.compareLiteral(date1Same));
        Assert.assertTrue(!datetime1Large.equals(datetime1Same) && 1 == datetime1Large.compareLiteral(datetime1Same));
        Assert.assertTrue(!datetime1Same.equals(datetime1Large) && -1 == datetime1Same.compareLiteral(datetime1Large));

        // infinity
        Assert.assertTrue(maxDate1.equals(maxDate1) && maxDate1.compareLiteral(maxDate1) == 0);
        Assert.assertTrue(maxDate1.equals(maxDate1Same) && maxDate1.compareLiteral(maxDate1Same) == 0);
        Assert.assertTrue(minDate1.equals(minDate1) && minDate1.compareLiteral(minDate1) == 0);
        Assert.assertTrue(minDate1.equals(minDate1Same) && minDate1.compareLiteral(minDate1Same) == 0);
        Assert.assertTrue(maxDatetime1.equals(maxDatetime1) && maxDatetime1.compareLiteral(maxDatetime1) == 0);
        Assert.assertTrue(maxDatetime1.equals(maxDatetime1Same) && maxDatetime1.compareLiteral(maxDatetime1Same) == 0);
        Assert.assertTrue(minDatetime1.equals(minDatetime1) && minDatetime1.compareLiteral(minDatetime1) == 0);
        Assert.assertTrue(minDatetime1.equals(minDatetime1Same) && minDatetime1.compareLiteral(minDatetime1Same) == 0);

        Assert.assertTrue(maxDate1.equals(date8) && maxDate1.compareLiteral(date8) == 0);
        Assert.assertTrue(minDate1.equals(date10) && minDate1.compareLiteral(date10) == 0);
        Assert.assertTrue(maxDatetime1.equals(date9) && maxDatetime1.compareLiteral(date9) == 0);
        Assert.assertTrue(minDatetime1.equals(date11) && minDatetime1.compareLiteral(date11) == 0);

        Assert.assertTrue(!maxDate1.equals(date1) && maxDate1.compareLiteral(date1) > 0);
        Assert.assertTrue(!minDate1.equals(date1) && minDate1.compareLiteral(date1) < 0);
        Assert.assertTrue(!maxDatetime1.equals(datetime1) && maxDatetime1.compareLiteral(datetime1) > 0);
        Assert.assertTrue(!minDatetime1.equals(datetime1) && minDatetime1.compareLiteral(datetime1) < 0);
    }

    @Test
    public void dateCompatibilityTest() throws AnalysisException {
        LiteralExpr date1 = new DateLiteral("2015-02-15", ScalarType.DATEV2);
        LiteralExpr date1Same = new DateLiteral("2015-02-15", ScalarType.DATEV2);
        LiteralExpr date1Large = new DateLiteral("2015-02-16", ScalarType.DATEV2);
        LiteralExpr datetime1 = new DateLiteral("2015-02-15 13:14:00", ScalarType.DATETIMEV2);
        LiteralExpr datetime1Same = new DateLiteral("2015-02-15 13:14:00", ScalarType.DATETIMEV2);
        LiteralExpr datetime1Large = new DateLiteral("2015-02-15 13:14:15", ScalarType.DATETIMEV2);

        // infinity
        LiteralExpr maxDate1 = new DateLiteral(ScalarType.DATE, true);
        LiteralExpr maxDate1Same = new DateLiteral(ScalarType.DATE, true);
        LiteralExpr minDate1 = new DateLiteral(ScalarType.DATE, false);
        LiteralExpr minDate1Same = new DateLiteral(ScalarType.DATE, false);
        LiteralExpr maxDatetime1 = new DateLiteral(ScalarType.DATETIME, true);
        LiteralExpr maxDatetime1Same = new DateLiteral(ScalarType.DATETIME, true);
        LiteralExpr minDatetime1 = new DateLiteral(ScalarType.DATETIME, false);
        LiteralExpr minDatetime1Same = new DateLiteral(ScalarType.DATETIME, false);
        LiteralExpr date8 = new DateLiteral("9999-12-31", ScalarType.DATEV2);
        LiteralExpr date9 = new DateLiteral("9999-12-31 23:59:59", ScalarType.DATETIMEV2);
        LiteralExpr date10 = new DateLiteral("0000-01-01", ScalarType.DATEV2);
        LiteralExpr date11 = new DateLiteral("0000-01-01 00:00:00", ScalarType.DATETIMEV2);

        Assert.assertTrue(date1.equals(date1Same) && date1.compareLiteral(date1Same) == 0);
        Assert.assertTrue(date1.equals(date1Same) && date1.compareLiteral(date1Same) == 0);
        Assert.assertTrue(datetime1.equals(datetime1Same) && datetime1.compareLiteral(datetime1Same) == 0);
        Assert.assertTrue(datetime1.equals(datetime1) && datetime1.compareLiteral(datetime1) == 0);

        // value compare
        Assert.assertTrue(!date1Large.equals(date1Same) && 1 == date1Large.compareLiteral(date1Same));
        Assert.assertTrue(!datetime1Large.equals(datetime1Same) && 1 == datetime1Large.compareLiteral(datetime1Same));
        Assert.assertTrue(!datetime1Same.equals(datetime1Large) && -1 == datetime1Same.compareLiteral(datetime1Large));

        // infinity
        Assert.assertTrue(maxDate1.equals(maxDate1) && maxDate1.compareLiteral(maxDate1) == 0);
        Assert.assertTrue(maxDate1.equals(maxDate1Same) && maxDate1.compareLiteral(maxDate1Same) == 0);
        Assert.assertTrue(minDate1.equals(minDate1) && minDate1.compareLiteral(minDate1) == 0);
        Assert.assertTrue(minDate1.equals(minDate1Same) && minDate1.compareLiteral(minDate1Same) == 0);
        Assert.assertTrue(maxDatetime1.equals(maxDatetime1) && maxDatetime1.compareLiteral(maxDatetime1) == 0);
        Assert.assertTrue(maxDatetime1.equals(maxDatetime1Same) && maxDatetime1.compareLiteral(maxDatetime1Same) == 0);
        Assert.assertTrue(minDatetime1.equals(minDatetime1) && minDatetime1.compareLiteral(minDatetime1) == 0);
        Assert.assertTrue(minDatetime1.equals(minDatetime1Same) && minDatetime1.compareLiteral(minDatetime1Same) == 0);

        Assert.assertTrue(maxDate1.equals(date8) && maxDate1.compareLiteral(date8) == 0);
        Assert.assertTrue(minDate1.equals(date10) && minDate1.compareLiteral(date10) == 0);
        Assert.assertTrue(maxDatetime1.equals(date9) && maxDatetime1.compareLiteral(date9) == 0);
        Assert.assertTrue(minDatetime1.equals(date11) && minDatetime1.compareLiteral(date11) == 0);

        Assert.assertTrue(!maxDate1.equals(date1) && maxDate1.compareLiteral(date1) > 0);
        Assert.assertTrue(!minDate1.equals(date1) && minDate1.compareLiteral(date1) < 0);
        Assert.assertTrue(!maxDatetime1.equals(datetime1) && maxDatetime1.compareLiteral(datetime1) > 0);
        Assert.assertTrue(!minDatetime1.equals(datetime1) && minDatetime1.compareLiteral(datetime1) < 0);
    }

    @Test
    public void decimalTest() throws AnalysisException {
        LiteralExpr decimal1 = new DecimalLiteral("1.23456");
        LiteralExpr decimal2 = new DecimalLiteral("1.23456");
        LiteralExpr decimal3 = new DecimalLiteral("1.23457");
        LiteralExpr decimal4 = new DecimalLiteral("2.23457");

        // value equal
        Assert.assertEquals(decimal1, decimal2);
        // self equal
        Assert.assertEquals(decimal1, decimal1);

        // value compare
        Assert.assertTrue(!decimal3.equals(decimal2) && 1 == decimal3.compareLiteral(decimal2));
        Assert.assertTrue(!decimal4.equals(decimal3) && 1 == decimal4.compareLiteral(decimal3));
        Assert.assertTrue(!decimal1.equals(decimal4) && -1 == decimal1.compareLiteral(decimal4));
        // value equal
        Assert.assertTrue(0 == decimal1.compareLiteral(decimal2));
        // self equal
        Assert.assertTrue(0 == decimal1.compareLiteral(decimal1));
    }

    public void floatAndDoubleExpr() {
        LiteralExpr float1 = new FloatLiteral(1.12345, ScalarType.FLOAT);
        LiteralExpr float2 = new FloatLiteral(1.12345, ScalarType.FLOAT);
        LiteralExpr float3 = new FloatLiteral(1.12346, ScalarType.FLOAT);
        LiteralExpr float4 = new FloatLiteral(2.12345, ScalarType.FLOAT);

        LiteralExpr double1 = new FloatLiteral(1.12345, ScalarType.DOUBLE);
        LiteralExpr double2 = new FloatLiteral(1.12345, ScalarType.DOUBLE);
        LiteralExpr double3 = new FloatLiteral(1.12346, ScalarType.DOUBLE);
        LiteralExpr double4 = new FloatLiteral(2.12345, ScalarType.DOUBLE);

        // float
        // value equal
        Assert.assertEquals(float1, float2);
        // self equal
        Assert.assertEquals(float1, float1);

        // value compare
        Assert.assertTrue(!float3.equals(float2) && 1 == float3.compareLiteral(float2));
        Assert.assertTrue(!float4.equals(float1) && 1 == float4.compareLiteral(float1));
        Assert.assertTrue(!float1.equals(float4) && -1 == float1.compareLiteral(float4));
        // value equal
        Assert.assertTrue(0 == float1.compareLiteral(float2));
        // self equal
        Assert.assertTrue(0 == float1.compareLiteral(float1));

        // double
        // value equal
        Assert.assertEquals(double1, double2);
        // self equal
        Assert.assertEquals(double1, double1);

        // value compare
        Assert.assertTrue(!double3.equals(double2) && 1 == double3.compareLiteral(double2));
        Assert.assertTrue(!double4.equals(double1) && 1 == double4.compareLiteral(double1));
        Assert.assertTrue(!double1.equals(double4) && -1 == double1.compareLiteral(double4));
        // value equal
        Assert.assertTrue(0 == double1.compareLiteral(double2));
        // self equal
        Assert.assertTrue(0 == double1.compareLiteral(double1));
    }

    private void intTestInternal(ScalarType type) throws AnalysisException {
        String maxValue = "";
        String minValue = "";
        String normalValue = "100";

        switch (type.getPrimitiveType()) {
            case TINYINT:
                maxValue = "127";
                minValue = "-128";
                break;
            case SMALLINT:
                maxValue = "32767";
                minValue = "-32768";
                break;
            case INT:
                maxValue = "2147483647";
                minValue = "-2147483648";
                break;
            case BIGINT:
                maxValue = "9223372036854775807";
                minValue = "-9223372036854775808";
                break;
            default:
                Assert.fail();
        }

        LiteralExpr tinyint1 = new IntLiteral(maxValue, type);
        LiteralExpr tinyint2 = new IntLiteral(maxValue, type);
        LiteralExpr tinyint3 = new IntLiteral(minValue, type);
        LiteralExpr tinyint4 = new IntLiteral(normalValue, type);

        // infinity
        LiteralExpr infinity1 = MaxLiteral.MAX_VALUE;
        LiteralExpr infinity2 = MaxLiteral.MAX_VALUE;
        LiteralExpr infinity3 = LiteralExpr.createInfinity(type, false);
        LiteralExpr infinity4 = LiteralExpr.createInfinity(type, false);

        // value equal
        Assert.assertEquals(tinyint1, tinyint1);
        // self equal
        Assert.assertEquals(tinyint1, tinyint2);

        // value compare
        Assert.assertTrue(!tinyint1.equals(tinyint3) && 1 == tinyint1.compareLiteral(tinyint3));
        Assert.assertTrue(!tinyint2.equals(tinyint4) && 1 == tinyint2.compareLiteral(tinyint4));
        Assert.assertTrue(!tinyint3.equals(tinyint4) && -1 == tinyint3.compareLiteral(tinyint4));
        // value equal
        Assert.assertTrue(0 == tinyint1.compareLiteral(tinyint1));
        // self equal
        Assert.assertTrue(0 == tinyint1.compareLiteral(tinyint2));

        // infinity
        Assert.assertEquals(infinity1, infinity1);
        Assert.assertEquals(infinity1, infinity2);
        Assert.assertEquals(infinity3, infinity3);
        Assert.assertEquals(infinity3, infinity4);
        Assert.assertNotEquals(tinyint1, infinity1);
        Assert.assertEquals(tinyint3, infinity3);

        Assert.assertTrue(0 == infinity1.compareLiteral(infinity1));
        Assert.assertTrue(0 == infinity1.compareLiteral(infinity2));
        Assert.assertTrue(!infinity1.equals(infinity3) && 1 == infinity1.compareLiteral(infinity3));
        Assert.assertTrue(!infinity4.equals(infinity2) && -1 == infinity4.compareLiteral(infinity2));

        Assert.assertTrue(!infinity4.equals(tinyint1) && -1 == infinity4.compareLiteral(tinyint1));
        Assert.assertTrue(!infinity3.equals(tinyint4) && -1 == infinity3.compareLiteral(tinyint4));

        Assert.assertTrue(infinity1.compareLiteral(tinyint2) == 1);
        Assert.assertTrue(0 == infinity4.compareLiteral(tinyint3));
    }

    @Test
    public void intTest() throws AnalysisException {
        intTestInternal(ScalarType.createType(PrimitiveType.TINYINT));
        intTestInternal(ScalarType.createType(PrimitiveType.SMALLINT));
        intTestInternal(ScalarType.createType(PrimitiveType.INT));
        intTestInternal(ScalarType.createType(PrimitiveType.BIGINT));
    }

    @Test
    public void largeIntTest() throws AnalysisException {
        LiteralExpr largeInt1 = new LargeIntLiteral("170141183460469231731687303715884105727");
        LiteralExpr largeInt3 = new LargeIntLiteral("-170141183460469231731687303715884105728");

        LiteralExpr infinity1 = new LargeIntLiteral(true);
        LiteralExpr infinity3 = new LargeIntLiteral(false);

        // value equal
        Assert.assertEquals(largeInt1, largeInt1);

        // value compare
        Assert.assertTrue(!largeInt1.equals(largeInt3) && 1 == largeInt1.compareLiteral(largeInt3));
        // value equal
        Assert.assertTrue(0 == largeInt1.compareLiteral(largeInt1));

        // infinity
        Assert.assertEquals(infinity1, infinity1);
        Assert.assertEquals(infinity3, infinity3);
        Assert.assertEquals(infinity1, largeInt1);
        Assert.assertEquals(infinity3, largeInt3);

        Assert.assertTrue(!infinity1.equals(largeInt3) && 1 == infinity1.compareLiteral(largeInt3));
        Assert.assertTrue(!infinity3.equals(infinity1) && -1 == infinity3.compareLiteral(infinity1));

        Assert.assertTrue(0 == infinity1.compareLiteral(infinity1));
        Assert.assertTrue(0 == infinity3.compareLiteral(infinity3));
        Assert.assertTrue(0 == infinity1.compareLiteral(largeInt1));
        Assert.assertTrue(0 == infinity3.compareLiteral(largeInt3));
    }

    @Test
    public void stringTest() throws AnalysisException {
        LiteralExpr string1 = new StringLiteral("abc");
        LiteralExpr string2 = new StringLiteral("abc");
        LiteralExpr string3 = new StringLiteral("bcd");
        LiteralExpr string4 = new StringLiteral("a");
        LiteralExpr string5 = new StringLiteral("aa");
        LiteralExpr empty = new StringLiteral("");

        Assert.assertTrue(string1.equals(string1) && string1.compareLiteral(string2) == 0);
        Assert.assertTrue(string1.equals(string2) && string1.compareLiteral(string1) == 0);

        Assert.assertTrue(!string3.equals(string1) && 1 == string3.compareLiteral(string1));
        Assert.assertTrue(!string1.equals(string3) && -1 == string1.compareLiteral(string3));
        Assert.assertTrue(!string5.equals(string4) && 1 == string5.compareLiteral(string4));
        Assert.assertTrue(!string3.equals(string4) && 1 == string3.compareLiteral(string4));
        Assert.assertTrue(!string4.equals(empty) && 1 == string4.compareLiteral(empty));
    }

}
