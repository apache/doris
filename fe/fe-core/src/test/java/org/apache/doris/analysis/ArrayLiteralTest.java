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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class ArrayLiteralTest  {
    @Test
    public void testGetStringForQuery() throws AnalysisException {
        FormatOptions options = FormatOptions.getDefault();
        IntLiteral intLiteral1 = new IntLiteral(1);
        FloatLiteral floatLiteral = new FloatLiteral("2.15");
        FloatLiteral floatLiteral1 = new FloatLiteral((double) (11 * 3600 + 22 * 60 + 33),
                FloatLiteral.getDefaultTimeType(Type.TIME));

        BoolLiteral boolLiteral = new BoolLiteral(true);
        StringLiteral stringLiteral = new StringLiteral("shortstring");
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("1000000000000000000000");
        NullLiteral nullLiteral = new NullLiteral();
        DateLiteral dateLiteral = new DateLiteral("2022-10-10", Type.DATE);
        DateLiteral datetimeLiteral = new DateLiteral("2022-10-10 12:10:10", Type.DATETIME);
        ArrayLiteral arrayLiteral1 = new ArrayLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("[1, 2.15]", arrayLiteral1.getStringValueForQuery(options));
        ArrayLiteral arrayLiteralWithTime = new ArrayLiteral(floatLiteral1);
        Assert.assertEquals("[\"11:22:33\"]", arrayLiteralWithTime.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral2 = new ArrayLiteral(boolLiteral, boolLiteral);
        Assert.assertEquals("[1, 1]", arrayLiteral2.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral3 = new ArrayLiteral(stringLiteral, stringLiteral);
        Assert.assertEquals("[\"shortstring\", \"shortstring\"]", arrayLiteral3.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral4 = new ArrayLiteral(largeIntLiteral, largeIntLiteral);
        Assert.assertEquals("[1000000000000000000000, 1000000000000000000000]",
                arrayLiteral4.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral5 = new ArrayLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("[null, null]", arrayLiteral5.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral6 = new ArrayLiteral(dateLiteral, dateLiteral);
        Assert.assertEquals("[\"2022-10-10\", \"2022-10-10\"]", arrayLiteral6.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral7 = new ArrayLiteral(datetimeLiteral, datetimeLiteral);
        Assert.assertEquals("[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]",
                arrayLiteral7.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral8 = new ArrayLiteral(arrayLiteral7, arrayLiteral7);
        Assert.assertEquals("[[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"], [\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]]",
                arrayLiteral8.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral9 = new ArrayLiteral();
        Assert.assertEquals("[]", arrayLiteral9.getStringValueForQuery(options));

        DecimalLiteral decimalLiteral = new DecimalLiteral("1.0");
        DecimalLiteral decimalLiteral2 = new DecimalLiteral("2");
        ArrayLiteral arrayLiteral10 = new ArrayLiteral(decimalLiteral, decimalLiteral2);
        Assert.assertEquals("[1.0, 2.0]", arrayLiteral10.getStringValueForQuery(options));

        //array(1, null)
        IntLiteral intLiteralWithNull = new IntLiteral(1);
        ArrayLiteral arrayLiteral11 = new ArrayLiteral(intLiteralWithNull, nullLiteral);
        Assert.assertEquals("[1, null]", arrayLiteral11.getStringValueForQuery(options));
        //array(null, 1)
        ArrayLiteral arrayLiteral12 = new ArrayLiteral(nullLiteral, intLiteralWithNull);
        Assert.assertEquals("[null, 1]", arrayLiteral12.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        MapLiteral mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        StructLiteral structLiteral = new StructLiteral(intLiteral1, floatLiteral, dateLiteral);
        ArrayLiteral arrayLiteral13 = new ArrayLiteral(arrayLiteral, arrayLiteral);
        Assert.assertEquals("[[1, 2.15], [1, 2.15]]",
                arrayLiteral13.getStringValueForQuery(options));
        ArrayLiteral arrayLiteral14 = new ArrayLiteral(mapLiteral);
        Assert.assertEquals("[{1:2.15}]", arrayLiteral14.getStringValueForQuery(options));
        ArrayLiteral arrayLiteral15 = new ArrayLiteral(structLiteral);
        Assert.assertEquals("[{\"col1\":1, \"col2\":2.15, \"col3\":\"2022-10-10\"}]",
                arrayLiteral15.getStringValueForQuery(options));
    }

    @Test
    public void testGetStringForQueryForPresto() throws AnalysisException {
        FormatOptions options = FormatOptions.getForPresto();
        IntLiteral intLiteral1 = new IntLiteral(1);
        FloatLiteral floatLiteral = new FloatLiteral("2.15");
        FloatLiteral floatLiteral1 = new FloatLiteral((double) (11 * 3600 + 22 * 60 + 33),
                FloatLiteral.getDefaultTimeType(Type.TIME));

        BoolLiteral boolLiteral = new BoolLiteral(true);
        StringLiteral stringLiteral = new StringLiteral("shortstring");
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("1000000000000000000000");
        NullLiteral nullLiteral = new NullLiteral();
        DateLiteral dateLiteral = new DateLiteral("2022-10-10", Type.DATE);
        DateLiteral datetimeLiteral = new DateLiteral("2022-10-10 12:10:10", Type.DATETIME);
        ArrayLiteral arrayLiteral1 = new ArrayLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("[1, 2.15]", arrayLiteral1.getStringValueForQuery(options));
        ArrayLiteral arrayLiteralWithTime = new ArrayLiteral(floatLiteral1);
        Assert.assertEquals("[11:22:33]", arrayLiteralWithTime.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral2 = new ArrayLiteral(boolLiteral, boolLiteral);
        Assert.assertEquals("[1, 1]", arrayLiteral2.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral3 = new ArrayLiteral(stringLiteral, stringLiteral);
        Assert.assertEquals("[shortstring, shortstring]", arrayLiteral3.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral4 = new ArrayLiteral(largeIntLiteral, largeIntLiteral);
        Assert.assertEquals("[1000000000000000000000, 1000000000000000000000]",
                arrayLiteral4.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral5 = new ArrayLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("[NULL, NULL]", arrayLiteral5.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral6 = new ArrayLiteral(dateLiteral, dateLiteral);
        Assert.assertEquals("[2022-10-10, 2022-10-10]", arrayLiteral6.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral7 = new ArrayLiteral(datetimeLiteral, datetimeLiteral);
        Assert.assertEquals("[2022-10-10 12:10:10, 2022-10-10 12:10:10]",
                arrayLiteral7.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral8 = new ArrayLiteral(arrayLiteral7, arrayLiteral7);
        Assert.assertEquals("[[2022-10-10 12:10:10, 2022-10-10 12:10:10], [2022-10-10 12:10:10, 2022-10-10 12:10:10]]",
                arrayLiteral8.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral9 = new ArrayLiteral();
        Assert.assertEquals("[]", arrayLiteral9.getStringValueForQuery(options));

        DecimalLiteral decimalLiteral = new DecimalLiteral("1.0");
        DecimalLiteral decimalLiteral2 = new DecimalLiteral("2");
        ArrayLiteral arrayLiteral10 = new ArrayLiteral(decimalLiteral, decimalLiteral2);
        Assert.assertEquals("[1.0, 2.0]", arrayLiteral10.getStringValueForQuery(options));

        //array(1, null)
        IntLiteral intLiteralWithNull = new IntLiteral(1);
        ArrayLiteral arrayLiteral11 = new ArrayLiteral(intLiteralWithNull, nullLiteral);
        Assert.assertEquals("[1, NULL]", arrayLiteral11.getStringValueForQuery(options));
        //array(null, 1)
        ArrayLiteral arrayLiteral12 = new ArrayLiteral(nullLiteral, intLiteralWithNull);
        Assert.assertEquals("[NULL, 1]", arrayLiteral12.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        MapLiteral mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        StructLiteral structLiteral = new StructLiteral(intLiteral1, floatLiteral, dateLiteral);
        ArrayLiteral arrayLiteral13 = new ArrayLiteral(arrayLiteral, arrayLiteral);
        Assert.assertEquals("[[1, 2.15], [1, 2.15]]", arrayLiteral13.getStringValueForQuery(options));
        ArrayLiteral arrayLiteral14 = new ArrayLiteral(mapLiteral);
        Assert.assertEquals("[{1=2.15}]", arrayLiteral14.getStringValueForQuery(options));
        ArrayLiteral arrayLiteral15 = new ArrayLiteral(structLiteral);
        Assert.assertEquals("[{col1=1, col2=2.15, col3=2022-10-10}]", arrayLiteral15.getStringValueForQuery(options));
    }

    @Test
    public void testGetStringForQueryForHive() throws AnalysisException {
        FormatOptions options = FormatOptions.getForHive();
        IntLiteral intLiteral1 = new IntLiteral(1);
        FloatLiteral floatLiteral = new FloatLiteral("2.15");
        FloatLiteral floatLiteral1 = new FloatLiteral((double) (11 * 3600 + 22 * 60 + 33),
                FloatLiteral.getDefaultTimeType(Type.TIME));

        BoolLiteral boolLiteral = new BoolLiteral(true);
        StringLiteral stringLiteral = new StringLiteral("shortstring");
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("1000000000000000000000");
        NullLiteral nullLiteral = new NullLiteral();
        DateLiteral dateLiteral = new DateLiteral("2022-10-10", Type.DATE);
        DateLiteral datetimeLiteral = new DateLiteral("2022-10-10 12:10:10", Type.DATETIME);
        ArrayLiteral arrayLiteral1 = new ArrayLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("[1,2.15]", arrayLiteral1.getStringValueForQuery(options));
        ArrayLiteral arrayLiteralWithTime = new ArrayLiteral(floatLiteral1);
        Assert.assertEquals("[\"11:22:33\"]", arrayLiteralWithTime.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral2 = new ArrayLiteral(boolLiteral, boolLiteral);
        Assert.assertEquals("[true,true]", arrayLiteral2.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral3 = new ArrayLiteral(stringLiteral, stringLiteral);
        Assert.assertEquals("[\"shortstring\",\"shortstring\"]", arrayLiteral3.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral4 = new ArrayLiteral(largeIntLiteral, largeIntLiteral);
        Assert.assertEquals("[1000000000000000000000,1000000000000000000000]",
                arrayLiteral4.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral5 = new ArrayLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("[null,null]", arrayLiteral5.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral6 = new ArrayLiteral(dateLiteral, dateLiteral);
        Assert.assertEquals("[\"2022-10-10\",\"2022-10-10\"]", arrayLiteral6.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral7 = new ArrayLiteral(datetimeLiteral, datetimeLiteral);
        Assert.assertEquals("[\"2022-10-10 12:10:10\",\"2022-10-10 12:10:10\"]",
                arrayLiteral7.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral8 = new ArrayLiteral(arrayLiteral7, arrayLiteral7);
        Assert.assertEquals(
                "[[\"2022-10-10 12:10:10\",\"2022-10-10 12:10:10\"],[\"2022-10-10 12:10:10\",\"2022-10-10 12:10:10\"]]",
                arrayLiteral8.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral9 = new ArrayLiteral();
        Assert.assertEquals("[]", arrayLiteral9.getStringValueForQuery(options));

        DecimalLiteral decimalLiteral = new DecimalLiteral("1.0");
        DecimalLiteral decimalLiteral2 = new DecimalLiteral("2");
        ArrayLiteral arrayLiteral10 = new ArrayLiteral(decimalLiteral, decimalLiteral2);
        Assert.assertEquals("[1.0,2.0]", arrayLiteral10.getStringValueForQuery(options));

        //array(1, null)
        IntLiteral intLiteralWithNull = new IntLiteral(1);
        ArrayLiteral arrayLiteral11 = new ArrayLiteral(intLiteralWithNull, nullLiteral);
        Assert.assertEquals("[1,null]", arrayLiteral11.getStringValueForQuery(options));
        //array(null, 1)
        ArrayLiteral arrayLiteral12 = new ArrayLiteral(nullLiteral, intLiteralWithNull);
        Assert.assertEquals("[null,1]", arrayLiteral12.getStringValueForQuery(options));

        ArrayLiteral arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        MapLiteral mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        StructLiteral structLiteral = new StructLiteral(intLiteral1, floatLiteral, dateLiteral);
        ArrayLiteral arrayLiteral13 = new ArrayLiteral(arrayLiteral, arrayLiteral);
        Assert.assertEquals("[[1,2.15],[1,2.15]]", arrayLiteral13.getStringValueForQuery(options));
        ArrayLiteral arrayLiteral14 = new ArrayLiteral(mapLiteral);
        Assert.assertEquals("[{1:2.15}]", arrayLiteral14.getStringValueForQuery(options));
        ArrayLiteral arrayLiteral15 = new ArrayLiteral(structLiteral);
        Assert.assertEquals("[{\"col1\":1,\"col2\":2.15,\"col3\":\"2022-10-10\"}]",
                arrayLiteral15.getStringValueForQuery(options));
    }

    @Test
    public void testGetStringForStreamLoad() throws AnalysisException {
        FormatOptions options = FormatOptions.getDefault();
        IntLiteral intLiteral1 = new IntLiteral(1);
        FloatLiteral floatLiteral = new FloatLiteral("2.15");
        FloatLiteral floatLiteral1 = new FloatLiteral((double) (11 * 3600 + 22 * 60 + 33),
                FloatLiteral.getDefaultTimeType(Type.TIME));

        BoolLiteral boolLiteral = new BoolLiteral(true);
        StringLiteral stringLiteral = new StringLiteral("shortstring");
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("1000000000000000000000");
        NullLiteral nullLiteral = new NullLiteral();
        DateLiteral dateLiteral = new DateLiteral("2022-10-10", Type.DATE);
        DateLiteral datetimeLiteral = new DateLiteral("2022-10-10 12:10:10", Type.DATETIME);
        ArrayLiteral arrayLiteral1 = new ArrayLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("[1, 2.15]", arrayLiteral1.getStringValueForStreamLoad(options));
        ArrayLiteral arrayLiteralWithTime = new ArrayLiteral(floatLiteral1);
        Assert.assertEquals("[\"11:22:33\"]", arrayLiteralWithTime.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral2 = new ArrayLiteral(boolLiteral, boolLiteral);
        Assert.assertEquals("[1, 1]", arrayLiteral2.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral3 = new ArrayLiteral(stringLiteral, stringLiteral);
        Assert.assertEquals("[\"shortstring\", \"shortstring\"]", arrayLiteral3.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral4 = new ArrayLiteral(largeIntLiteral, largeIntLiteral);
        Assert.assertEquals("[1000000000000000000000, 1000000000000000000000]",
                arrayLiteral4.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral5 = new ArrayLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("[null, null]", arrayLiteral5.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral6 = new ArrayLiteral(dateLiteral, dateLiteral);
        Assert.assertEquals("[\"2022-10-10\", \"2022-10-10\"]", arrayLiteral6.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral7 = new ArrayLiteral(datetimeLiteral, datetimeLiteral);
        Assert.assertEquals("[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]",
                arrayLiteral7.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral8 = new ArrayLiteral(arrayLiteral7, arrayLiteral7);
        Assert.assertEquals(
                "[[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"], [\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]]",
                arrayLiteral8.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral9 = new ArrayLiteral();
        Assert.assertEquals("[]", arrayLiteral9.getStringValueForStreamLoad(options));

        DecimalLiteral decimalLiteral = new DecimalLiteral("1.0");
        DecimalLiteral decimalLiteral2 = new DecimalLiteral("2");
        ArrayLiteral arrayLiteral10 = new ArrayLiteral(decimalLiteral, decimalLiteral2);
        Assert.assertEquals("[1.0, 2.0]", arrayLiteral10.getStringValueForStreamLoad(options));

        //array(1, null)
        IntLiteral intLiteralWithNull = new IntLiteral(1);
        ArrayLiteral arrayLiteral11 = new ArrayLiteral(intLiteralWithNull, nullLiteral);
        Assert.assertEquals("[1, null]", arrayLiteral11.getStringValueForStreamLoad(options));
        //array(null, 1)
        ArrayLiteral arrayLiteral12 = new ArrayLiteral(nullLiteral, intLiteralWithNull);
        Assert.assertEquals("[null, 1]", arrayLiteral12.getStringValueForStreamLoad(options));

        ArrayLiteral arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        MapLiteral mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        StructLiteral structLiteral = new StructLiteral(intLiteral1, floatLiteral, dateLiteral);
        ArrayLiteral arrayLiteral13 = new ArrayLiteral(arrayLiteral, arrayLiteral);
        Assert.assertEquals("[[1, 2.15], [1, 2.15]]",
                arrayLiteral13.getStringValueForStreamLoad(options));
        ArrayLiteral arrayLiteral14 = new ArrayLiteral(mapLiteral);
        Assert.assertEquals("[{1:2.15}]", arrayLiteral14.getStringValueForStreamLoad(options));
        ArrayLiteral arrayLiteral15 = new ArrayLiteral(structLiteral);
        Assert.assertEquals("[{\"col1\":1, \"col2\":2.15, \"col3\":\"2022-10-10\"}]",
                arrayLiteral15.getStringValueForStreamLoad(options));
    }
}
