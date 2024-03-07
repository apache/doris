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

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class ArrayLiteralTest  {
    @Test
    public void testGetStringValueForArray() throws AnalysisException {
        IntLiteral intLiteral1 = new IntLiteral(1);
        FloatLiteral floatLiteral = new FloatLiteral("2.15");
        BoolLiteral boolLiteral = new BoolLiteral(true);
        StringLiteral stringLiteral = new StringLiteral("shortstring");
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("1000000000000000000000");
        NullLiteral nullLiteral = new NullLiteral();
        DateLiteral dateLiteral = new DateLiteral("2022-10-10", Type.DATE);
        DateLiteral datetimeLiteral = new DateLiteral("2022-10-10 12:10:10", Type.DATETIME);
        ArrayLiteral arrayLiteral1 = new ArrayLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("[\"1\", \"2.15\"]", arrayLiteral1.getStringValueForArray());

        ArrayLiteral arrayLiteral2 = new ArrayLiteral(boolLiteral, boolLiteral);
        Assert.assertEquals("[\"1\", \"1\"]", arrayLiteral2.getStringValueForArray());

        ArrayLiteral arrayLiteral3 = new ArrayLiteral(stringLiteral, stringLiteral);
        Assert.assertEquals("[\"shortstring\", \"shortstring\"]", arrayLiteral3.getStringValueForArray());

        ArrayLiteral arrayLiteral4 = new ArrayLiteral(largeIntLiteral, largeIntLiteral);
        Assert.assertEquals("[\"1000000000000000000000\", \"1000000000000000000000\"]", arrayLiteral4.getStringValueForArray());

        ArrayLiteral arrayLiteral5 = new ArrayLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("[null, null]", arrayLiteral5.getStringValueForArray());

        ArrayLiteral arrayLiteral6 = new ArrayLiteral(dateLiteral, dateLiteral);
        Assert.assertEquals("[\"2022-10-10\", \"2022-10-10\"]", arrayLiteral6.getStringValueForArray());

        ArrayLiteral arrayLiteral7 = new ArrayLiteral(datetimeLiteral, datetimeLiteral);
        Assert.assertEquals("[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]", arrayLiteral7.getStringValueForArray());

        ArrayLiteral arrayLiteral8 = new ArrayLiteral(arrayLiteral7, arrayLiteral7);
        Assert.assertEquals("[[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"], [\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]]",
                arrayLiteral8.getStringValueForArray());

        ArrayLiteral arrayLiteral9 = new ArrayLiteral();
        Assert.assertEquals("[]", arrayLiteral9.getStringValueForArray());

        ArrayLiteral arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        MapLiteral mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        StructLiteral structLiteral = new StructLiteral(intLiteral1, floatLiteral, dateLiteral);
        ArrayLiteral arrayLiteral10 = new ArrayLiteral(arrayLiteral, arrayLiteral);
        Assert.assertEquals("[[\"1\", \"2.15\"], [\"1\", \"2.15\"]]", arrayLiteral10.getStringValueForArray());
        ArrayLiteral arrayLiteral11 = new ArrayLiteral(mapLiteral);
        Assert.assertEquals("[{\"1\":\"2.15\"}]", arrayLiteral11.getStringValueForArray());
        ArrayLiteral arrayLiteral12 = new ArrayLiteral(structLiteral);
        Assert.assertEquals("[{\"1\", \"2.15\", \"2022-10-10\"}]", arrayLiteral12.getStringValueForArray());
    }


    @Test
    public void testGetStringInFe() throws AnalysisException {
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
        Assert.assertEquals("[1.0, 2.15]", arrayLiteral1.getStringValueInFe());
        ArrayLiteral arrayLiteralWithTime = new ArrayLiteral(floatLiteral1);
        Assert.assertEquals("[\"11:22:33\"]", arrayLiteralWithTime.getStringValueInFe());

        ArrayLiteral arrayLiteral2 = new ArrayLiteral(boolLiteral, boolLiteral);
        Assert.assertEquals("[1, 1]", arrayLiteral2.getStringValueInFe());

        ArrayLiteral arrayLiteral3 = new ArrayLiteral(stringLiteral, stringLiteral);
        Assert.assertEquals("[\"shortstring\", \"shortstring\"]", arrayLiteral3.getStringValueInFe());

        ArrayLiteral arrayLiteral4 = new ArrayLiteral(largeIntLiteral, largeIntLiteral);
        Assert.assertEquals("[1000000000000000000000, 1000000000000000000000]",
                arrayLiteral4.getStringValueInFe());

        ArrayLiteral arrayLiteral5 = new ArrayLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("[null, null]", arrayLiteral5.getStringValueInFe());

        ArrayLiteral arrayLiteral6 = new ArrayLiteral(dateLiteral, dateLiteral);
        Assert.assertEquals("[\"2022-10-10\", \"2022-10-10\"]", arrayLiteral6.getStringValueInFe());

        ArrayLiteral arrayLiteral7 = new ArrayLiteral(datetimeLiteral, datetimeLiteral);
        Assert.assertEquals("[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]",
                arrayLiteral7.getStringValueInFe());

        ArrayLiteral arrayLiteral8 = new ArrayLiteral(arrayLiteral7, arrayLiteral7);
        Assert.assertEquals("[[\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"], [\"2022-10-10 12:10:10\", \"2022-10-10 12:10:10\"]]",
                arrayLiteral8.getStringValueInFe());

        ArrayLiteral arrayLiteral9 = new ArrayLiteral();
        Assert.assertEquals("[]", arrayLiteral9.getStringValueInFe());

        DecimalLiteral decimalLiteral = new DecimalLiteral("1.0");
        DecimalLiteral decimalLiteral2 = new DecimalLiteral("2");
        ArrayLiteral arrayLiteral10 = new ArrayLiteral(decimalLiteral, decimalLiteral2);
        Assert.assertEquals("[1.0, 2.0]", arrayLiteral10.getStringValueInFe());

        //array(1, null)
        IntLiteral intLiteralWithNull = new IntLiteral(1);
        ArrayLiteral arrayLiteral11 = new ArrayLiteral(intLiteralWithNull, nullLiteral);
        Assert.assertEquals("[1, null]", arrayLiteral11.getStringValueInFe());
        //array(null, 1)
        ArrayLiteral arrayLiteral12 = new ArrayLiteral(nullLiteral, intLiteralWithNull);
        Assert.assertEquals("[null, 1]", arrayLiteral12.getStringValueInFe());

        ArrayLiteral arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        MapLiteral mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        StructLiteral structLiteral = new StructLiteral(intLiteral1, floatLiteral, dateLiteral);
        ArrayLiteral arrayLiteral13 = new ArrayLiteral(arrayLiteral, arrayLiteral);
        Assert.assertEquals("[[\"1\", \"2.15\"], [\"1\", \"2.15\"]]", arrayLiteral13.getStringValueForArray());
        ArrayLiteral arrayLiteral14 = new ArrayLiteral(mapLiteral);
        Assert.assertEquals("[{\"1\":\"2.15\"}]", arrayLiteral14.getStringValueForArray());
        ArrayLiteral arrayLiteral15 = new ArrayLiteral(structLiteral);
        Assert.assertEquals("[{\"1\", \"2.15\", \"2022-10-10\"}]", arrayLiteral15.getStringValueForArray());

    }
}
