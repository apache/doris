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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MapLiteralTest {
    static IntLiteral intLiteral1;
    static FloatLiteral floatLiteral;
    static FloatLiteral floatLiteral1;
    static BoolLiteral boolLiteral;
    static StringLiteral stringLiteral;
    static LargeIntLiteral largeIntLiteral;
    static NullLiteral nullLiteral;
    static DateLiteral dateLiteral;
    static DateLiteral datetimeLiteral;
    static DecimalLiteral decimalLiteral1;
    static DecimalLiteral decimalLiteral2;
    static ArrayLiteral arrayLiteral;
    static MapLiteral mapLiteral;
    static StructLiteral structLiteral;


    @BeforeAll
    public static void setUp() throws AnalysisException {
        intLiteral1 = new IntLiteral(1);
        floatLiteral = new FloatLiteral("2.15");
        floatLiteral1 = new FloatLiteral((double) (11 * 3600 + 22 * 60 + 33),
            FloatLiteral.getDefaultTimeType(Type.TIME));
        boolLiteral = new BoolLiteral(true);
        stringLiteral = new StringLiteral("shortstring");
        largeIntLiteral = new LargeIntLiteral("1000000000000000000000");
        nullLiteral = new NullLiteral();
        dateLiteral = new DateLiteral("2022-10-10", Type.DATE);
        datetimeLiteral = new DateLiteral("2022-10-10 12:10:10", Type.DATETIME);
        decimalLiteral1 = new DecimalLiteral("1.0");
        decimalLiteral2 = new DecimalLiteral("2");
        arrayLiteral = new ArrayLiteral(intLiteral1, floatLiteral);
        mapLiteral = new MapLiteral(intLiteral1, floatLiteral);
        structLiteral = new StructLiteral(intLiteral1, floatLiteral, decimalLiteral1, dateLiteral);

    }

    @Test
    public void testGetStringValueForArray() throws AnalysisException {
        MapLiteral mapLiteral1 = new MapLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("{\"1\":\"2.15\"}", mapLiteral1.getStringValueForArray());
        MapLiteral mapLiteral2 = new MapLiteral(boolLiteral, stringLiteral);
        Assert.assertEquals("{\"1\":\"shortstring\"}", mapLiteral2.getStringValueForArray());
        MapLiteral mapLiteral3 = new MapLiteral(largeIntLiteral, dateLiteral);
        Assert.assertEquals("{\"1000000000000000000000\":\"2022-10-10\"}", mapLiteral3.getStringValueForArray());
        MapLiteral mapLiteral4 = new MapLiteral(nullLiteral, nullLiteral);
        Assert.assertEquals("{null:null}", mapLiteral4.getStringValueForArray());
        MapLiteral mapLiteral5 = new MapLiteral(datetimeLiteral, dateLiteral);
        Assert.assertEquals("{\"2022-10-10 12:10:10\":\"2022-10-10\"}", mapLiteral5.getStringValueForArray());

        MapLiteral mapLiteral6 = new MapLiteral();
        Assert.assertEquals("{}", mapLiteral6.getStringValueForArray());

        MapLiteral mapLiteral7 = new MapLiteral(nullLiteral, intLiteral1);
        Assert.assertEquals("{null:\"1\"}", mapLiteral7.getStringValueForArray());
        MapLiteral mapLiteral8 = new MapLiteral(intLiteral1, nullLiteral);
        Assert.assertEquals("{\"1\":null}", mapLiteral8.getStringValueForArray());

        MapLiteral mapLiteral10 = new MapLiteral(intLiteral1, arrayLiteral);
        Assert.assertEquals("{\"1\":[\"1\", \"2.15\"]}", mapLiteral10.getStringValueForArray());
        try {
            new MapLiteral(arrayLiteral, floatLiteral);
        } catch (Exception e) {
            Assert.assertEquals("errCode = 2, "
                    + "detailMessage = Invalid key type in Map, not support ARRAY<DOUBLE>", e.getMessage());
        }

        MapLiteral mapLiteral11 = new MapLiteral(decimalLiteral1, mapLiteral);
        Assert.assertEquals("{\"1.0\":{\"1\":\"2.15\"}}", mapLiteral11.getStringValueForArray());
        try {
            new MapLiteral(mapLiteral, decimalLiteral1);
        } catch (Exception e) {
            Assert.assertEquals("errCode = 2, "
                    + "detailMessage = Invalid key type in Map, not support MAP<TINYINT,DOUBLE>", e.getMessage());
        }

        MapLiteral mapLiteral13 = new MapLiteral(stringLiteral, structLiteral);
        Assert.assertEquals("{\"shortstring\":{\"1\", \"2.15\", \"1.0\", \"2022-10-10\"}}",
                mapLiteral13.getStringValueForArray());
        try {
            new MapLiteral(structLiteral, stringLiteral);
        } catch (Exception e) {
            Assert.assertEquals("errCode = 2, detailMessage = Invalid key type in Map, "
                    + "not support STRUCT<col:TINYINT,col:DOUBLE,col:DECIMALV3(2, 1),col:DATE>", e.getMessage());
        }

    }


    @Test
    public void testGetStringInFe() throws AnalysisException {
        MapLiteral mapLiteral1 = new MapLiteral(intLiteral1, floatLiteral);
        Assert.assertEquals("{\"1\":2.15}", mapLiteral1.getStringValueInFe());
        MapLiteral mapLiteral11 = new MapLiteral(intLiteral1, floatLiteral1);
        Assert.assertEquals("{\"1\":\"11:22:33\"}", mapLiteral11.getStringValueInFe());
        MapLiteral mapLiteral2 = new MapLiteral(boolLiteral, stringLiteral);
        Assert.assertEquals("{\"1\":\"shortstring\"}", mapLiteral2.getStringValueInFe());
        MapLiteral mapLiteral3 = new MapLiteral(largeIntLiteral, dateLiteral);
        Assert.assertEquals("{\"1000000000000000000000\":\"2022-10-10\"}", mapLiteral3.getStringValueInFe());
        MapLiteral mapLiteral4 = new MapLiteral(floatLiteral1, nullLiteral);
        Assert.assertEquals("{\"11:22:33\":null}", mapLiteral4.getStringValueInFe());
        MapLiteral mapLiteral5 = new MapLiteral(datetimeLiteral, dateLiteral);
        Assert.assertEquals("{\"2022-10-10 12:10:10\":\"2022-10-10\"}", mapLiteral5.getStringValueInFe());
        MapLiteral mapLiteral6 = new MapLiteral(decimalLiteral1, decimalLiteral2);
        Assert.assertEquals("{\"1.0\":2}", mapLiteral6.getStringValueInFe());

        MapLiteral mapLiteral7 = new MapLiteral();
        Assert.assertEquals("{}", mapLiteral7.getStringValueInFe());
        MapLiteral mapLiteral8 = new MapLiteral(nullLiteral, intLiteral1);
        Assert.assertEquals("{null:1}", mapLiteral8.getStringValueInFe());
        MapLiteral mapLiteral9 = new MapLiteral(intLiteral1, nullLiteral);
        Assert.assertEquals("{\"1\":null}", mapLiteral9.getStringValueInFe());

        MapLiteral mapLiteral10 = new MapLiteral(intLiteral1, arrayLiteral);
        Assert.assertEquals("{\"1\":[\"1\", \"2.15\"]}", mapLiteral10.getStringValueForArray());
        try {
            new MapLiteral(arrayLiteral, floatLiteral);
        } catch (Exception e) {
            Assert.assertEquals("errCode = 2, "
                    + "detailMessage = Invalid key type in Map, not support ARRAY<DOUBLE>", e.getMessage());
        }

        MapLiteral mapLiteral12 = new MapLiteral(decimalLiteral1, mapLiteral);
        Assert.assertEquals("{\"1.0\":{\"1\":\"2.15\"}}", mapLiteral12.getStringValueForArray());
        try {
            new MapLiteral(mapLiteral, decimalLiteral1);
        } catch (Exception e) {
            Assert.assertEquals("errCode = 2, "
                    + "detailMessage = Invalid key type in Map, not support MAP<TINYINT,DOUBLE>", e.getMessage());
        }

        MapLiteral mapLiteral13 = new MapLiteral(stringLiteral, structLiteral);
        Assert.assertEquals("{\"shortstring\":{\"1\", \"2.15\", \"1.0\", \"2022-10-10\"}}",
                mapLiteral13.getStringValueForArray());
        try {
            new MapLiteral(structLiteral, stringLiteral);
        } catch (Exception e) {
            Assert.assertEquals("errCode = 2, "
                    + "detailMessage = Invalid key type in Map, "
                    + "not support STRUCT<col:TINYINT,col:DOUBLE,col:DECIMALV3(2, 1),col:DATE>", e.getMessage());
        }

    }
}
