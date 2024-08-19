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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class StructLiteralTest {
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
        FormatOptions options = FormatOptions.getDefault();
        StructLiteral structLiteral1 = new StructLiteral(intLiteral1, floatLiteral, floatLiteral1, boolLiteral,
                stringLiteral, largeIntLiteral, decimalLiteral1, decimalLiteral2, dateLiteral,
                datetimeLiteral);
        Assert.assertEquals("{\"1\", \"2.15\", \"11:22:33\", \"1\", \"shortstring\", "
                        + "\"1000000000000000000000\", \"1.0\", \"2\", \"2022-10-10\", \"2022-10-10 12:10:10\"}",
                structLiteral1.getStringValueForArray(options));
        StructLiteral structLiteral2 = new StructLiteral(arrayLiteral, mapLiteral, structLiteral);
        Assert.assertEquals("{[\"1\", \"2.15\"], {\"1\":\"2.15\"}, {\"1\", \"2.15\", \"1.0\", \"2022-10-10\"}}",
                structLiteral2.getStringValueForArray(options));
        StructLiteral structLiteral3 = new StructLiteral();
        Assert.assertEquals("{}", structLiteral3.getStringValueForArray(options));

        StructLiteral nullStruct = new StructLiteral(nullLiteral, intLiteral1);
        Assert.assertEquals("{null, \"1\"}", nullStruct.getStringValueForArray(options));

    }

    @Test
    public void testGetStringInFe() throws AnalysisException {
        FormatOptions options = FormatOptions.getDefault();
        StructLiteral structLiteral1 = new StructLiteral(intLiteral1, floatLiteral, floatLiteral1, boolLiteral,
                stringLiteral, largeIntLiteral, decimalLiteral1, decimalLiteral2, dateLiteral, datetimeLiteral);
        Assert.assertEquals("{\"col1\":1, \"col2\":2.15, \"col3\":\"11:22:33\", \"col4\":1, \"col5\":"
                        + "\"shortstring\", \"col6\":1000000000000000000000, \"col7\":1.0, \"col8\":2, \"col9\":\"2022-10-10\", \"col10\":\"2022-10-10 12:10:10\"}",
                structLiteral1.getStringValueInFe(options));
        StructLiteral structLiteral2 = new StructLiteral(arrayLiteral, mapLiteral, structLiteral);
        Assert.assertEquals("{\"col1\":[1.0, 2.15], \"col2\":{1:2.15}, \"col3\":"
                        + "{\"col1\":1, \"col2\":2.15, \"col3\":1.0, \"col4\":\"2022-10-10\"}}",
                structLiteral2.getStringValueInFe(options));
        StructLiteral structLiteral3 = new StructLiteral();
        Assert.assertEquals("{}", structLiteral3.getStringValueInFe(options));

        StructLiteral nullStruct = new StructLiteral(nullLiteral, intLiteral1);
        Assert.assertEquals("{\"col1\":null, \"col2\":1}", nullStruct.getStringValueInFe(options));
    }

    @Test
    public void testGetStringValueForArrayForPreto() throws AnalysisException {
        FormatOptions options = FormatOptions.getForPresto();
        StructLiteral structLiteral1 = new StructLiteral(intLiteral1, floatLiteral, floatLiteral1, boolLiteral,
                stringLiteral, largeIntLiteral, decimalLiteral1, decimalLiteral2, dateLiteral,
                datetimeLiteral);
        Assert.assertEquals("{1, 2.15, 11:22:33, 1, shortstring, "
                        + "1000000000000000000000, 1.0, 2, 2022-10-10, 2022-10-10 12:10:10}",
                structLiteral1.getStringValueForArray(options));
        StructLiteral structLiteral2 = new StructLiteral(arrayLiteral, mapLiteral, structLiteral);
        Assert.assertEquals("{[1, 2.15], {1=2.15}, {1, 2.15, 1.0, 2022-10-10}}",
                structLiteral2.getStringValueForArray(options));
        StructLiteral structLiteral3 = new StructLiteral();
        Assert.assertEquals("{}", structLiteral3.getStringValueForArray(options));

        StructLiteral nullStruct = new StructLiteral(nullLiteral, intLiteral1);
        Assert.assertEquals("{NULL, 1}", nullStruct.getStringValueForArray(options));

    }

    @Test
    public void testGetStringInFeForPresto() throws AnalysisException {
        FormatOptions options = FormatOptions.getForPresto();
        StructLiteral structLiteral1 = new StructLiteral(intLiteral1, floatLiteral, floatLiteral1, boolLiteral,
                stringLiteral, largeIntLiteral, decimalLiteral1, decimalLiteral2, dateLiteral, datetimeLiteral);
        Assert.assertEquals("{col1=1, col2=2.15, col3=11:22:33, col4=1, col5="
                        + "shortstring, col6=1000000000000000000000, col7=1.0, col8=2, col9=2022-10-10, col10=2022-10-10 12:10:10}",
                structLiteral1.getStringValueInFe(options));
        StructLiteral structLiteral2 = new StructLiteral(arrayLiteral, mapLiteral, structLiteral);
        Assert.assertEquals("{col1=[1.0, 2.15], col2={1=2.15}, col3="
                        + "{col1=1, col2=2.15, col3=1.0, col4=2022-10-10}}",
                structLiteral2.getStringValueInFe(options));
        StructLiteral structLiteral3 = new StructLiteral();
        Assert.assertEquals("{}", structLiteral3.getStringValueInFe(options));

        StructLiteral nullStruct = new StructLiteral(nullLiteral, intLiteral1);
        Assert.assertEquals("{col1=NULL, col2=1}", nullStruct.getStringValueInFe(options));
    }
}
