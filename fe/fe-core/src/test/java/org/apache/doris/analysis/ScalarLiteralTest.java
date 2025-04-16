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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FormatOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ScalarLiteralTest {
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
    public void testGetStringForQuery() {
        FormatOptions options = FormatOptions.getDefault();
        Assertions.assertEquals("1", intLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("2.15", floatLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("11:22:33", floatLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("1", boolLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("shortstring", stringLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("1000000000000000000000", largeIntLiteral.getStringValueForQuery(options));
        Assertions.assertEquals(null, nullLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("2022-10-10", dateLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("2022-10-10 12:10:10", datetimeLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("1.0", decimalLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("2", decimalLiteral2.getStringValueForQuery(options));
    }

    @Test
    public void testGetStringForQueryForPresto() {
        FormatOptions options = FormatOptions.getForPresto();
        Assertions.assertEquals("1", intLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("2.15", floatLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("11:22:33", floatLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("1", boolLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("shortstring", stringLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("1000000000000000000000", largeIntLiteral.getStringValueForQuery(options));
        Assertions.assertEquals(null, nullLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("2022-10-10", dateLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("2022-10-10 12:10:10", datetimeLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("1.0", decimalLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("2", decimalLiteral2.getStringValueForQuery(options));
    }

    @Test
    public void testGetStringForQueryForHive() {
        FormatOptions options = FormatOptions.getForHive();
        Assertions.assertEquals("1", intLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("2.15", floatLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("11:22:33", floatLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("1", boolLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("shortstring", stringLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("1000000000000000000000", largeIntLiteral.getStringValueForQuery(options));
        Assertions.assertEquals(null, nullLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("2022-10-10", dateLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("2022-10-10 12:10:10", datetimeLiteral.getStringValueForQuery(options));
        Assertions.assertEquals("1.0", decimalLiteral1.getStringValueForQuery(options));
        Assertions.assertEquals("2", decimalLiteral2.getStringValueForQuery(options));
    }

    @Test
    public void testGetStringForStreamLoad() {
        FormatOptions options = FormatOptions.getDefault();
        Assertions.assertEquals("1", intLiteral1.getStringValueForStreamLoad(options));
        Assertions.assertEquals("2.15", floatLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals("11:22:33", floatLiteral1.getStringValueForStreamLoad(options));
        Assertions.assertEquals("1", boolLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals("shortstring", stringLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals("1000000000000000000000", largeIntLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals(FeConstants.null_string, nullLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals("2022-10-10", dateLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals("2022-10-10 12:10:10", datetimeLiteral.getStringValueForStreamLoad(options));
        Assertions.assertEquals("1.0", decimalLiteral1.getStringValueForStreamLoad(options));
        Assertions.assertEquals("2", decimalLiteral2.getStringValueForStreamLoad(options));
    }
}
