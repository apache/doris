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
        Assert.assertEquals("[\"1.0\", \"2.15\"]", arrayLiteral1.getStringValueForArray());

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
    }
}
