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

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.sparkdpp.EtlJobConfig;

import org.junit.Assert;
import org.junit.Test;

public class ColumnParserTest {

    // TODO(wb) try to keep ut consistent with be's ut
    @Test
    public void testBoundCheck() {
        // tinyint
        TinyIntParser tinyIntParser = new TinyIntParser();
        // 1 normal
        String tinyint = "100";
        Assert.assertTrue(tinyIntParser.parse(tinyint));
        // 2 upper
        String tinyintUpper = "128";
        Assert.assertFalse(tinyIntParser.parse(tinyintUpper));
        // 3 lower
        String tinyintLower = "-129";
        Assert.assertFalse(tinyIntParser.parse(tinyintLower));

        // smallint
        SmallIntParser smallIntParser = new SmallIntParser();
        // 1 normal
        String smallint = "100";
        Assert.assertTrue(smallIntParser.parse(smallint));
        // 2 upper
        String smallintUpper = "32768";
        Assert.assertFalse(smallIntParser.parse(smallintUpper));
        // 3 lower
        String smallintLower = "-32769";
        Assert.assertFalse(smallIntParser.parse(smallintLower));

        // int
        IntParser intParser = new IntParser();
        // 1 normal
        String intValue = "100";
        Assert.assertTrue(intParser.parse(intValue));
        // 2 upper
        String intUpper = "2147483648";
        Assert.assertFalse(intParser.parse(intUpper));
        // 3 lower
        String intLower = "-2147483649";
        Assert.assertFalse(intParser.parse(intLower));

        // bigint
        BigIntParser bigIntParser = new BigIntParser();
        // 1 normal
        String bigint = "100";
        Assert.assertTrue(bigIntParser.parse(bigint));
        // 2 upper
        String bigintUpper = "9223372036854775808";
        Assert.assertFalse(bigIntParser.parse(bigintUpper));
        // 3 lower
        String bigintLower = "-9223372036854775809";
        Assert.assertFalse(bigIntParser.parse(bigintLower));

        // largeint
        LargeIntParser largeIntParser = new LargeIntParser();
        // 1 normal
        String largeint = "100";
        Assert.assertTrue(largeIntParser.parse(largeint));
        // 2 upper
        String largeintUpper = "170141183460469231731687303715884105728";
        Assert.assertFalse(largeIntParser.parse(largeintUpper));
        // 3 lower
        String largeintLower = "-170141183460469231731687303715884105729";
        Assert.assertFalse(largeIntParser.parse(largeintLower));

        // float
        FloatParser floatParser = new FloatParser();
        // normal
        String floatValue = "1.1";
        Assert.assertTrue(floatParser.parse(floatValue));
        // inf
        String inf = "Infinity";
        Assert.assertFalse(floatParser.parse(inf));
        // nan
        String nan = "NaN";
        // failed
        Assert.assertFalse(floatParser.parse(nan));

        // double
        DoubleParser doubleParser = new DoubleParser();
        // normal
        Assert.assertTrue(doubleParser.parse(floatValue));
        // inf
        Assert.assertFalse(doubleParser.parse(inf));
        // nan
        Assert.assertFalse(doubleParser.parse(nan));

        // decimal
        EtlJobConfig.EtlColumn etlColumn = new EtlJobConfig.EtlColumn();
        etlColumn.precision = 5;
        etlColumn.scale = 3;
        DecimalParser decimalParser = new DecimalParser(etlColumn);
        // normal
        String decimalValue = "10.333";
        Assert.assertTrue(decimalParser.parse(decimalValue));
        // overflow
        String decimalOverflow = "1000.3333333333";
        Assert.assertFalse(decimalParser.parse(decimalOverflow));

        // string
        EtlJobConfig.EtlColumn stringColumn = new EtlJobConfig.EtlColumn();
        stringColumn.stringLength = 3;
        StringParser stringParser = new StringParser(stringColumn);
        // normal
        String stringnormal = "a";
        Assert.assertTrue(stringParser.parse(stringnormal));
        // overflow
        String stringoverflow = "中文";
        Assert.assertFalse(stringParser.parse(stringoverflow));
    }

}
