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

import org.apache.spark.sql.RowFactory;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class SparkDppTest {

    @Test
    public void testValidateData() {
        SparkDpp sparkDpp = new SparkDpp();

        // decimal
        EtlJobConfig.EtlColumn etlColumn = new EtlJobConfig.EtlColumn();
        etlColumn.columnType = "DECIMALV2";
        etlColumn.precision = 3;
        etlColumn.scale = 2;

        DecimalParser decimalParser = new DecimalParser(etlColumn);
        // test max/min
        Assert.assertEquals(decimalParser.getMaxValue().toString(), "9.99");
        Assert.assertEquals(decimalParser.getMinValue().toString(), "-9.99");
        // normal
        BigDecimal bigDecimal = new BigDecimal("1.21");
        Assert.assertTrue(sparkDpp.validateData(bigDecimal, etlColumn, decimalParser, RowFactory.create(bigDecimal)));
        // failed
        BigDecimal bigDecimalFailed = new BigDecimal("10");
        Assert.assertFalse(sparkDpp.validateData(bigDecimalFailed, etlColumn, decimalParser, RowFactory.create(bigDecimalFailed)));

        // string
        EtlJobConfig.EtlColumn stringColumn = new EtlJobConfig.EtlColumn();
        stringColumn.stringLength = 3;
        stringColumn.columnType = "VARCHAR";
        StringParser stringParser = new StringParser(stringColumn);
        // normal
        String normalString = "a1";
        Assert.assertTrue(sparkDpp.validateData(normalString, stringColumn, stringParser, RowFactory.create(normalString)));
        // cn normal
        String normalStringCN = "中";
        Assert.assertTrue(sparkDpp.validateData(normalStringCN, stringColumn, stringParser, RowFactory.create(normalStringCN)));
        // cn failed
        String failedStringCN = "中a";
        Assert.assertFalse(sparkDpp.validateData(failedStringCN, stringColumn, stringParser, RowFactory.create(failedStringCN)));
    }

}
