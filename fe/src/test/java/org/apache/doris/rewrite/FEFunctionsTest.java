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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

/*
 * Author: Chenmingyu
 * Date: Mar 13, 2019
 */

public class FEFunctionsTest {

    @Test
    public void unixtimestampTest() {
        try {
            IntLiteral timestamp = FEFunctions.unix_timestamp(new DateLiteral("2018-01-01", Type.DATE));
            Assert.assertEquals(1514736000, timestamp.getValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dateFormatUtilTest() {
        try {
            Assert.assertEquals("19670102,196701,196701,0101", FEFunctions.dateFormat(new DateLiteral("1967-01-02 13:04:05", Type.DATETIME), new StringLiteral("%Y%m%d,%X%V,%x%v,%U%u")).getStringValue());
            Assert.assertEquals("19960105,199553,199601,0001", FEFunctions.dateFormat(new DateLiteral("1996-01-05 13:04:05", Type.DATETIME), new StringLiteral("%Y%m%d,%X%V,%x%v,%U%u")).getStringValue());

            Assert.assertEquals("2017-01-01,01,00", FEFunctions.dateFormat(new DateLiteral("2017-01-01 13:04:05", Type.DATETIME), new StringLiteral("%Y-%m-%d,%U,%u")).getStringValue());
            Assert.assertEquals("201753,201752,5352", FEFunctions.dateFormat(new DateLiteral("2017-12-31 13:04:05", Type.DATETIME),new StringLiteral("%X%V,%x%v,%U%u")).getStringValue());

            DateLiteral testDate = new DateLiteral("2001-01-09 13:04:05", Type.DATETIME);
            Assert.assertEquals("Tue", FEFunctions.dateFormat(testDate, new StringLiteral("%a")).getStringValue());
            Assert.assertEquals("Jan", FEFunctions.dateFormat(testDate, new StringLiteral("%b")).getStringValue());
            Assert.assertEquals("1", FEFunctions.dateFormat(testDate, new StringLiteral("%c")).getStringValue());
            Assert.assertEquals("09", FEFunctions.dateFormat(testDate, new StringLiteral("%d")).getStringValue());
            Assert.assertEquals("9", FEFunctions.dateFormat(testDate, new StringLiteral("%e")).getStringValue());
            Assert.assertEquals("13", FEFunctions.dateFormat(testDate, new StringLiteral("%H")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%h")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%I")).getStringValue());
            Assert.assertEquals("04", FEFunctions.dateFormat(testDate, new StringLiteral("%i")).getStringValue());
            Assert.assertEquals("009", FEFunctions.dateFormat(testDate, new StringLiteral("%j")).getStringValue());
            Assert.assertEquals("13", FEFunctions.dateFormat(testDate, new StringLiteral("%k")).getStringValue());
            Assert.assertEquals("1", FEFunctions.dateFormat(testDate, new StringLiteral("%l")).getStringValue());
            Assert.assertEquals("January", FEFunctions.dateFormat(testDate, new StringLiteral("%M")).getStringValue());
            Assert.assertEquals( "01", FEFunctions.dateFormat(testDate, new StringLiteral("%m")).getStringValue());
            Assert.assertEquals("PM", FEFunctions.dateFormat(testDate, new StringLiteral("%p")).getStringValue());
            Assert.assertEquals("01:04:05 PM", FEFunctions.dateFormat(testDate, new StringLiteral("%r")).getStringValue());
            Assert.assertEquals("05", FEFunctions.dateFormat(testDate, new StringLiteral("%S")).getStringValue());
            Assert.assertEquals("05", FEFunctions.dateFormat(testDate, new StringLiteral("%s")).getStringValue());
            Assert.assertEquals("13:04:05", FEFunctions.dateFormat(testDate, new StringLiteral("%T")).getStringValue());
            Assert.assertEquals("02", FEFunctions.dateFormat(testDate, new StringLiteral("%v")).getStringValue());
            Assert.assertEquals("Tuesday", FEFunctions.dateFormat(testDate, new StringLiteral("%W")).getStringValue());
            Assert.assertEquals("2", FEFunctions.dateFormat(testDate, new StringLiteral("%w")).getStringValue());
            Assert.assertEquals("2001", FEFunctions.dateFormat(testDate, new StringLiteral("%Y")).getStringValue());
            Assert.assertEquals("01", FEFunctions.dateFormat(testDate, new StringLiteral("%y")).getStringValue());
            Assert.assertEquals("%", FEFunctions.dateFormat(testDate, new StringLiteral("%%")).getStringValue());
            Assert.assertEquals("foo", FEFunctions.dateFormat(testDate, new StringLiteral("foo")).getStringValue());
            Assert.assertEquals("g", FEFunctions.dateFormat(testDate, new StringLiteral("%g")).getStringValue());
            Assert.assertEquals("4", FEFunctions.dateFormat(testDate, new StringLiteral("%4")).getStringValue());
            Assert.assertEquals("2001 02" ,FEFunctions.dateFormat(testDate, new StringLiteral("%x %v")).getStringValue());
            Assert.assertEquals("9th" ,FEFunctions.dateFormat(testDate, new StringLiteral("%D")).getStringValue());
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
