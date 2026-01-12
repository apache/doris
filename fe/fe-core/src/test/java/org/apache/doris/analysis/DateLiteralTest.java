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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateLiteralTest {

    @Test
    public void testTimestampTzInit() throws AnalysisException {
        String value;
        DateLiteral dateLiteral;
        String expectedResult;

        value = "2020-02-02 09:03:04.123456+08:00";
        dateLiteral = new DateLiteral(value, ScalarType.createTimeStampTzType(6));
        expectedResult = "'2020-02-02 01:03:04.123456+00:00'";
        Assertions.assertEquals(expectedResult, dateLiteral.toSql());
    }

    @Test
    public void testTimestampTzStringForQuery() throws AnalysisException {
        try {
            ConnectContext context = new ConnectContext();
            context.setThreadLocalInfo();
            DateLiteral dateLiteral = new DateLiteral("2020-02-02 12:00:03.123456+00:00",
                    ScalarType.createTimeStampTzType(6));
            String timeZone;
            String expected;

            timeZone = "+08:00";
            context.getSessionVariable().setTimeZone(timeZone);
            expected = "2020-02-02 20:00:03.123456+08:00";
            Assertions.assertEquals(expected, dateLiteral.getStringValueForQuery(null));

            timeZone = "-08:00";
            context.getSessionVariable().setTimeZone(timeZone);
            expected = "2020-02-02 04:00:03.123456-08:00";
            Assertions.assertEquals(expected, dateLiteral.getStringValueForQuery(null));
        } finally {
            ConnectContext.remove();
        }

    }

    @Test
    public void testDatetimeV2Init() throws AnalysisException {
        String value;
        DateLiteral dateLiteral;
        String expectedResult;

        value = "2020-02-02 09:03:04.123456+08:00";
        dateLiteral = new DateLiteral(value, ScalarType.createDatetimeV2Type(6));
        expectedResult = "'2020-02-02 09:03:04.123456'";
        Assertions.assertEquals(expectedResult, dateLiteral.toSql());

        value = "2020-02-02 09:03:04.123456+09:00";
        dateLiteral = new DateLiteral(value, ScalarType.createDatetimeV2Type(6));
        expectedResult = "'2020-02-02 08:03:04.123456'";
        Assertions.assertEquals(expectedResult, dateLiteral.toSql());
    }

    @Test
    public void testDateV2Init() throws AnalysisException {
        String value;
        DateLiteral dateLiteral;
        String expectedResult;

        value = "2020-02-02+08:00";
        dateLiteral = new DateLiteral(value, ScalarType.DATEV2);
        expectedResult = "'2020-02-02'";
        Assertions.assertEquals(expectedResult, dateLiteral.toSql());
        value = "2020-02-02+09:00";
        dateLiteral = new DateLiteral(value, ScalarType.DATEV2);
        expectedResult = "'2020-02-01'";
        Assertions.assertEquals(expectedResult, dateLiteral.toSql());
    }
}
