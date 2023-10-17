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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

class DateLiteralTest {
    @Test
    void reject() {
        // TODO: reject them.
        // Now parse them as date + offset.
        // PG parse them as date + offset, MySQL parse them as date + time (rubbish behavior!)
        // So strange! reject these strange case.
        // Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01-01"));
        // Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01-1"));
        // Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01+01"));
        // Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01+1"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 01:00:00.000000"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 00:01:00.000000"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 00:00:01.000000"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01 00:00:00.000001"));
    }

    @Test
    void testNormalize() {
        String s = DateLiteral.normalize("2021-5");
        Assertions.assertEquals("2021-05", s);
        s = DateLiteral.normalize("2021-5-1");
        Assertions.assertEquals("2021-05-01", s);
        s = DateLiteral.normalize("2021-5-01");
        Assertions.assertEquals("2021-05-01", s);

        s = DateLiteral.normalize("2021-5-01 0:0:0");
        Assertions.assertEquals("2021-05-01 00:00:00", s);
        s = DateLiteral.normalize("2021-5-01 0:0:0.001");
        Assertions.assertEquals("2021-05-01 00:00:00.001", s);

        s = DateLiteral.normalize("2021-5-01 0:0:0.001+8:0");
        Assertions.assertEquals("2021-05-01 00:00:00.001+08:00", s);
        s = DateLiteral.normalize("2021-5-01 0:0:0.001+8:0:0");
        Assertions.assertEquals("2021-05-01 00:00:00.001+08:00:00", s);

        s = DateLiteral.normalize("2021-5-01 0:0:0.001UTC+8:0");
        Assertions.assertEquals("2021-05-01 00:00:00.001UTC+08:00", s);
        s = DateLiteral.normalize("2021-5-01 0:0:0.001UTC+8:0:0");
        Assertions.assertEquals("2021-05-01 00:00:00.001UTC+08:00:00", s);

    }

    @Test
    void testDate() {
        new DateLiteral("220101");
        new DateLiteral("22-01-01");
        new DateLiteral("22-01-1");
        new DateLiteral("22-1-1");

        new DateLiteral("2022-01-01");
        new DateLiteral("2022-01-1");
        new DateLiteral("2022-1-1");
        new DateLiteral("20220101");

        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("-01-01"));
    }

    @Test
    void testZone() {
        new DateLiteral("2022-01-01Z");
        new DateLiteral("2022-01-01UTC");
        new DateLiteral("2022-01-01GMT");
        new DateLiteral("2022-01-01UTC+08");
        new DateLiteral("2022-01-01UTC-06");
        new DateLiteral("2022-01-01UTC+08:00");
        new DateLiteral("2022-01-01UTC-06:00");
        new DateLiteral("2022-01-01Europe/London");
    }

    @Test
    void testOffset() {
        new DateLiteral("2022-01-01+01:00:00");
        new DateLiteral("2022-01-01+01:00");
        new DateLiteral("2022-01-01+01");
        new DateLiteral("2022-01-01+1:0:0");
        new DateLiteral("2022-01-01+1:0");
        new DateLiteral("2022-01-01+1");

        new DateLiteral("2022-01-01-01:00:00");
        new DateLiteral("2022-01-01-01:00");
        new DateLiteral("2022-01-01-1:0:0");
        new DateLiteral("2022-01-01-1:0");
    }

    @Test
    void testIrregularDate() {
        Consumer<DateLiteral> assertFunc = (DateLiteral dateLiteral) -> {
            Assertions.assertEquals("2016-07-02", dateLiteral.toString());
        };
        DateLiteral dateLiteral;

        dateLiteral = new DateLiteral("2016-07-02");
        assertFunc.accept(dateLiteral);

        dateLiteral = new DateLiteral("2016-7-02");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-07-2");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-7-2");
        assertFunc.accept(dateLiteral);

        dateLiteral = new DateLiteral("2016-07-02");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-07-2");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-7-02");
        assertFunc.accept(dateLiteral);
        dateLiteral = new DateLiteral("2016-7-2");
        assertFunc.accept(dateLiteral);
    }
}
