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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class DateLiteralTest {
    @Test
    void testDate() {
        new DateLiteral("220101");
        new DateLiteral("22-01-01");

        new DateLiteral("2022-01-01");
        new DateLiteral("20220101");

        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("-01-01"));
    }

    @Test
    void testZone() {
        new DateLiteral("2022-01-01Z");
        new DateLiteral("2022-01-01UTC");
        new DateLiteral("2022-01-01GMT");
        new DateLiteral("2022-01-01UTC+8");
        new DateLiteral("2022-01-01UTC-6");

        new DateLiteral("20220801GMT+5");
        new DateLiteral("20220801GMT-3");
    }

    @Test
    @Disabled
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

        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01-01"));
        Assertions.assertThrows(AnalysisException.class, () -> new DateLiteral("2022-01-01-1"));
    }

    @Disabled
    @Test
    void testIrregularDate() {
        new DateLiteral("2016-07-02");

        new DateLiteral("2016-7-02");
        new DateLiteral("2016-07-2");
        new DateLiteral("2016-7-2");

        new DateLiteral("2016-07-02");
        new DateLiteral("2016-07-2");
        new DateLiteral("2016-7-02");
        new DateLiteral("2016-7-2");
    }
}
