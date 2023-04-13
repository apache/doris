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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateArithmeticTest {
    @Test
    public void testYearsAddAndSub() {
        Object[][] answer = new Object[][] {{}};
        for (int i = 0; i < DateUtils.DATETIME_LITERALS.length; ++i) {
            for (int j = 0; j < DateUtils.INTEGER_LITERALS.length; ++j) {
                Assertions.assertEquals(DateTimeArithmetic.hoursAdd(DateUtils.DATETIME_LITERALS[i], DateUtils.INTEGER_LITERALS[j]), answer[i][j]);
            }
        }
    }

    @Test
    public void testMonthsAddAndSub() {

    }

    @Test
    public void testDaysAddAndSub() {

    }

    @Test
    public void testHoursAddAndSub() {

    }

    @Test
    public void testMinutesAddAndSub() {

    }

    @Test
    public void testSecondsAddAndSub() {

    }
}
