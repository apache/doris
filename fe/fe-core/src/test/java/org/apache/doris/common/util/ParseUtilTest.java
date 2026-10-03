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

package org.apache.doris.common.util;

import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigInteger;

public class ParseUtilTest {
    @ParameterizedTest
    @CsvSource({
            "B, 1", "K, 1024", "KB, 1024", "M, 1048576", "MB, 1048576",
            "G, 1073741824", "GB, 1073741824", "T, 1099511627776", "TB, 1099511627776",
            "P, 1125899906842624", "PB, 1125899906842624"
    })
    public void testDataVolumeUnitBoundaries(String unit, long multiplier) throws AnalysisException {
        Assertions.assertEquals(7 * multiplier, ParseUtil.analyzeDataVolume("7" + unit));
        long maxUnits = Long.MAX_VALUE / multiplier;
        Assertions.assertEquals(maxUnits * multiplier, ParseUtil.analyzeDataVolume(maxUnits + unit));
        String overflow = BigInteger.valueOf(maxUnits).add(BigInteger.ONE) + unit;
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ParseUtil.analyzeDataVolume(overflow));
        Assertions.assertTrue(exception.getMessage().contains("invalid data volume:"));
    }

    @Test
    public void testDataVolumeWithoutUnit() throws AnalysisException {
        Assertions.assertEquals(7, ParseUtil.analyzeDataVolume("7"));
        Assertions.assertEquals(Long.MAX_VALUE, ParseUtil.analyzeDataVolume(Long.toString(Long.MAX_VALUE)));
        Assertions.assertThrows(AnalysisException.class,
                () -> ParseUtil.analyzeDataVolume("9223372036854775808"));
    }

    @Test
    public void testPositiveOverflow() {
        Assertions.assertThrows(AnalysisException.class, () -> ParseUtil.analyzeDataVolume("16385PB"));
    }

    @Test
    public void testInvalidDataVolume() {
        for (String value : new String[] {"0", "0KB", "-1", "-1KB", "1XB", "", "KB"}) {
            Assertions.assertThrows(AnalysisException.class, () -> ParseUtil.analyzeDataVolume(value), value);
        }
    }
}
