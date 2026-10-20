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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Type;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PaimonTimestampNsTest {
    @Test
    public void nanosecondTimestampRoundTripsWithoutTimezone() {
        for (boolean flag : new boolean[] {false, true}) {
            for (int precision : new int[] {0, 3, 6, 7, 8, 9}) {
                Type mapped = PaimonUtil.paimonTypeToDorisType(new TimestampType(precision), flag, flag);
                Assertions.assertEquals(precision > 6, mapped.isTimeStampNs());
                Assertions.assertTrue(PaimonUtil.paimonTypeToDorisType(
                        new LocalZonedTimestampType(precision), flag, flag).isTimeStampTz());
            }
        }
        Assertions.assertEquals(new TimestampType(9), new DorisToPaimonTypeVisitor().atomic(Type.TIMESTAMP_NS));
    }

    @Test
    public void predicateLiteralsRetainNanoseconds() {
        for (long nanos : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
            java.time.LocalDateTime value = java.time.LocalDateTime.ofEpochSecond(
                    Math.floorDiv(nanos, 1_000_000_000L), (int) Math.floorMod(nanos, 1_000_000_000L),
                    java.time.ZoneOffset.UTC);
            org.apache.doris.datasource.paimon.source.PaimonValueConverter converter =
                    new org.apache.doris.datasource.paimon.source.PaimonValueConverter(
                            new org.apache.doris.analysis.TimeStampNsLiteral(value));
            Assertions.assertEquals(value, converter.visit(new TimestampType(9)).toLocalDateTime());
        }
    }

    @Test
    public void nestedNanosecondsRequireTheLosslessReader() {
        Assertions.assertTrue(containsNanosecondTimestamp(new TimestampType(9)));
        Assertions.assertTrue(containsNanosecondTimestamp(
                new ArrayType(new TimestampType(8))));
        Assertions.assertTrue(containsNanosecondTimestamp(
                new MapType(new IntType(), new TimestampType(7))));
        Assertions.assertTrue(containsNanosecondTimestamp(
                RowType.of(new IntType(), new ArrayType(new TimestampType(9)))));
        Assertions.assertFalse(containsNanosecondTimestamp(
                new ArrayType(new TimestampType(6))));
        Assertions.assertFalse(containsNanosecondTimestamp(
                new LocalZonedTimestampType(9)));
    }

    private boolean containsNanosecondTimestamp(org.apache.paimon.types.DataType type) {
        return PaimonUtil.containsTimestampNs(PaimonUtil.paimonTypeToDorisType(type, false, false));
    }
}
