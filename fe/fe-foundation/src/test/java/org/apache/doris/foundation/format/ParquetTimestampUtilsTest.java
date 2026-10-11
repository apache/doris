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

package org.apache.doris.foundation.format;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParquetTimestampUtilsTest {
    @Test
    public void canonicalizesOffsetsAndPreservesIanaZones() {
        Assertions.assertEquals("", ParquetTimestampUtils.parseHiveTimeZone("  "));
        Assertions.assertEquals("+08:00", ParquetTimestampUtils.parseHiveTimeZone(" 8:00 "));
        Assertions.assertEquals("GMT-07:00", ParquetTimestampUtils.parseHiveTimeZone("GMT-7:00"));
        for (String value : new String[] {"UTC", "GMT", "Asia/Shanghai", "America/Los_Angeles",
                "+14:00", "-12:00", "UTC+14:00", "GMT-12:00"}) {
            Assertions.assertEquals(value, ParquetTimestampUtils.parseHiveTimeZone(value));
        }
    }

    @Test
    public void rejectsAmbiguousAliasesAndOutOfRangeOffsets() {
        for (String value : new String[] {"CST", "PRC", "AET", "Invalid/Zone", "+14:01", "-12:01",
                "UTC+15:00", "GMT-13:00", "+08:60", "UTC+99:00"}) {
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ParquetTimestampUtils.parseHiveTimeZone(value));
            Assertions.assertTrue(error.getMessage().contains(ParquetTimestampUtils.HIVE_TIME_ZONE));
        }
    }
}
