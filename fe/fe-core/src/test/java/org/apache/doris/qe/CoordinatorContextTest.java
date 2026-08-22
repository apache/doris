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

package org.apache.doris.qe;

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TQueryGlobals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class CoordinatorContextTest {
    @Test
    void testSetQueryGlobalsCurrentTimeUsesOneClockSample() {
        Instant currentTime = Instant.ofEpochSecond(1_704_067_201L, 987_654_321L);
        TQueryGlobals queryGlobals = new TQueryGlobals();

        CoordinatorContext.setQueryGlobalsCurrentTime(queryGlobals, currentTime);

        Assertions.assertEquals(TimeUtils.getDatetimeFormatWithTimeZone().format(currentTime),
                queryGlobals.getNowString());
        Assertions.assertEquals(currentTime.toEpochMilli(), queryGlobals.getTimestampMs());
        Assertions.assertEquals(currentTime.getNano(), queryGlobals.getNanoSeconds());
    }

    @Test
    void testLoadQueryGlobalsSetNanoseconds() {
        TQueryGlobals queryGlobals = new TQueryGlobals();

        CoordinatorContext.setQueryGlobalsForLoad(queryGlobals, "UTC", true);

        Assertions.assertTrue(queryGlobals.isSetNanoSeconds());
        Assertions.assertEquals("UTC", queryGlobals.getTimeZone());
        Assertions.assertTrue(queryGlobals.isLoadZeroTolerance());
    }
}
