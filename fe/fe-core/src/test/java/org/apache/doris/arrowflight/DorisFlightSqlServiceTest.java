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

package org.apache.doris.arrowflight;

import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class DorisFlightSqlServiceTest {

    // The former token cache's settings are no longer read: a fe.conf that still tunes them is told so
    // at startup, by name and value, and one that leaves them alone is not.
    @Test
    public void testIgnoredTokenSettingsNameOnlyWhatDiffersFromTheDefault() {
        Assertions.assertEquals(Collections.emptyList(), DorisFlightSqlService.ignoredTokenSettings(
                DorisFlightSqlService.DEFAULT_TOKEN_CACHE_SIZE, DorisFlightSqlService.DEFAULT_TOKEN_ALIVE_TIME_SECOND));
        Assertions.assertEquals(Collections.singletonList("arrow_flight_token_cache_size=10"),
                DorisFlightSqlService.ignoredTokenSettings(10, DorisFlightSqlService.DEFAULT_TOKEN_ALIVE_TIME_SECOND));
        Assertions.assertEquals(Collections.singletonList("arrow_flight_token_alive_time_second=3600"),
                DorisFlightSqlService.ignoredTokenSettings(DorisFlightSqlService.DEFAULT_TOKEN_CACHE_SIZE, 3600));
        Assertions.assertEquals(
                Arrays.asList("arrow_flight_token_cache_size=0", "arrow_flight_token_alive_time_second=0"),
                DorisFlightSqlService.ignoredTokenSettings(0, 0));
    }

    // The defaults the startup check compares against are the fields' own, so that an untouched
    // fe.conf is never reported.
    @Test
    public void testDefaultsMatchTheConfigFields() {
        Assertions.assertEquals(DorisFlightSqlService.DEFAULT_TOKEN_CACHE_SIZE, Config.arrow_flight_token_cache_size);
        Assertions.assertEquals(DorisFlightSqlService.DEFAULT_TOKEN_ALIVE_TIME_SECOND,
                Config.arrow_flight_token_alive_time_second);
    }
}
