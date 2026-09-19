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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DorisFlightSqlServiceTest {

    // The token cache size floors the effective Flight sub-quota at 1, but never the configured cap:
    // a legal sub-quota of 0 (qe_max_connection = 1) keeps one token so the first request reaches the
    // pool and is refused with RESOURCE_EXHAUSTED, while an illegal arrow_flight_token_cache_size (<= 0)
    // is passed through so the base's loud failure (Guava rejects a negative maximumSize; 0 evicts every
    // token) is preserved rather than the FE silently running on a one-token cache.
    @Test
    public void testEffectiveTokenCacheSizeFloorsOnlyTheSubQuota() {
        // Legal sub-quota of 0 -> floored to 1.
        Assertions.assertEquals(1, DorisFlightSqlService.effectiveTokenCacheSize(0, 4096));
        // Normal: the smaller of the sub-quota and the cap.
        Assertions.assertEquals(512, DorisFlightSqlService.effectiveTokenCacheSize(512, 4096));
        Assertions.assertEquals(4096, DorisFlightSqlService.effectiveTokenCacheSize(8192, 4096));
        Assertions.assertEquals(10, DorisFlightSqlService.effectiveTokenCacheSize(512, 10));
        // An illegal cap is not floored: it stays <= 0 for the base's loud failure.
        Assertions.assertEquals(0, DorisFlightSqlService.effectiveTokenCacheSize(0, 0));
        Assertions.assertEquals(-1, DorisFlightSqlService.effectiveTokenCacheSize(512, -1));
    }
}
