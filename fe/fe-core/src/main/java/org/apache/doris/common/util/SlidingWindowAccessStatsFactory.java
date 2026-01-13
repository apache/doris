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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory class for creating and managing SlidingWindowAccessStats instances for different ID types
 */
public class SlidingWindowAccessStatsFactory {
    private static final Logger LOG = LogManager.getLogger(SlidingWindowAccessStatsFactory.class);

    // Map to store instances for different ID types
    private static final Map<AccessStatsIdType, SlidingWindowAccessStats> instances = new ConcurrentHashMap<>();

    /**
     * Get or create a SlidingWindowAccessStats instance for the specified ID type
     */
    public static SlidingWindowAccessStats getInstance(AccessStatsIdType idType) {
        return instances.computeIfAbsent(idType, type -> {
            SlidingWindowAccessStats stats = new SlidingWindowAccessStats(type);
            LOG.info("Created SlidingWindowAccessStats instance for type: {}", type);
            return stats;
        });
    }


    // async record tablet instance access
    public static void recordTablet(long id) {
        SlidingWindowAccessStats sas = getInstance(AccessStatsIdType.TABLET);
        sas.recordAccessAsync(id);
    }

    public static SlidingWindowAccessStats getTabletAccessStats() {
        return getInstance(AccessStatsIdType.TABLET);
    }
}

