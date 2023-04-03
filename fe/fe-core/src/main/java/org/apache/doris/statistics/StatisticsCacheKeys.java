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

package org.apache.doris.statistics;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticsCacheKeys extends LinkedHashMap<StatisticsCacheKey, Object> implements Writable {

    private static final String DELIMITER = ",";

    /**
     * This should only get invoked when reading journal.
     */
    public StatisticsCacheKeys(Map<StatisticsCacheKey, Object> keys) {
        this();
        putAll(keys);
    }

    public StatisticsCacheKeys() {
        super(StatisticConstants.PRELOAD_STATS_CACHE_ENTRY_COUNT, 0.75f, true);
    }

    public synchronized void update(StatisticsCacheKey key) {
        put(key, null);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String keysString = keySet().stream().map(StatisticsCacheKey::toString)
                .collect(Collectors.joining(DELIMITER));
        Text.writeString(out, keysString);
    }

    public static StatisticsCacheKeys read(DataInput input) throws IOException {
        String logs = Text.readString(input);
        if (StringUtils.isEmpty(logs)) {
            return new StatisticsCacheKeys();
        }
        Map<StatisticsCacheKey, Object> keys = new HashMap<>();
        for (String s : logs.split(DELIMITER)) {
            StatisticsCacheKey k = StatisticsCacheKey.fromString(s);
            keys.put(k, null);
        }
        return new StatisticsCacheKeys(keys);
    }

    protected boolean removeEldestEntry(Map.Entry<StatisticsCacheKey, Object> eldest) {
        return size() > StatisticConstants.PRELOAD_STATS_CACHE_ENTRY_COUNT;
    }
}
