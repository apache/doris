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

package org.apache.doris.datasource.paimon.profile;

import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PaimonMetricRegistry extends MetricRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonMetricRegistry.class);
    private static final String TABLE_TAG_KEY = "table";
    private final ConcurrentHashMap<String, MetricGroup> groups = new ConcurrentHashMap<>();

    @Override
    protected MetricGroup createMetricGroup(String name, Map<String, String> tags) {
        MetricGroup group = new MetricGroupImpl(name, tags);
        String table = tags == null ? "" : tags.getOrDefault(TABLE_TAG_KEY, "");
        groups.put(buildKey(name, table), group);
        LOG.debug("Created metric group: {}:{}", name, table);
        return group;
    }

    public MetricGroup getGroup(String name, String table) {
        String key = buildKey(name, table);
        MetricGroup group = groups.get(key);
        if (group == null) {
            LOG.warn("MetricGroup not found: {}", key);
        }
        return group;
    }

    public void removeGroup(String name, String table) {
        groups.remove(buildKey(name, table));
    }

    public Collection<MetricGroup> getAllGroups() {
        return groups.values();
    }

    public Map<String, MetricGroup> getAllGroupsAsMap() {
        return new ConcurrentHashMap<>(groups);
    }

    public void clear() {
        groups.clear();
    }

    private static String buildKey(String name, String table) {
        return name + ":" + table;
    }
}
