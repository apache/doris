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

package org.apache.doris.nereids.metrics;

import org.apache.doris.nereids.metrics.event.CostStateUpdateEvent;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.metrics.event.EnforcerEvent;
import org.apache.doris.nereids.metrics.event.FunctionCallEvent;
import org.apache.doris.nereids.metrics.event.GroupMergeEvent;
import org.apache.doris.nereids.metrics.event.StatsStateEvent;
import org.apache.doris.nereids.metrics.event.TransformEvent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * parser
 */
public class EventSwitchParser {
    private static final Map<String, Class<? extends Event>> EVENT_TYPE_SET =
            new Builder<String, Class<? extends Event>>()
            .put("costState", CostStateUpdateEvent.class)
            .put("counter", CounterEvent.class)
            .put("enforcer", EnforcerEvent.class)
            .put("functionCall", FunctionCallEvent.class)
            .put("groupMerge", GroupMergeEvent.class)
            .put("statsState", StatsStateEvent.class)
            .put("transform", TransformEvent.class)
            .build();

    /**
     * parse
     */
    public static Set<Class<? extends Event>> parse(List<String> eventTypeMode) {
        if ("all".equals(eventTypeMode.get(0))) {
            if (eventTypeMode.size() == 1) {
                return ImmutableSet.copyOf(EVENT_TYPE_SET.values());
            }
            Map targetClasses = Maps.newHashMap(EVENT_TYPE_SET);
            for (String str : eventTypeMode.subList(2, eventTypeMode.size())) {
                targetClasses.remove(str);
            }
            return ImmutableSet.copyOf(targetClasses.values());
        }
        return eventTypeMode.stream()
                .filter(EVENT_TYPE_SET::containsKey)
                .map(str -> ((Class<? extends Event>) EVENT_TYPE_SET.get(str)))
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * check
     */
    public static List<String> checkEventModeStringAndSplit(String eventTypeMode) {
        List<String> strings = Arrays.stream(eventTypeMode.toLowerCase().split("[\\s+,]"))
                .map(String::trim)
                .collect(ImmutableList.toImmutableList());
        if (strings.size() == 0) {
            return null;
        } else if ("all".equals(strings.get(0))) {
            if (strings.size() == 1) {
                return strings;
            } else if (strings.size() == 2 || !"except".equals(strings.get(1))) {
                return null;
            }
        }
        return strings;
    }
}
