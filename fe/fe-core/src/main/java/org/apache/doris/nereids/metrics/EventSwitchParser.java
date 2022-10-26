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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * parser
 */
public class EventSwitchParser {
    /**
     * parse
     */
    public static Set<Class<? extends Event>> parse(String eventTypeMode) {
        List<String> strings = Arrays.stream(eventTypeMode.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        Preconditions.checkArgument(strings.size() > 0);
        if ("all".equals(strings.get(0))) {
            if (strings.size() == 1) {
                return ImmutableSet.copyOf(Event.EVENT_TYPE_SET.values());
            }
            Preconditions.checkArgument(strings.size() > 2 && "except".equals(strings.get(1)));
            Map targetClasses = Maps.newHashMap(Event.EVENT_TYPE_SET);
            for (String str : strings.subList(2, strings.size())) {
                targetClasses.remove(str);
            }
            return ImmutableSet.copyOf(targetClasses.values());
        }
        return strings.stream()
                .filter(str -> Event.EVENT_TYPE_SET.containsKey(str))
                .map(str -> ((Class<? extends Event>) Event.EVENT_TYPE_SET.get(str)))
                .collect(ImmutableSet.toImmutableSet());
    }
}
