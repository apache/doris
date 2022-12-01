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

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * event producer
 */
public class EventProducer {
    private final EventChannel channel;
    private final List<EventFilter> filters;
    private final Class<? extends Event> eventClass;

    /**
     * constructor
     * @param eventClass event's class info for the producer, a producer can only supply one type of event.
     * @param filters event's filter, the event that satisfy the filter's condition can be submitted to the channel.
     *        the filters are AND in logic, see checkAndLog for detail.
     * @param channel the channel to transport event to consumer.
     */
    public EventProducer(Class<? extends Event> eventClass, EventChannel channel, EventFilter ...filters) {
        this.channel = channel;
        Preconditions.checkArgument(Arrays.stream(filters).allMatch(f -> f.getTargetClass().equals(eventClass)));
        this.filters = Arrays.asList(filters);
        this.eventClass = eventClass;
    }

    private void checkAndLog(Event event) {
        for (EventFilter filter : filters) {
            event = filter.checkEvent(event);
            if (event == null) {
                return;
            }
        }
        channel.add(event);
    }

    public void log(Event event) {
        if (event == null || channel == null) {
            return;
        }
        Preconditions.checkArgument(event.getClass().equals(eventClass));
        checkAndLog(event);
    }

    public void log(Event event, Supplier<Boolean> f) {
        if (f.get()) {
            log(event);
        }
    }
}
